package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

type GotifyApp struct {
	ID          int64  `json:"id"`
	Token       string `json:"token"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Image       string `json:"image"`
}

// Gotify message struct (simplified)
type GotifyMessage struct {
	ID       int64  `json:"id"`
	AppID    int64  `json:"appid"`
	Title    string `json:"title"`
	Message  string `json:"message"`
	Priority int    `json:"priority"`
}

type AppStore struct {
	mu   sync.RWMutex
	byID map[int64]GotifyApp
}

// Map Gotify (0–10) to ntfy (1–5)
/*func mapGotifyToNtfyPriority(gotify int) int {
	if gotify <= 2 {
		return 1 // min
	}
	if gotify <= 4 {
		return 2 // low
	}
	if gotify <= 6 {
		return 3 // default
	}
	if gotify <= 8 {
		return 4 // high
	}
	return 5 // max
}*/

// Config holds the configuration settings for Gotify and ntfy communication.
// It includes server URLs, authentication tokens, database path, and synchronization preferences.
type Config struct {
	GotifyURL     string
	GotifyToken   string
	NtfyURL       string
	NtfyTopic     string
	NtfyAuthToken string
	NtfyPriority  int
	NtfyIcon      string
	SplitTopics   bool
	SyncInterval  time.Duration
	Debug         bool
	Timezone      string
	AppsDBPath    string
}

func loadConfig() (*Config, error) {
	// load .env into environment (only if present)
	_ = godotenv.Load()

	cfg := &Config{
		GotifyURL:     os.Getenv("GOTIFY_URL"),
		GotifyToken:   os.Getenv("GOTIFY_CLIENT_TOKEN"),
		NtfyURL:       os.Getenv("NTFY_URL"),
		NtfyTopic:     os.Getenv("NTFY_TOPIC"),
		NtfyAuthToken: os.Getenv("NTFY_AUTH_TOKEN"),
		NtfyIcon:      os.Getenv("NTFY_ICON"),
		Timezone:      os.Getenv("TZ"),
		AppsDBPath:    os.Getenv("GOTIFY_APPS_DB"),
	}

	if cfg.AppsDBPath == "" {
		cfg.AppsDBPath = "apps_db.json"
	}

	cfg.SplitTopics = strings.ToLower(os.Getenv("NTFY_SPLIT_TOPICS")) == "true"
	if interval, err := strconv.Atoi(os.Getenv("NTFY_SYNC_INTERVAL")); err == nil {
		cfg.SyncInterval = time.Duration(interval) * time.Second
	} else {
		cfg.SyncInterval = 5 * time.Minute
	}

	cfg.Debug = strings.ToLower(os.Getenv("NTFY_DEBUG")) == "true"

	dbg(cfg, "Using SplitTopics: %t", cfg.SplitTopics)
	if cfg.NtfyAuthToken != "" {
		dbg(cfg, "Using auth token")
	}
	// parse priority with default
	if p, err := strconv.Atoi(os.Getenv("NTFY_PRIORITY")); err == nil {
		cfg.NtfyPriority = p
	} else {
		cfg.NtfyPriority = 3
	}

	// sanity check
	if cfg.GotifyURL == "" || cfg.GotifyToken == "" || cfg.NtfyURL == "" || cfg.NtfyTopic == "" {
		return nil, fmt.Errorf("missing required env vars: GOTIFY_URL, GOTIFY_CLIENT_TOKEN, NTFY_URL, NTFY_TOPIC")
	}

	return cfg, nil
}

func dbg(cfg *Config, format string, a ...interface{}) {
	if cfg.Debug {
		log.Printf("[DEBUG] "+format, a...)
	}
}

var topicRe = regexp.MustCompile(`[^a-zA-Z0-9_-]+`)

func sanitizeTopic(s string) string {
	s = strings.ToLower(s)
	s = topicRe.ReplaceAllString(s, "_")
	s = strings.Trim(s, "_")
	if s == "" {
		return "default"
	}
	return s
}

func NewAppStore(initial []GotifyApp) *AppStore {
	as := &AppStore{byID: make(map[int64]GotifyApp)}
	as.SetAll(initial)
	return as
}

func (a *AppStore) SetAll(apps []GotifyApp) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, app := range apps {
		a.byID[app.ID] = app
	}
}

func (a *AppStore) Upsert(app GotifyApp) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.byID[app.ID] = app
}

func (a *AppStore) Get(appID int64) (GotifyApp, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	app, ok := a.byID[appID]
	return app, ok
}

func (a *AppStore) TopicFor(appID int64, fallback string) string {
	app, ok := a.Get(appID)
	if !ok {
		return fallback
	}
	return sanitizeTopic(app.Name)
}

func mapGotifyToNtfyPriority(gotify int) int {
	p := int(math.Round(float64(gotify) / 2.5))        // 0–10 -> 0–4
	return int(math.Min(math.Max(float64(p+1), 1), 5)) // clamp to 1–5
}

func getApplications(cfg *Config) ([]GotifyApp, error) {
	// Build the REST base URL from the configured websocket URL, preserving subpaths.
	// Examples:
	//   wss://host/gotify/stream     -> https://host/gotify/application
	//   ws://host/stream?x=y         -> http://host/application
	//   https://host/gotify/stream   -> https://host/gotify/application
	u, err := url.Parse(cfg.GotifyURL)
	if err != nil {
		return nil, fmt.Errorf("invalid GOTIFY_URL: %w", err)
	}

	// Map ws(s) -> http(s); keep http/https as-is
	switch u.Scheme {
	case "wss":
		u.Scheme = "https"
	case "ws":
		u.Scheme = "http"
	case "http", "https":
		// keep
	default:
		// default to https to be safe
		u.Scheme = "https"
	}

	basePath := strings.TrimSuffix(u.EscapedPath(), "/stream")
	u.RawQuery = ""
	u.Fragment = ""
	u.Path = path.Join(basePath, "/application")

	appsURL := u.String()

	req, err := http.NewRequest("GET", appsURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Gotify-Key", cfg.GotifyToken)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Gotify /application failed: %s", resp.Status)
	}

	var apps []GotifyApp
	if err := json.NewDecoder(resp.Body).Decode(&apps); err != nil {
		return nil, err
	}
	return apps, nil
}

func ensureTopic(cfg *Config, topic string) error {
	// ntfy topics are virtual and do not require creation.
	// IMPORTANT: Do NOT PUT/POST here, as that would publish a message and trigger subscribers.
	// We only validate the topic format locally and return.
	if topic == "" {
		return fmt.Errorf("topic is empty")
	}
	// Optionally log the prepared topic without touching ntfy
	dbg(cfg, "[SYNC] Topic validated (no-op): %s", topic)
	return nil
}

func loadKnownApps(path string) (map[int64]GotifyApp, error) {
	m := make(map[int64]GotifyApp)
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return m, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

func saveKnownApps(path string, m map[int64]GotifyApp) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(m); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func sendNtfy(cfg *Config, topic, title, body string, priority int) error {
	endpoint := strings.TrimRight(cfg.NtfyURL, "/") + "/" + url.PathEscape(strings.TrimLeft(topic, "/"))
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBufferString(body))
	if err != nil {
		return err
	}
	if title != "" {
		req.Header.Set("Title", title)
	}
	if priority <= 0 {
		priority = cfg.NtfyPriority
	}

    if cfg.NtfyIcon != "" {
        req.Header.Set("Icon", cfg.NtfyIcon)
    }
	
	req.Header.Set("Priority", fmt.Sprint(mapGotifyToNtfyPriority(priority)))
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if cfg.NtfyAuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.NtfyAuthToken)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ntfy error: %s: %s", resp.Status, string(b))
	}
	return nil
}

func syncTopics(cfg *Config, store *AppStore, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	known, err := loadKnownApps(cfg.AppsDBPath)
	if err != nil {
		log.Printf("[SYNC ERROR] could not load known apps db: %v", err)
		known = make(map[int64]GotifyApp)
	}

	// Seed from current Gotify
	current, err := getApplications(cfg)
	if err == nil {
		for _, a := range current {
			known[a.ID] = a
		}
		_ = saveKnownApps(cfg.AppsDBPath, known)
		store.SetAll(current)
	} else {
		log.Printf("[SYNC WARN] initial getApplications failed: %v", err)
	}

	for {
		cur, err := getApplications(cfg)
		if err != nil {
			log.Printf("[SYNC ERROR] Could not load applications: %v", err)
			<-ticker.C
			continue
		}

		// Detect new or changed apps
		for _, a := range cur {
			old, ok := known[a.ID]
			if !ok {
				// New app detected
				title := "New Gotify app detected"
				body := fmt.Sprintf("Name: %s (ID=%d)\nDescription: %q", a.Name, a.ID, a.Description)

				if err := sendNtfy(cfg, cfg.NtfyTopic, title, body, 4); err != nil {
					log.Printf("[SYNC ERROR] failed to notify about new app %s (ID=%d): %v", a.Name, a.ID, err)
				} else {
					log.Printf("[SYNC] Notified about new app: %s (ID=%d)", a.Name, a.ID)
				}

				// Add the new app to the store and known apps
				store.Upsert(a)
				known[a.ID] = a
			} else if old.Description != a.Description {
				// Description changed
				title := "Gotify app description updated"
				body := fmt.Sprintf("App: %s (ID=%d)\nOld: %q\nNew: %q", a.Name, a.ID, old.Description, a.Description)
				if err := sendNtfy(cfg, cfg.NtfyTopic, title, body, 3); err != nil {
					log.Printf("[SYNC ERROR] failed to notify about description change for %s (ID=%d): %v", a.Name, a.ID, err)
				} else {
					log.Printf("[SYNC] Notified description change for app %s (ID=%d)", a.Name, a.ID)
				}

				store.Upsert(a)
				known[a.ID] = a
			}
		}

		if err := saveKnownApps(cfg.AppsDBPath, known); err != nil {
			log.Printf("[SYNC ERROR] could not save known apps db: %v", err)
		}

		// Validate topics locally (no network)
		for _, a := range cur {
			topic := sanitizeTopic(a.Name)
			if err := ensureTopic(cfg, topic); err != nil {
				log.Printf("[SYNC ERROR] Could not validate topic %s: %v", topic, err)
			} else {
				dbg(cfg, "[SYNC] Topic ready: %s", topic)
			}
		}

		<-ticker.C
	}
}

// Pass config pointer instead of multiple args
func listenAndForward(cfg *Config, store *AppStore) error {
	headers := http.Header{}
	headers.Set("X-Gotify-Key", cfg.GotifyToken)

	conn, _, err := websocket.DefaultDialer.Dial(cfg.GotifyURL, headers)
	if err != nil {
		return err
	}
	defer conn.Close()

	log.Println("Connected to Gotify stream")

	// Channel to decouple WebSocket reads from HTTP posts
	msgCh := make(chan GotifyMessage, 100)
	defer close(msgCh)

	// Start a few workers
	workerCount := 4
	var wg sync.WaitGroup
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func(id int) {
			defer wg.Done()
			for m := range msgCh {
				if err := forwardToNtfy(cfg, store, m); err != nil {
					log.Printf("[worker %d] forward error: %v", id, err)
				} else {
					dbg(cfg, "[worker %d] Forwarded to ntfy", id)
				}
			}
		}(i + 1)
	}

	// Read loop
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			// Let workers drain then return to trigger reconnect in main
			break
		}

		var gotifyMsg GotifyMessage
		if err := json.Unmarshal(message, &gotifyMsg); err != nil {
			log.Println("json error:", err)
			continue
		}

		// Non-blocking enqueue; drop if full (log and continue)
		select {
		case msgCh <- gotifyMsg:
			// ok
		default:
			log.Printf("[WARN] message channel full, dropping message appID=%d id=%d", gotifyMsg.AppID, gotifyMsg.ID)
		}
	}

	// Close channel & wait workers before leaving
	close(msgCh)
	wg.Wait()
	return fmt.Errorf("websocket closed")
}

// Forward to ntfy.sh
func forwardToNtfy(cfg *Config, store *AppStore, msg GotifyMessage) error {
	appTopic := cfg.NtfyTopic
	if cfg.SplitTopics {
		appTopic = store.TopicFor(msg.AppID, cfg.NtfyTopic)
	}

	endpoint := strings.TrimRight(cfg.NtfyURL, "/") + "/" + url.PathEscape(strings.TrimLeft(appTopic, "/"))
	payload := []byte(msg.Message)


	dbg(cfg, "Forwarding to ntfy URL: %s", endpoint)
	dbg(cfg, "Payload:\n%s", payload)
	dbg(cfg, "Incoming priority (Gotify or default): %d", msg.Priority)

	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	if msg.Title != "" {
		req.Header.Set("Title", msg.Title)
	}

	incoming := msg.Priority
	if incoming == 0 {
		incoming = cfg.NtfyPriority
	}
	mapped := mapGotifyToNtfyPriority(incoming)
	req.Header.Set("Priority", fmt.Sprint(mapped))
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	dbg(cfg, "Mapped priority to ntfy: %d -> %d", incoming, mapped)

	if cfg.NtfyAuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.NtfyAuthToken)
		dbg(cfg, "Using auth token")
	}

	if cfg.NtfyIcon != "" {
        req.Header.Set("Icon", cfg.NtfyIcon)
        dbg(cfg, "Using icon: %s", cfg.NtfyIcon)
    }

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	dbg(cfg, "ntfy response status: %s", resp.Status)

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		dbg(cfg, "ntfy.sh error body: %s", string(body))
		return fmt.Errorf("ntfy.sh error: %s", resp.Status)
	}
	return nil
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Starting forwarder: Gotify=%s -> ntfy=%s/%s",
		cfg.GotifyURL, cfg.NtfyURL, cfg.NtfyTopic)

	// Seed apps (best effort)
	initialApps, err := getApplications(cfg)
	if err != nil {
		log.Printf("Could not load applications: %v", err)
	} else {
		log.Printf("Got %d apps:", len(initialApps))

		// Prepare message body for ntfy
		var lines []string
		for _, app := range initialApps {
			if cfg.Debug {
				log.Printf("- ID=%d Name=%s Description=%s Token=%s", app.ID, app.Name, app.Description, app.Token)
			} else {
				masked := strings.Repeat("*", len(app.Token))
				log.Printf("- ID=%d Name=%s Description=%s Token=%s", app.ID, app.Name, app.Description, masked)
			}
			// Add name & description to ntfy message
			lines = append(lines, fmt.Sprintf("- %s: %s", app.Name, app.Description))
		}

		// Send startup message to ntfy
		body := "Gotify apps on startup:\n" + strings.Join(lines, "\n")
		title := "Gotify Apps found on startup"
		if err := sendNtfy(cfg, cfg.NtfyTopic, title, body, 3); err != nil {
			log.Printf("[NTFY ERROR] failed to send startup message: %v", err)
		} else {
			log.Printf("[NTFY] Sent startup message with %d apps", len(initialApps))
		}
	}

	store := NewAppStore(initialApps)

	if cfg.SplitTopics {
		go syncTopics(cfg, store, cfg.SyncInterval)
	}

	attempt := 0
	for {
		err := listenAndForward(cfg, store)
		if err != nil {
			log.Printf("connection error: %v", err)
		}

		sleep := time.Duration(math.Min(float64(5*int(math.Pow(2, float64(attempt)))), 60)) * time.Second
		log.Printf("Reconnecting in %v...", sleep)
		time.Sleep(sleep)

		if attempt < 6 {
			attempt++
		}
	}
}
