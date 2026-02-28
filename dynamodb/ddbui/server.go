package ddbui

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/acksell/bezos/dynamodb/ddbiface"
	"github.com/acksell/bezos/dynamodb/schema"
)

//go:embed static/*
var staticFiles embed.FS

// Server is the debug UI HTTP server.
type Server struct {
	port       int
	client     ddbiface.AWSDynamoClientV2
	schema     *LoadedSchema
	httpServer *http.Server
}

// NewServer creates a new debug UI server.
// The client must implement the AWSDynamoClientV2 interface (e.g., *ddbstore.Store
// or the AWS SDK v2 *dynamodb.Client).
// Multiple schemas can be passed and will be merged.
func NewServer(client ddbiface.AWSDynamoClientV2, port int, schemas ...schema.Schema) (*Server, error) {
	if len(schemas) == 0 {
		return nil, fmt.Errorf("at least one schema is required")
	}

	loaded, err := LoadFromSchema(schemas...)
	if err != nil {
		return nil, fmt.Errorf("loading schemas: %w", err)
	}

	return &Server{
		port:   port,
		client: client,
		schema: loaded,
	}, nil
}

// Handler returns an http.Handler for the debug UI.
// This can be used to mount the UI on an existing HTTP server.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Register API handlers
	apiHandler := NewAPIHandler(s.client, s.schema)
	apiHandler.RegisterRoutes(mux)

	// Serve static files
	staticFS, err := fs.Sub(staticFiles, "static")
	if err != nil {
		// This should never happen with embedded files
		panic(fmt.Sprintf("creating static fs: %v", err))
	}
	fileServer := http.FileServer(http.FS(staticFS))
	mux.Handle("GET /", fileServer)

	return corsMiddleware(loggingMiddleware(mux))
}

// Run starts the server and blocks until shutdown.
// The caller is responsible for closing the client after Run returns.
func (s *Server) Run() error {
	// Create HTTP server
	s.httpServer = &http.Server{
		Addr:         fmt.Sprintf(":%d", s.port),
		Handler:      s.Handler(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	// Handle shutdown signals
	done := make(chan struct{})
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("\nShutting down...")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		s.httpServer.Shutdown(ctx)
		close(done)
	}()

	// Print startup banner
	s.printBanner()

	// Start server
	if err := s.httpServer.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}

	<-done
	return nil
}

// Shutdown gracefully shuts down the HTTP server.
// This does NOT close the underlying client - the caller is responsible for that.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

func (s *Server) printBanner() {
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    DynamoDB Debug UI                         ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  URL: http://localhost:%-40d║\n", s.port)
	fmt.Println("╠══════════════════════════════════════════════════════════════╣")
	fmt.Println("║  Tables:                                                     ║")
	for name, t := range s.schema.Tables {
		gsiCount := len(t.GSIs)
		entityCount := len(t.Entities)
		line := fmt.Sprintf("    - %s (%d GSIs, %d entities)", name, gsiCount, entityCount)
		fmt.Printf("║  %-60s║\n", truncate(line, 60))
	}
	fmt.Println("╠══════════════════════════════════════════════════════════════╣")
	fmt.Println("║  Press Ctrl+C to stop                                        ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

// loggingMiddleware logs HTTP requests.
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		if r.URL.Path != "/favicon.ico" {
			log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
		}
	})
}

// corsMiddleware adds CORS headers for development.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
