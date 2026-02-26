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

	"github.com/acksell/bezos/dynamodb/ddbstore"
)

//go:embed static/*
var staticFiles embed.FS

// ServerConfig configures the debug UI server.
type ServerConfig struct {
	// Port is the HTTP port to listen on.
	Port int
	// DBPath is the path to the BadgerDB database. Empty for in-memory mode.
	DBPath string
	// SchemaPattern is a glob pattern for schema YAML files.
	SchemaPattern string
}

// Server is the debug UI HTTP server.
type Server struct {
	config     ServerConfig
	store      *ddbstore.Store
	schema     *LoadedSchema
	httpServer *http.Server
}

// NewServer creates a new debug UI server.
func NewServer(config ServerConfig) (*Server, error) {
	// Load schemas
	schema, err := LoadSchemas(config.SchemaPattern)
	if err != nil {
		return nil, fmt.Errorf("loading schemas: %w", err)
	}

	// Create store
	store, err := ddbstore.New(
		ddbstore.StoreOptions{
			Path:     config.DBPath,
			InMemory: config.DBPath == "",
		},
		schema.TableDefinitions...,
	)
	if err != nil {
		return nil, fmt.Errorf("creating store: %w", err)
	}

	return &Server{
		config: config,
		store:  store,
		schema: schema,
	}, nil
}

// Run starts the server and blocks until shutdown.
func (s *Server) Run() error {
	mux := http.NewServeMux()

	// Register API handlers
	apiHandler := NewAPIHandler(s.store, s.schema)
	apiHandler.RegisterRoutes(mux)

	// Serve static files
	staticFS, err := fs.Sub(staticFiles, "static")
	if err != nil {
		return fmt.Errorf("creating static fs: %w", err)
	}
	fileServer := http.FileServer(http.FS(staticFS))
	mux.Handle("GET /", fileServer)

	// Create HTTP server
	s.httpServer = &http.Server{
		Addr:         fmt.Sprintf(":%d", s.config.Port),
		Handler:      corsMiddleware(loggingMiddleware(mux)),
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
		s.store.Close()
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

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			return err
		}
	}
	if s.store != nil {
		return s.store.Close()
	}
	return nil
}

func (s *Server) printBanner() {
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    DynamoDB Debug UI                         ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  URL: http://localhost:%-40d║\n", s.config.Port)
	if s.config.DBPath == "" {
		fmt.Println("║  Mode: In-memory (data will be lost on exit)                 ║")
	} else {
		fmt.Printf("║  Database: %-51s║\n", truncate(s.config.DBPath, 51))
	}
	fmt.Println("╠══════════════════════════════════════════════════════════════╣")
	fmt.Println("║  Tables:                                                     ║")
	for name, sf := range s.schema.Tables {
		gsiCount := len(sf.Table.GSIs)
		entityCount := len(sf.Entities)
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
