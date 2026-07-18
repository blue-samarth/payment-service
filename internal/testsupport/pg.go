package testsupport

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"samarth/payment-service/config"
	"samarth/payment-service/internal/adapters/postgres"
)

type PG struct {
	DB *postgres.DB
	Q  *postgres.Queries
}

var (
	pgOnce   sync.Once
	sharedPG *PG
	pgErr    error
)

func RequirePostgres(t *testing.T) *PG {
	t.Helper()
	pgOnce.Do(func() { sharedPG, pgErr = setupPostgres() })
	if pgErr != nil {
		t.Skipf("integration postgres unavailable: %v\n  start it with: docker compose -f deploy/docker/docker-compose.test.yml up -d", pgErr)
	}
	return sharedPG
}

func (p *PG) Truncate(t *testing.T, tables ...string) {
	t.Helper()
	if len(tables) == 0 {
		tables = []string{
			"transactions", "processing_lease", "idempotency_keys",
			"outbox_events", "outbox_dead_letters", "refunds",
		}
	}
	sql := "TRUNCATE " + strings.Join(tables, ", ") + " CASCADE"
	if _, err := p.DB.Pool().Exec(context.Background(), sql); err != nil {
		t.Fatalf("testsupport: truncate %v: %v", tables, err)
	}
}

func setupPostgres() (*PG, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	db, err := postgres.New(ctx, dbConfigFromEnv())
	if err != nil {
		return nil, err
	}
	if err := resetSchema(ctx, db); err != nil {
		return nil, fmt.Errorf("reset schema: %w", err)
	}
	if err := applyMigrations(ctx, db); err != nil {
		return nil, fmt.Errorf("apply migrations: %w", err)
	}
	q, err := postgres.LoadQueries()
	if err != nil {
		return nil, err
	}
	return &PG{DB: db, Q: q}, nil
}

func dbConfigFromEnv() config.DatabaseConfig {
	port, _ := strconv.Atoi(getEnv("PAYMENT_TEST_DB_PORT", "5432"))
	return config.DatabaseConfig{
		PrimaryHost:       getEnv("PAYMENT_TEST_DB_HOST", "localhost"),
		Port:              port,
		Name:              getEnv("PAYMENT_TEST_DB_NAME", "payment_test"),
		User:              getEnv("PAYMENT_TEST_DB_USER", "payment"),
		Password:          getEnv("PAYMENT_TEST_DB_PASSWORD", "payment"),
		SSLMode:           getEnv("PAYMENT_TEST_DB_SSLMODE", "disable"),
		SearchPath:        testSchema(),
		MaxOpenConns:      10,
		MaxIdleConns:      2,
		ConnMaxLifetime:   5 * time.Minute,
		ConnMaxIdleTime:   1 * time.Minute,
		HealthCheckPeriod: 30 * time.Second,
	}
}

// testSchema is a per-process schema name, so integration packages running in
// parallel each get an isolated schema instead of racing on a shared public
// schema (each package test binary is its own process, so the PID is unique).
func testSchema() string {
	return fmt.Sprintf("test_%d", os.Getpid())
}

func resetSchema(ctx context.Context, db *postgres.DB) error {
	schema := testSchema()
	_, err := db.Pool().Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE; CREATE SCHEMA %s;", schema, schema))
	return err
}

func applyMigrations(ctx context.Context, db *postgres.DB) error {
	dir := migrationsDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	var files []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".up.sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	for _, name := range files {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		if _, err := db.Pool().Exec(ctx, string(data)); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}

func migrationsDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "migrations")
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// AllShards returns the full shard range [0, 64) for tests that poll the whole
// outbox without partitioning across relay workers.
func AllShards() []int {
	shards := make([]int, 64)
	for i := range shards {
		shards[i] = i
	}
	return shards
}
