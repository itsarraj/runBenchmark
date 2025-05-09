package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

type DBConfig struct {
	Host     string
	User     string
	Password string
	Database string
	PoolSize int
}

func loadConfig() DBConfig {
	err := godotenv.Load()
	if err != nil {
		log.Printf("Warning: Could not load .env file (using environment variables directly): %v", err)
	}

	return DBConfig{
		Host:     getEnv("DB_HOST", "localhost"),
		User:     getEnv("DB_USER", "berufplattf"),
		Password: getEnv("DB_PASS", "berufplattf.db.password"),
		Database: getEnv("DB_NAME", "berufplattform_db"),
		PoolSize: getEnvAsInt("DB_POOL_SIZE", 5),
	}
}

func createConnectionPool(config DBConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&multiStatements=true",
		config.User, config.Password, config.Host, config.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %v", err)
	}

	db.SetMaxOpenConns(config.PoolSize)
	db.SetMaxIdleConns(config.PoolSize)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("database ping failed: %v", err)
	}

	return db, nil
}

func insertUsingPoolQuery(db *sql.DB, n int) error {
	start := time.Now()

	for i := 0; i < n; i++ {
		_, err := db.Query(
			"INSERT INTO benchmark_users (name, email) VALUES (?, ?)",
			fmt.Sprintf("UserPool%d", i),
			fmt.Sprintf("pool%d@example.com", i),
		)
		if err != nil {
			return fmt.Errorf("query error: %v", err)
		}
	}

	duration := time.Since(start)
	log.Printf("Direct db.Query: Inserted %d rows in %v", n, duration)
	return nil
}

func insertUsingGetConnection(db *sql.DB, n int) error {
	start := time.Now()

	conn, err := db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("get connection error: %v", err)
	}
	defer conn.Close()

	for i := 0; i < n; i++ {
		_, err := conn.ExecContext(
			context.Background(),
			"INSERT INTO benchmark_users (name, email) VALUES (?, ?)",
			fmt.Sprintf("UserConn%d", i),
			fmt.Sprintf("conn%d@example.com", i),
		)
		if err != nil {
			return fmt.Errorf("exec error: %v", err)
		}
	}

	duration := time.Since(start)
	log.Printf("Using db.Conn.ExecContext: Inserted %d rows in %v", n, duration)
	return nil
}

func insertUsingPoolExec(db *sql.DB, n int) error {
	start := time.Now()

	for i := 0; i < n; i++ {
		_, err := db.Exec(
			"INSERT INTO benchmark_users (name, email) VALUES (?, ?)",
			fmt.Sprintf("UserExec%d", i),
			fmt.Sprintf("exec%d@example.com", i),
		)
		if err != nil {
			return fmt.Errorf("exec error: %v", err)
		}
	}

	duration := time.Since(start)
	log.Printf("Direct db.Exec: Inserted %d rows in %v", n, duration)
	return nil
}

func insertUsingTransaction(db *sql.DB, n int) error {
	start := time.Now()

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction error: %v", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	for i := 0; i < n; i++ {
		_, err := tx.Exec(
			"INSERT INTO benchmark_users (name, email) VALUES (?, ?)",
			fmt.Sprintf("UserTx%d", i),
			fmt.Sprintf("tx%d@example.com", i),
		)
		if err != nil {
			return fmt.Errorf("tx exec error: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit error: %v", err)
	}

	duration := time.Since(start)
	log.Printf("Using transaction: Inserted %d rows in %v", n, duration)
	return nil
}

func runBenchmark(db *sql.DB, n int) error {
	log.Println("Starting benchmark...\n")

	if err := insertUsingPoolQuery(db, n); err != nil {
		return err
	}

	if err := insertUsingGetConnection(db, n); err != nil {
		return err
	}

	if err := insertUsingPoolExec(db, n); err != nil {
		return err
	}

	if err := insertUsingTransaction(db, n); err != nil {
		return err
	}

	log.Println("\nBenchmark completed.")
	return nil
}

func main() {
	config := loadConfig()
	db, err := createConnectionPool(config)
	if err != nil {
		log.Fatalf("Failed to create connection pool: %v", err)
	}
	defer db.Close()

	log.Println("Database connected successfully")

	insertCount := getEnvAsInt("BENCHMARK_INSERT_COUNT", 1000)
	if err := runBenchmark(db, insertCount); err != nil {
		log.Fatalf("Benchmark failed: %v", err)
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	if value, exists := os.LookupEnv(key); exists {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}
