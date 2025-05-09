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
	// Load .env file
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

	// Verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("database ping failed: %v", err)
	}

	return db, nil
}

// [Keep all the benchmark functions from previous code: insertUsingPoolQuery,
// insertUsingGetConnection, insertUsingPoolExec, insertUsingTransaction, runBenchmark]

func main() {
	// Load configuration from .env
	config := loadConfig()

	// Create database connection pool
	db, err := createConnectionPool(config)
	if err != nil {
		log.Fatalf("Failed to create connection pool: %v", err)
	}
	defer db.Close()

	log.Println("Database connected successfully")

	// Run benchmark with configurable number of inserts
	insertCount := getEnvAsInt("BENCHMARK_INSERT_COUNT", 1000)
	if err := runBenchmark(db, insertCount); err != nil {
		log.Fatalf("Benchmark failed: %v", err)
	}
}

// Helper functions
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
