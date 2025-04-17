package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"stackexchange-data-analysis/internal/config"
	"stackexchange-data-analysis/internal/database"
	"stackexchange-data-analysis/internal/importer"
	"stackexchange-data-analysis/internal/queries"
)

func main() {
	logger := setupLogger()
	defer logger.Sync()

	mode := flag.String("mode", "", "Режим работы: import, queries, all")
	configPath := flag.String("config", "", "Путь к файлу конфигурации")
	flag.Parse()

	if *mode == "" && flag.NArg() > 0 {
		*mode = flag.Arg(0)
	}

	var cfg *config.Config
	var err error

	if *configPath != "" {
		logger.Info("использую указанный конфигурационный файл", zap.String("path", *configPath))
	}

	cfg, err = config.Load()
	if err != nil {
		logger.Fatal("ошибка загрузки конфигурации", zap.Error(err))
	}

	db, err := connectToDatabase(cfg, logger)
	if err != nil {
		logger.Fatal("ошибка подключения к базе данных", zap.Error(err))
	}
	defer db.Close()

	scriptsDir := "../scripts"
	if _, err := os.Stat(scriptsDir); os.IsNotExist(err) {
		scriptsDir = "./scripts"
		if _, err := os.Stat(scriptsDir); os.IsNotExist(err) {
			logger.Fatal("директория со скриптами не найдена", zap.String("path", scriptsDir))
		}
	}

	schemaPath := filepath.Join(scriptsDir, "create_schema.sql")
	indexesPath := filepath.Join(scriptsDir, "indexes.sql")
	queriesDir := scriptsDir
	resultsDir := "./results"

	switch *mode {
	case "import":
		err = runImport(db, cfg, schemaPath, indexesPath, logger)
	case "queries":
		err = runQueries(db, queriesDir, resultsDir, logger)
	case "analysis":
		err = runAnalysis(db, queriesDir, resultsDir, logger)
	case "all":
		err = runAll(db, cfg, schemaPath, indexesPath, queriesDir, resultsDir, logger)
	default:
		logger.Fatal("неизвестный режим работы, используйте: import, queries, analysis или all")
	}

	if err != nil {
		logger.Fatal("ошибка выполнения", zap.Error(err))
	}

	logger.Info("работа завершена успешно")
}

func setupLogger() *zap.Logger {
	cfg := zap.NewProductionConfig()
	cfg.EncoderConfig.TimeKey = "time"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

	logger, err := cfg.Build()
	if err != nil {
		fmt.Printf("ошибка инициализации логгера: %v\n", err)
		os.Exit(1)
	}

	return logger
}

func connectToDatabase(cfg *config.Config, logger *zap.Logger) (*sqlx.DB, error) {
	logger.Info("подключение к базе данных",
		zap.String("host", cfg.Database.Host),
		zap.Int("port", cfg.Database.Port),
		zap.String("db", cfg.Database.Name))

	db, err := sqlx.Connect("postgres", cfg.Database.ConnString())
	if err != nil {
		return nil, fmt.Errorf("ошибка подключения к базе данных: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ошибка проверки соединения: %w", err)
	}

	logger.Info("успешное подключение к базе данных")
	return db, nil
}

func runImport(db *sqlx.DB, cfg *config.Config, schemaPath, indexesPath string, logger *zap.Logger) error {
	logger.Info("начало импорта данных")

	postgresDB, err := database.NewPostgresDB(&cfg.Database, logger)
	if err != nil {
		return err
	}
	defer postgresDB.Close()

	if err := postgresDB.CreateSchema(schemaPath); err != nil {
		return err
	}

	importer := importer.NewImporter(db, cfg, logger)

	if err := importer.ImportAll(); err != nil {
		return err
	}

	if err := postgresDB.CreateIndexes(indexesPath); err != nil {
		return err
	}

	logger.Info("импорт данных завершен успешно")
	return nil
}

func runQueries(db *sqlx.DB, queriesDir, resultsDir string, logger *zap.Logger) error {
	logger.Info("начало выполнения запросов")

	queryRunner := queries.NewQueryRunner(db, logger)

	if err := queryRunner.RunAllQueries(queriesDir, resultsDir); err != nil {
		return err
	}

	logger.Info("выполнение запросов завершено успешно")
	return nil
}

func runAll(db *sqlx.DB, cfg *config.Config, schemaPath, indexesPath, queriesDir, resultsDir string, logger *zap.Logger) error {
	if err := runImport(db, cfg, schemaPath, indexesPath, logger); err != nil {
		return err
	}

	if err := runQueries(db, queriesDir, resultsDir, logger); err != nil {
		return err
	}

	return nil
}

func runAnalysis(db *sqlx.DB, queriesDir, resultsDir string, logger *zap.Logger) error {
	logger.Info("начало выполнения аналитических запросов")

	queryRunner := queries.NewQueryRunner(db, logger)

	if err := queryRunner.RunAllQueries(queriesDir, resultsDir); err != nil {
		return err
	}

	logger.Info("выполнение аналитических запросов завершено успешно")
	return nil
}
