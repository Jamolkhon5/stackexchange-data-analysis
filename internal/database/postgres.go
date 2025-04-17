package database

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"stackexchange-data-analysis/internal/config"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
)

type PostgresDB struct {
	db     *sqlx.DB
	logger *zap.Logger
}

func NewPostgresDB(cfg *config.DatabaseConfig, logger *zap.Logger) (*PostgresDB, error) {
	db, err := sqlx.Connect("postgres", cfg.ConnString())
	if err != nil {
		return nil, fmt.Errorf("не удалось подключиться к базе данных: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	return &PostgresDB{
		db:     db,
		logger: logger,
	}, nil
}

func (p *PostgresDB) Close() error {
	return p.db.Close()
}

func (p *PostgresDB) CreateSchema(schemaPath string) error {
	p.logger.Info("создание схемы базы данных")

	schema, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("не удалось прочитать файл схемы: %w", err)
	}

	_, err = p.db.Exec(string(schema))
	if err != nil {
		return fmt.Errorf("ошибка при выполнении sql схемы: %w", err)
	}

	p.logger.Info("схема базы данных успешно создана")
	return nil
}

func (p *PostgresDB) CreateIndexes(indexesPath string) error {
	p.logger.Info("создание индексов")

	indexes, err := ioutil.ReadFile(indexesPath)
	if err != nil {
		return fmt.Errorf("не удалось прочитать файл индексов: %w", err)
	}

	_, err = p.db.Exec(string(indexes))
	if err != nil {
		return fmt.Errorf("ошибка при создании индексов: %w", err)
	}

	p.logger.Info("индексы успешно созданы")
	return nil
}

func (p *PostgresDB) ExecuteQueryFile(queryPath string) ([]map[string]interface{}, error) {
	queryName := filepath.Base(queryPath)
	p.logger.Info("выполнение запроса", zap.String("query", queryName))

	queryBytes, err := ioutil.ReadFile(queryPath)
	if err != nil {
		return nil, fmt.Errorf("не удалось прочитать файл запроса: %w", err)
	}

	queryStr := string(queryBytes)

	parts := strings.Split(queryStr, "EXPLAIN ANALYZE")
	if len(parts) < 2 {
		return nil, fmt.Errorf("запрос должен содержать EXPLAIN ANALYZE: %s", queryPath)
	}
	queryText := parts[1]

	planRows, err := p.db.Query(queryStr)
	if err != nil {
		return nil, fmt.Errorf("ошибка выполнения запроса: %w", err)
	}
	defer planRows.Close()

	p.logger.Info("план запроса", zap.String("query", queryName))
	for planRows.Next() {
		var planLine string
		if err := planRows.Scan(&planLine); err != nil {
			return nil, fmt.Errorf("ошибка сканирования плана запроса: %w", err)
		}
		fmt.Println(planLine)
	}

	if err = planRows.Err(); err != nil {
		return nil, fmt.Errorf("ошибка при обработке результатов плана: %w", err)
	}

	dataRows, err := p.db.Queryx(queryText)
	if err != nil {
		return nil, fmt.Errorf("ошибка выполнения запроса (без EXPLAIN): %w", err)
	}
	defer dataRows.Close()

	var results []map[string]interface{}
	for dataRows.Next() {
		result := make(map[string]interface{})
		if err := dataRows.MapScan(result); err != nil {
			return nil, fmt.Errorf("ошибка сканирования результатов: %w", err)
		}
		results = append(results, result)
	}

	if err = dataRows.Err(); err != nil {
		return nil, fmt.Errorf("ошибка при обработке результатов запроса: %w", err)
	}

	p.logger.Info("запрос выполнен успешно",
		zap.String("query", queryName),
		zap.Int("rows", len(results)))

	return results, nil
}
