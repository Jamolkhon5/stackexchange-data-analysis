package queries

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

type QueryRunner struct {
	db     *sqlx.DB
	logger *zap.Logger
}

func NewQueryRunner(db *sqlx.DB, logger *zap.Logger) *QueryRunner {
	return &QueryRunner{
		db:     db,
		logger: logger,
	}
}

func (q *QueryRunner) ExecuteQuery(queryFilePath, outputDir string) error {
	q.logger.Info("выполнение запроса", zap.String("file", queryFilePath))

	queryBytes, err := os.ReadFile(queryFilePath)
	if err != nil {
		return fmt.Errorf("не удалось прочитать файл запроса: %w", err)
	}

	queryText := string(queryBytes)
	queryText = stripExplainAnalyze(queryText)

	queryName := filepath.Base(queryFilePath)

	rows, err := q.db.Queryx(queryText)
	if err != nil {
		return fmt.Errorf("ошибка выполнения запроса: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		result := make(map[string]interface{})
		if err := rows.MapScan(result); err != nil {
			return fmt.Errorf("ошибка сканирования результатов: %w", err)
		}
		for k, v := range result {
			if b, ok := v.([]byte); ok {
				result[k] = string(b)
			}
		}

		results = append(results, result)
	}

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("не удалось создать директорию для результатов: %w", err)
	}

	outputFilePath := filepath.Join(outputDir, fmt.Sprintf("%s.json", queryName))
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		return fmt.Errorf("не удалось создать файл результата: %w", err)
	}
	defer outputFile.Close()

	encoder := json.NewEncoder(outputFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(results); err != nil {
		return fmt.Errorf("ошибка сериализации результатов: %w", err)
	}

	q.logger.Info("запрос выполнен успешно",
		zap.String("file", queryFilePath),
		zap.Int("row_count", len(results)),
		zap.String("output", outputFilePath))

	return nil
}

func (q *QueryRunner) ExplainQuery(queryFilePath, outputDir string) error {
	q.logger.Info("анализ запроса", zap.String("file", queryFilePath))

	queryBytes, err := os.ReadFile(queryFilePath)
	if err != nil {
		return fmt.Errorf("не удалось прочитать файл запроса: %w", err)
	}

	queryText := string(queryBytes)

	if containsTransaction(queryText) {
		q.logger.Warn("файл содержит транзакции, пропускаем EXPLAIN ANALYZE",
			zap.String("file", queryFilePath))
		return nil
	}

	queryText = stripExplainAnalyze(queryText)
	explainQuery := "EXPLAIN ANALYZE " + queryText

	queryName := filepath.Base(queryFilePath)

	rows, err := q.db.Query(explainQuery)
	if err != nil {
		q.logger.Warn("ошибка выполнения запроса EXPLAIN, запускаем без EXPLAIN ANALYZE",
			zap.String("file", queryFilePath),
			zap.Error(err))
		return q.ExecuteQuery(queryFilePath, outputDir)
	}
	defer rows.Close()

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("не удалось создать директорию для результатов: %w", err)
	}

	outputFilePath := filepath.Join(outputDir, fmt.Sprintf("%s.explain.txt", queryName))
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		return fmt.Errorf("не удалось создать файл плана запроса: %w", err)
	}
	defer outputFile.Close()

	var planLines []string
	for rows.Next() {
		var planLine string
		if err := rows.Scan(&planLine); err != nil {
			return fmt.Errorf("ошибка сканирования результата: %w", err)
		}

		planLines = append(planLines, planLine)
		fmt.Fprintln(outputFile, planLine)
	}

	q.logger.Info("анализ запроса выполнен успешно",
		zap.String("file", queryFilePath),
		zap.String("output", outputFilePath))
	return q.ExecuteQuery(queryFilePath, outputDir)
}

func containsTransaction(query string) bool {
	queryLower := strings.ToLower(query)
	return strings.Contains(queryLower, "begin") ||
		strings.Contains(queryLower, "commit") ||
		strings.Contains(queryLower, "create ") ||
		strings.Contains(queryLower, "alter ")
}

func stripExplainAnalyze(query string) string {
	queryLower := strings.ToLower(query)
	if strings.Contains(queryLower, "explain analyze") {
		explainPos := strings.Index(queryLower, "explain analyze")
		startPos := explainPos + len("explain analyze")
		for startPos < len(query) && (query[startPos] == ' ' || query[startPos] == '\n' || query[startPos] == '\r' || query[startPos] == '\t') {
			startPos++
		}
		if startPos < len(query) {
			return query[startPos:]
		}
		return ""
	}
	return query
}

// выполняет только аналитические запросы (без скриптов создания/изменения схемы)
func (q *QueryRunner) RunAnalyticalQueries(queryDir, outputDir string) error {
	q.logger.Info("выполнение аналитических запросов", zap.String("dir", queryDir))
	analyticalQueries := []string{
		filepath.Join(queryDir, "q1.sql"),
		filepath.Join(queryDir, "q2.sql"),
	}

	for _, queryPath := range analyticalQueries {
		if _, err := os.Stat(queryPath); os.IsNotExist(err) {
			q.logger.Warn("файл запроса не найден, пропускаем",
				zap.String("file", queryPath))
			continue
		}

		q.logger.Info("обработка запроса", zap.String("file", queryPath))
		if err := q.ExplainQuery(queryPath, outputDir); err != nil {
			q.logger.Error("ошибка при анализе запроса",
				zap.String("file", queryPath),
				zap.Error(err))
		}
	}

	q.logger.Info("все аналитические запросы выполнены")
	return nil
}

// выполняет запросы из директории
func (q *QueryRunner) RunAllQueries(queryDir, outputDir string) error {
	q.logger.Info("выполнение всех запросов", zap.String("dir", queryDir))
	postTagsScript := filepath.Join(queryDir, "create_post_tags.sql")
	if _, err := os.Stat(postTagsScript); os.IsNotExist(err) {
		postTagsSQL := `
CREATE OR REPLACE FUNCTION extract_tags(tags_text TEXT)
RETURNS TABLE(tag TEXT) AS $$
BEGIN
    IF tags_text IS NULL THEN
        RETURN;
    END IF;

    RETURN QUERY
    SELECT
        unnest(
                string_to_array(
                        regexp_replace(
                                regexp_replace(tags_text, '[<>]', ' ', 'g'),
                                '\s+', ' ', 'g'
                        ),
                        ' '
                )
        ) AS tag
    WHERE length(tag) > 0;  -- Используем правильный алиас tag вместо unnest
END;
$$ LANGUAGE plpgsql;

-- Удаляем представление, если оно существует
DROP MATERIALIZED VIEW IF EXISTS post_tags;

-- Создаем представление с разобранными тегами для каждого поста
CREATE MATERIALIZED VIEW post_tags AS
SELECT
    p.id AS post_id,
    t.tag
FROM
    posts p,
    LATERAL extract_tags(p.tags) t
WHERE
    p.tags IS NOT NULL;

-- Индекс на представление для быстрого поиска постов по тегу
CREATE INDEX IF NOT EXISTS idx_post_tags_tag ON post_tags(tag);
CREATE INDEX IF NOT EXISTS idx_post_tags_post_id ON post_tags(post_id);`

		q.logger.Info("создание файла create_post_tags.sql")
		if err := os.WriteFile(postTagsScript, []byte(postTagsSQL), 0644); err != nil {
			q.logger.Warn("не удалось создать файл create_post_tags.sql", zap.Error(err))
		}
	}

	q.logger.Info("создание/обновление материализованного представления post_tags")
	if content, err := os.ReadFile(postTagsScript); err == nil {
		_, err := q.db.Exec(string(content))
		if err != nil {
			q.logger.Error("ошибка при создании post_tags", zap.Error(err))
		} else {
			q.logger.Info("материализованное представление post_tags успешно создано")
		}
	}

	constraintsScript := filepath.Join(queryDir, "add_constraints.sql")
	if _, err := os.Stat(constraintsScript); err == nil {
		q.logger.Info("добавление ограничений внешнего ключа")
		if content, err := os.ReadFile(constraintsScript); err == nil {
			_, err := q.db.Exec(string(content))
			if err != nil {
				q.logger.Error("ошибка при добавлении ограничений", zap.Error(err))
			} else {
				q.logger.Info("ограничения успешно добавлены")
			}
		}
	}

	return q.RunAnalyticalQueries(queryDir, outputDir)
}
