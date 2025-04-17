package importer

import (
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
)

func extract7zArchive(archivePath, outputDir string, logger *zap.Logger) error {
	logger.Info("распаковка архива", zap.String("archive", archivePath))

	if _, err := os.Stat(archivePath); os.IsNotExist(err) {
		return fmt.Errorf("архив не найден: %s", archivePath)
	}

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("не удалось создать директорию для извлечения: %w", err)
	}

	cmd := exec.Command("7z", "x", archivePath, "-o"+outputDir, "-y")
	output, err := cmd.CombinedOutput()
	if err != nil {
		logger.Error("ошибка распаковки архива",
			zap.String("output", string(output)),
			zap.Error(err))
		return fmt.Errorf("ошибка при распаковке архива: %w", err)
	}

	logger.Info("архив успешно распакован", zap.String("output_dir", outputDir))
	return nil
}

func findXmlFile(directory, fileType string) (string, error) {
	pattern := fmt.Sprintf("%s/*.xml", directory)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return "", fmt.Errorf("ошибка при поиске xml файлов: %w", err)
	}

	if len(files) == 0 {
		return "", fmt.Errorf("xml файлы не найдены в директории %s", directory)
	}

	if fileType != "" {
		fileTypeLower := strings.ToLower(fileType)
		for _, file := range files {
			baseFileName := strings.ToLower(filepath.Base(file))
			if strings.Contains(baseFileName, fileTypeLower) {
				return file, nil
			}
		}
		return "", fmt.Errorf("файл типа %s не найден в директории %s", fileType, directory)
	}

	return files[0], nil
}

func parseXmlFile(filePath string, rowProcessor func(row *xml.StartElement) error, logger *zap.Logger) error {
	logger.Info("начало парсинга xml файла", zap.String("file", filePath))

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("не удалось открыть файл: %w", err)
	}
	defer file.Close()

	decoder := xml.NewDecoder(file)
	var rowCount int

	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("ошибка чтения xml токена: %w", err)
		}

		if startElement, ok := token.(xml.StartElement); ok {
			if startElement.Name.Local == "row" {
				if err := rowProcessor(&startElement); err != nil {
					return fmt.Errorf("ошибка обработки строки: %w", err)
				}

				rowCount++
				if rowCount%10000 == 0 {
					logger.Info("обработано строк", zap.Int("count", rowCount))
				}
			}
		}
	}

	logger.Info("парсинг файла завершен",
		zap.String("file", filePath),
		zap.Int("total_rows", rowCount))

	return nil
}

func startElementToMap(start *xml.StartElement) map[string]string {
	attrMap := make(map[string]string)
	for _, attr := range start.Attr {
		attrMap[attr.Name.Local] = attr.Value
	}
	return attrMap
}
