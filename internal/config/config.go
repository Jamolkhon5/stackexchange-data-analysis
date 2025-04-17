package config

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

type Config struct {
	Database    DatabaseConfig
	DataDir     string
	Concurrency int
}

type DatabaseConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Name     string
	SSLMode  string
}

func setDefaults() {
	viper.SetDefault("database.host", "localhost")
	viper.SetDefault("database.port", 5432)
	viper.SetDefault("database.user", "postgres")
	viper.SetDefault("database.password", "postgres")
	viper.SetDefault("database.name", "stackexchange")
	viper.SetDefault("database.sslmode", "disable")
	viper.SetDefault("data_dir", "./data")
	viper.SetDefault("concurrency", 4)
}

func Load() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	setDefaults()

	viper.AutomaticEnv()
	viper.SetEnvPrefix("APP")

	viper.BindEnv("database.host", "DB_HOST")
	viper.BindEnv("database.port", "DB_PORT")
	viper.BindEnv("database.user", "DB_USER")
	viper.BindEnv("database.password", "DB_PASSWORD")
	viper.BindEnv("database.name", "DB_NAME")
	viper.BindEnv("database.sslmode", "DB_SSLMODE")
	viper.BindEnv("data_dir", "DATA_DIR")
	viper.BindEnv("concurrency", "CONCURRENCY")

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("ошибка чтения конфигурации: %w", err)
		}
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("ошибка парсинга конфигурации: %w", err)
	}

	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "./data"
	}
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			fmt.Printf("Предупреждение: не удалось создать директорию %s: %v\n", dataDir, err)
		}
	}
	cfg.DataDir = dataDir

	return &cfg, nil
}

func (c *DatabaseConfig) ConnString() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.Name, c.SSLMode)
}
