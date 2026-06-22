package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all runtime configuration loaded from environment variables.
// Both the TCP and UDP binaries read the same Config; each binary only uses
// the fields relevant to its transport.
type Config struct {
	// TCP binary
	TCPAddr     string
	TCPHTTPAddr string

	// UDP binary
	UDPAddr     string
	UDPHTTPAddr string

	RedisAddr     string
	RedisPassword string
	RedisDB       int
	RedisPoolSize int

	StreamKey    string
	StreamMaxLen int64
	SessionPrefix string
	OnlineZKey   string
	CmdChannel   string

	AuthTimeout      time.Duration
	HeartbeatTimeout time.Duration
	WriteTimeout     time.Duration

	DBEnabled      bool
	DBHost         string
	DBPort         string
	DBUser         string
	DBPassword     string
	DBName         string
	DBDeviceTypeID int64

	DBDevicesTable    string
	DBIMEIColumn      string
	DBBroadcastColumn string
	DBStatusColumn    string
	DBTypeIDColumn    string
	DBNameColumn      string
	DBNotesColumn     string
	DBCreatedAtColumn string
	DBUpdatedAtColumn string

	Debug    bool
	ServerID string
}

func (c *Config) MySQLDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&timeout=5s&readTimeout=5s&writeTimeout=5s",
		c.DBUser, c.DBPassword, c.DBHost, c.DBPort, c.DBName,
	)
}

func Load() *Config {
	dbHostSet := os.Getenv("DB_HOST") != ""
	dbEnabled := envBool("DB_ENABLED", dbHostSet)

	return &Config{
		TCPAddr:     envStr("H02_TCP_ADDR", ":7020"),
		TCPHTTPAddr: envStr("H02_TCP_HTTP_ADDR", ":9092"),
		UDPAddr:     envStr("H02_UDP_ADDR", ":7021"),
		UDPHTTPAddr: envStr("H02_UDP_HTTP_ADDR", ":9093"),

		RedisAddr:     envStr("REDIS_HOST", "redis") + ":" + envStr("REDIS_PORT", "6379"),
		RedisPassword: envStr("REDIS_PASSWORD", ""),
		RedisDB:       envInt("REDIS_H02_DB", 2),
		RedisPoolSize: envInt("REDIS_POOL_SIZE", 100),

		StreamKey:    envStr("STREAM_KEY", "h02:telemetry"),
		StreamMaxLen: envInt64("STREAM_MAX_LEN", 100_000),
		SessionPrefix: envStr("SESSION_PREFIX", "h02:session:"),
		OnlineZKey:   envStr("ONLINE_Z_KEY", "h02:online"),
		CmdChannel:   envStr("CMD_CHANNEL", "h02:cmd:"),

		AuthTimeout:      envDuration("AUTH_TIMEOUT", 30*time.Second),
		HeartbeatTimeout: envDuration("HEARTBEAT_TIMEOUT", 3*time.Minute),
		WriteTimeout:     envDuration("WRITE_TIMEOUT", 10*time.Second),

		DBEnabled:      dbEnabled,
		DBHost:         envStr("DB_HOST", "mysql"),
		DBPort:         envStr("DB_PORT", "3306"),
		DBUser:         envStr("DB_USERNAME", "laravel"),
		DBPassword:     envStr("DB_PASSWORD", ""),
		DBName:         envStr("DB_DATABASE", "laravel"),
		DBDeviceTypeID: envInt64("DB_DEVICE_TYPE_ID", 3),

		DBDevicesTable:    envStr("DB_DEVICES_TABLE", "devices"),
		DBIMEIColumn:      envStr("DB_IMEI_COLUMN", "imei"),
		DBBroadcastColumn: envStr("DB_BROADCAST_COLUMN", "broadcast_id"),
		DBStatusColumn:    envStr("DB_STATUS_COLUMN", "status"),
		DBTypeIDColumn:    envStr("DB_TYPE_ID_COLUMN", "device_type_id"),
		DBNameColumn:      envStr("DB_NAME_COLUMN", "name"),
		DBNotesColumn:     envStr("DB_NOTES_COLUMN", "notes"),
		DBCreatedAtColumn: envStr("DB_CREATED_AT_COLUMN", "created_at"),
		DBUpdatedAtColumn: envStr("DB_UPDATED_AT_COLUMN", "updated_at"),

		Debug:    envStr("APP_DEBUG", "false") == "true",
		ServerID: envStr("SERVER_ID", mustHostname()),
	}
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func mustHostname() string {
	h, _ := os.Hostname()
	return h
}
