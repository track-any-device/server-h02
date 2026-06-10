package session

import (
	"context"
	"fmt"
	"h02-server/server/internal/config"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Registry manages the local TCP session map and synchronises presence state to Redis.
type Registry struct {
	mu      sync.RWMutex
	local   map[string]*Session // keyed by IMEI
	rdb     *redis.Client
	log     *zap.Logger
	prefix  string
	onlineZ string
}

func NewRegistry(cfg *config.Config, rdb *redis.Client, log *zap.Logger) *Registry {
	return &Registry{
		local:   make(map[string]*Session),
		rdb:     rdb,
		log:     log,
		prefix:  cfg.SessionPrefix,
		onlineZ: cfg.OnlineZKey,
	}
}

// Register adds a session to the local map and syncs to Redis.
// If the same IMEI already has an active session, the old connection is closed.
func (r *Registry) Register(ctx context.Context, s *Session) {
	r.mu.Lock()
	if old, ok := r.local[s.IMEI]; ok && old != s {
		old.Close()
		delete(r.local, s.IMEI)
	}
	r.local[s.IMEI] = s
	r.mu.Unlock()

	go func() {
		key := r.prefix + s.IMEI
		now := time.Now()
		pipe := r.rdb.Pipeline()
		pipe.HSet(ctx, key,
			"imei", s.IMEI,
			"transport", "tcp",
			"connected_at", now.Unix(),
			"last_heartbeat", now.Unix(),
		)
		pipe.Expire(ctx, key, 24*time.Hour)
		pipe.ZAdd(ctx, r.onlineZ, redis.Z{Score: float64(now.UnixNano()), Member: s.IMEI})
		if _, err := pipe.Exec(ctx); err != nil {
			r.log.Warn("registry: redis register failed", zap.String("imei", s.IMEI), zap.Error(err))
		}
	}()
}

// Unregister removes a session from the local map and clears it from Redis.
func (r *Registry) Unregister(ctx context.Context, s *Session) {
	if s.IMEI == "" {
		return
	}
	r.mu.Lock()
	if r.local[s.IMEI] == s {
		delete(r.local, s.IMEI)
	}
	r.mu.Unlock()

	go func() {
		pipe := r.rdb.Pipeline()
		pipe.Del(ctx, r.prefix+s.IMEI)
		pipe.ZRem(ctx, r.onlineZ, s.IMEI)
		if _, err := pipe.Exec(ctx); err != nil {
			r.log.Warn("registry: redis unregister failed", zap.String("imei", s.IMEI), zap.Error(err))
		}
	}()
}

// Heartbeat refreshes the session TTL in Redis.
func (r *Registry) Heartbeat(ctx context.Context, s *Session) {
	s.Touch()
	go func() {
		now := time.Now()
		pipe := r.rdb.Pipeline()
		pipe.HSet(ctx, r.prefix+s.IMEI, "last_heartbeat", now.Unix())
		pipe.Expire(ctx, r.prefix+s.IMEI, 24*time.Hour)
		pipe.ZAdd(ctx, r.onlineZ, redis.Z{Score: float64(now.UnixNano()), Member: s.IMEI})
		if _, err := pipe.Exec(ctx); err != nil {
			r.log.Warn("registry: redis heartbeat failed", zap.String("imei", s.IMEI), zap.Error(err))
		}
	}()
}

func (r *Registry) Get(imei string) *Session {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.local[imei]
}

func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.local)
}

// PruneStale removes stale IMEI entries from the Redis online sorted set.
func (r *Registry) PruneStale(ctx context.Context, ttl time.Duration) error {
	cutoff := float64(time.Now().Add(-ttl).UnixNano())
	return r.rdb.ZRemRangeByScore(ctx, r.onlineZ, "-inf", fmt.Sprintf("%f", cutoff)).Err()
}
