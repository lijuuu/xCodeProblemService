package cache

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisCache struct {
	client *redis.Client
}

func NewRedisCache(addr, password string, db int) *RedisCache {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	return &RedisCache{client: client}
}

func (r *RedisCache) Set(key string, value interface{}, expiration time.Duration) error {
	log.Printf("Cache: Setting key '%s' with expiration %v", key, expiration)
	err := r.client.Set(context.Background(), key, value, expiration).Err()
	if err != nil {
		log.Printf("Cache ERROR: Failed to set key '%s': %v", key, err)
		return fmt.Errorf("failed to set key %s in cache: %v", key, err)
	}
	log.Printf("Cache: Successfully set key '%s'", key)
	return nil
}

func (r *RedisCache) Get(key string) (interface{}, error) {
	log.Printf("Cache: Attempting to get key '%s'", key)
	val, err := r.client.Get(context.Background(), key).Result()
	if err == redis.Nil {
		log.Printf("Cache MISS: Key '%s' not found", key)
		return nil, nil
	}
	if err != nil {
		log.Printf("Cache ERROR: Failed to get key '%s': %v", key, err)
		return nil, fmt.Errorf("failed to get key %s from cache: %v", key, err)
	}
	log.Printf("Cache HIT: Successfully retrieved key '%s'", key)
	return val, nil
}

func (r *RedisCache) Delete(key string) error {
	log.Printf("Cache: Attempting to delete key '%s'", key)
	err := r.client.Del(context.Background(), key).Err()
	if err != nil {
		log.Printf("Cache ERROR: Failed to delete key '%s': %v", key, err)
		return fmt.Errorf("failed to delete key %s from cache: %v", key, err)
	}
	log.Printf("Cache: Successfully deleted key '%s'", key)
	return nil
}

func (r *RedisCache) Exists(key string) (bool, error) {
	log.Printf("Cache: Checking existence of key '%s'", key)
	result, err := r.client.Exists(context.Background(), key).Result()
	if err != nil {
		log.Printf("Cache ERROR: Failed to check existence of key '%s': %v", key, err)
		return false, fmt.Errorf("failed to check existence of key %s in cache: %v", key, err)
	}
	exists := result > 0
	log.Printf("Cache: Key '%s' exists: %v", key, exists)
	return exists, nil
}
