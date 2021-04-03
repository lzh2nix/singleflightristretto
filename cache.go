package singleflightristretto

import (
	"context"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/dgraph-io/ristretto"
)

// A Getter loads data for a key.
type Getter interface {
	// return value, cost, TTL for given key
	Get(ctx context.Context, key string) (interface{}, int64, time.Duration, error)
}

// A GetterFunc implements Getter with a function.
type GetterFunc func(ctx context.Context, key string) (interface{}, int64, time.Duration, error)

func (f GetterFunc) Get(ctx context.Context, key string) (interface{}, int64, time.Duration, error) {
	return f(ctx, key)
}

// config for cache
type Config struct {
	ristretto.Config
}

type Cache struct {
	ristretto *ristretto.Cache    // backend cache
	getter    Getter              // get function, load data when miss
	sg        *singleflight.Group // singleflight for load
}

// just ristretto cache
func New(cfg *Config) (*Cache, error) {

	rist, err := ristretto.NewCache(&cfg.Config)
	if err != nil {
		return nil, err
	}
	return &Cache{ristretto: rist}, nil
}

// ristretto cache with Getter function when cache miss
func NewWithGetter(cfg *Config, getter Getter) (*Cache, error) {

	rist, err := ristretto.NewCache(&cfg.Config)
	if err != nil {
		return nil, err
	}
	return &Cache{ristretto: rist, getter: getter, sg: &singleflight.Group{}}, nil
}

// get value from cache, if getter is setted then call getter
func (c *Cache) Get(key string) (interface{}, bool) {
	v, found := c.ristretto.Get(key)
	if found && v != nil {
		return v, true
	}
	if c.getter != nil {
		return c.load(key)
	}
	return nil, false
}

// singleflight load
func (c *Cache) load(key string) (interface{}, bool) {
	v, err, _ := c.sg.Do(key, func() (interface{}, error) {
		v, cost, ttl, err := c.getter.Get(context.Background(), key)
		if err != nil {
			return nil, err
		}
		if ttl != 0 {
			c.SetWithTTL(key, v, cost, ttl)
		} else {
			c.Set(key, v, cost)
		}
		return v, nil
	})
	if err == nil && v != nil {
		return v, true
	}
	return nil, false
}

// attempts add key,vlaue to store(maybe drop even if return true)
func (c *Cache) Set(key string, value interface{}, cost int64) bool {
	return c.ristretto.Set(key, value, cost)
}

// set with ttl
func (c *Cache) SetWithTTL(key string, value interface{}, cost int64, ttl time.Duration) bool {

	return c.ristretto.SetWithTTL(key, value, cost, ttl)
}

// delete from cache
func (c *Cache) Del(key string) {
	c.ristretto.Del(key)
}

// close cache
func (c *Cache) Close() {
	c.ristretto.Close()
}
