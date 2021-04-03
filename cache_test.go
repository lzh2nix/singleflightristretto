package singleflightristretto_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lzh2nix/singleflightristretto"
	"github.com/stretchr/testify/assert"
)

func TestWithNewCache(t *testing.T) {
	ast := assert.New(t)

	var cfg singleflightristretto.Config
	cfg.Metrics = false
	cfg.NumCounters = 1e7 // 10M
	cfg.MaxCost = 1 << 30 // 1G
	cfg.BufferItems = 64
	cfg.Metrics = false
	cache, err := singleflightristretto.New(&cfg)
	ast.Nil(err)

	// set a value with a cost of 1
	cache.Set("key", "value", 1)
	// wait for value to pass through buffers
	time.Sleep(10 * time.Millisecond)
	value, found := cache.Get("key")
	ast.True(found)
	ast.Equal("value", value)
	cache.Del("key")

	// set a value with a cost of 1
	cache.SetWithTTL("key2", "value2", 1, time.Millisecond*30)
	// wait for value to pass through buffers
	time.Sleep(10 * time.Millisecond)
	value, found = cache.Get("key2")
	ast.True(found)
	ast.Equal("value2", value)
	time.Sleep(25 * time.Millisecond)
	_, found = cache.Get("key2")
	ast.False(found)
	cache.Close()
}

type MockBackend struct {
	m map[string]int64
	sync.Mutex
}

func NewMockBackend() *MockBackend {
	return &MockBackend{m: map[string]int64{}}
}
func (mock *MockBackend) Get(ctx context.Context, key string) (interface{}, int64, time.Duration, error) {
	mock.Lock()
	defer mock.Unlock()
	if v, e := mock.m[key]; e {
		mock.m[key] = v + 1
	} else {
		mock.m[key] = 0
	}
	return mock.m[key], 1, time.Millisecond * 10, nil
}

//
func TestWithNewCaheWithGetter(t *testing.T) {
	ast := assert.New(t)

	var cfg singleflightristretto.Config
	cfg.Metrics = false
	cfg.NumCounters = 1e7 // 10M
	cfg.MaxCost = 1 << 30 // 1G
	cfg.BufferItems = 64
	cfg.Metrics = false
	b := NewMockBackend()
	cache, err := singleflightristretto.NewWithGetter(&cfg, b)
	ast.Nil(err)

	value, found := cache.Get("key2")
	ast.True(found)
	ast.Equal(int64(0), value)
	time.Sleep(time.Millisecond * 30)
	value, found = cache.Get("key2")
	ast.True(found)
	ast.Equal(int64(1), value)
	cache.Del("key2")

	var wg sync.WaitGroup
	// test for singleflight
	n := 10000
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cache.Get("key3")
			time.Sleep(time.Millisecond * 15)
			cache.Get("key3")
			time.Sleep(time.Millisecond * 20)
			cache.Get("key3")
			time.Sleep(time.Millisecond * 25)
			cache.Get("key3")
			time.Sleep(time.Millisecond * 15)
			cache.Get("key3")

		}()
	}
	wg.Wait()
	// 2/1000 gothrough the cache(may be ristretto set fail or evicted)
	ast.Greater(int64(20), b.m["key3"]-5)
}
