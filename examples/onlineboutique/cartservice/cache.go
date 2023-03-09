// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cartservice

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ServiceWeaver/weaver"
	"github.com/redis/go-redis/v9"
)

type errNotFound struct{}

var _ error = errNotFound{}

func (e errNotFound) Error() string { return "not found" }

// TODO(spetrovic): Allow the cache struct to reside in a different package.

type CartCache interface {
	Add(context.Context, string, []CartItem) error
	Get(context.Context, string) ([]CartItem, error)
	Remove(context.Context, string) (bool, error)
}

type config struct {
	Host string `toml:"cache_host"` // Distributed cache host
	TTL  string `toml:"cache_ttl"`  // Distributed cache TTL
}

func (cfg *config) Validate() error {
	if cfg.Host != "" {
		return fmt.Errorf("distributed cache host must be set")
	}
	if _, err := time.ParseDuration(cfg.TTL); err != nil {
		return fmt.Errorf("distributed cache TTL must be a valid duration")
	}
	return nil
}

type cartCacheImpl struct {
	weaver.Implements[CartCache]
	weaver.WithConfig[config]

	redisCli *redis.Client
	ttl      time.Duration
}

func (c *cartCacheImpl) Init(ctx context.Context) (err error) {
	cfg := c.Config()
	c.ttl, err = time.ParseDuration(cfg.TTL)
	if err != nil {
		return err
	}
	c.redisCli = redis.NewClient(&redis.Options{
		Addr: cfg.Host,
	})
	err = c.redisCli.Ping(ctx).Err()
	return err
}

// Add adds the given (key, val) pair to the cache.
func (c *cartCacheImpl) Add(ctx context.Context, key string, val []CartItem) error {
	for _, item := range val {
		encItem, err := json.Marshal(item)
		if err != nil {
			return err
		}
		if err := c.redisCli.LPush(ctx, key, encItem).Err(); err != nil {
			return err
		}
	}
	if err := c.redisCli.Expire(ctx, key, c.ttl).Err(); err != nil {
		return err
	}
	return nil
}

// Get returns the value associated with the given key in the cache, or
// ErrNotFound if there is no associated value.
func (c *cartCacheImpl) Get(ctx context.Context, key string) ([]CartItem, error) {
	val, err := c.redisCli.LRange(ctx, key, 0, -1).Result()
	if err == redis.Nil {
		return nil, errNotFound{}
	}
	cart := make([]CartItem, len(val))
	for i, v := range val {
		var item CartItem
		err := json.Unmarshal([]byte(v), &item)
		if err != nil {
			return nil, err
		}
		cart[i] = item
	}

	return cart, nil
}

// Remove removes an entry with the given key from the cache.
func (c *cartCacheImpl) Remove(ctx context.Context, key string) (bool, error) {
	err := c.redisCli.Del(ctx, key).Err()
	if err != nil {
		return false, err
	}
	return true, nil
}
