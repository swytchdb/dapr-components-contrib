/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package swytch

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/swytchdb/engine/beacon"
	"github.com/swytchdb/engine/effects"
)

var ErrNotInitialized = errors.New("swytch runtime is not initialized")

type runtimeKey [sha256.Size]byte

type sharedRuntime struct {
	runtime *beacon.Runtime
	refs    int
}

var runtimePool = struct {
	sync.Mutex
	entries map[runtimeKey]*sharedRuntime
}{
	entries: make(map[runtimeKey]*sharedRuntime),
}

// Runtime owns one reference to a process-shared Swytch runtime and drains
// active component operations before releasing it.
type Runtime struct {
	log logger.Logger

	mu    sync.RWMutex
	key   runtimeKey
	entry *sharedRuntime
}

func NewRuntime(log logger.Logger) *Runtime {
	return &Runtime{log: log}
}

// Start parses shared component metadata and starts the embedded runtime.
func (r *Runtime) Start(ctx context.Context, meta metadata.Base) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cfg, err := ParseMetadata(meta)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entry != nil {
		return errors.New("swytch runtime is already initialized")
	}

	key := keyForConfig(cfg)
	entry, reused, err := acquireRuntime(ctx, key, cfg)
	if err != nil {
		return err
	}
	r.key = key
	r.entry = entry
	if reused {
		r.log.Infof("Reusing Swytch runtime with node ID %d", entry.runtime.Engine.NodeID())
	} else {
		r.log.Infof("Swytch runtime initialized with node ID %d", entry.runtime.Engine.NodeID())
	}
	return nil
}

// Acquire returns the engine and pins its runtime until release is called.
func (r *Runtime) Acquire() (*effects.Engine, func(), error) {
	r.mu.RLock()
	if r.entry == nil {
		r.mu.RUnlock()
		return nil, nil, ErrNotInitialized
	}
	return r.entry.runtime.Engine, r.mu.RUnlock, nil
}

// Close waits for active operations and stops the shared runtime after its
// final component releases it.
func (r *Runtime) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entry == nil {
		return nil
	}
	entry := r.entry
	key := r.key
	r.entry = nil
	r.key = runtimeKey{}
	return releaseRuntime(key, entry)
}

func acquireRuntime(ctx context.Context, key runtimeKey, cfg beacon.RuntimeConfig) (*sharedRuntime, bool, error) {
	runtimePool.Lock()
	defer runtimePool.Unlock()

	if entry := runtimePool.entries[key]; entry != nil {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		entry.refs++
		return entry, true, nil
	}

	// Engine v1.0.3 does not yet accept a context. Keep the join synchronous:
	// serving before convergence would violate component consistency semantics.
	runtime, err := beacon.NewRuntime(cfg)
	if err != nil {
		return nil, false, fmt.Errorf("start swytch runtime: %w", err)
	}
	if err = ctx.Err(); err != nil {
		return nil, false, errors.Join(err, runtime.Stop())
	}
	entry := &sharedRuntime{runtime: runtime, refs: 1}
	runtimePool.entries[key] = entry
	return entry, false, nil
}

func releaseRuntime(key runtimeKey, entry *sharedRuntime) error {
	runtimePool.Lock()
	defer runtimePool.Unlock()

	current := runtimePool.entries[key]
	if current != entry {
		return errors.New("swytch runtime reference is not registered")
	}
	entry.refs--
	if entry.refs > 0 {
		return nil
	}
	delete(runtimePool.entries, key)
	return entry.runtime.Stop()
}

func keyForConfig(cfg beacon.RuntimeConfig) runtimeKey {
	encoded, _ := json.Marshal(struct {
		MemoryLimit        int64
		MemoryLimitPercent float64
		ClusterPassphrase  string
		ConnectionSecret   string
		JoinAddr           string
		CompressValues     bool
		ClusterPort        int
		AdvertiseAddr      string
	}{
		MemoryLimit:        cfg.MemoryLimit,
		MemoryLimitPercent: cfg.MemoryLimitPercent,
		ClusterPassphrase:  cfg.ClusterPassphrase,
		ConnectionSecret:   cfg.ConnectionSecret,
		JoinAddr:           cfg.JoinAddr,
		CompressValues:     cfg.CompressValues,
		ClusterPort:        cfg.ClusterPort,
		AdvertiseAddr:      cfg.AdvertiseAddr,
	})
	return sha256.Sum256(encoded)
}
