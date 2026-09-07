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
	"bytes"
	"context"
	"errors"
	"reflect"
	"time"

	swytchcomponent "github.com/dapr/components-contrib/common/component/swytch"
	"github.com/dapr/components-contrib/lock"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/swytchdb/engine/effects"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxUnlockAttempts = 3

// LockStore is a Dapr distributed lock store backed by Swytch scalar effects.
type LockStore struct {
	runtime *swytchcomponent.Runtime
}

func NewSwytchLockStore(log logger.Logger) lock.Store {
	return &LockStore{runtime: swytchcomponent.NewRuntime(log)}
}

func (s *LockStore) InitLockStore(ctx context.Context, meta lock.Metadata) error {
	return s.runtime.Start(ctx, meta.Base)
}

// TryLock atomically creates an expiring owner value when the resource is not
// currently locked.
func (s *LockStore) TryLock(ctx context.Context, req *lock.TryLockRequest) (*lock.TryLockResponse, error) {
	if req == nil {
		return nil, errors.New("try-lock request is nil")
	}
	if req.ResourceID == "" {
		return nil, errors.New("resource ID is empty")
	}
	if req.LockOwner == "" {
		return nil, errors.New("lock owner is empty")
	}
	if req.ExpiryInSeconds <= 0 {
		return nil, errors.New("expiryInSeconds must be positive")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	engine, release, err := s.runtime.Acquire()
	if err != nil {
		return nil, err
	}
	defer release()

	unlock := swytchcomponent.LockKeys(engine, []string{req.ResourceID})
	defer unlock()

	engineCtx := engine.NewContext()
	engineCtx.SetTraceCtx(ctx)
	engineCtx.BeginTx()
	snap, tips, err := engineCtx.GetSnapshot(req.ResourceID)
	if err != nil {
		engineCtx.Abort()
		return nil, err
	}
	if swytchcomponent.SnapshotExists(snap) {
		engineCtx.Abort()
		return &lock.TryLockResponse{Success: false}, nil
	}
	if err = swytchcomponent.EmitScalar(engineCtx, req.ResourceID, []byte(req.LockOwner), tips); err != nil {
		engineCtx.Abort()
		return nil, err
	}
	expiresAt := timestamppb.New(time.Now().Add(time.Duration(req.ExpiryInSeconds) * time.Second))
	if err = swytchcomponent.EmitExpiration(engineCtx, req.ResourceID, expiresAt); err != nil {
		engineCtx.Abort()
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		engineCtx.Abort()
		return nil, err
	}
	if err = engineCtx.Flush(); err != nil {
		if errors.Is(err, effects.ErrTxnAborted) {
			return &lock.TryLockResponse{Success: false}, nil
		}
		return nil, err
	}
	return &lock.TryLockResponse{Success: true}, nil
}

// Unlock removes a lock only when its current owner matches the request.
func (s *LockStore) Unlock(ctx context.Context, req *lock.UnlockRequest) (*lock.UnlockResponse, error) {
	if req == nil {
		return nil, errors.New("unlock request is nil")
	}
	if req.ResourceID == "" {
		return nil, errors.New("resource ID is empty")
	}
	if req.LockOwner == "" {
		return nil, errors.New("lock owner is empty")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	engine, release, err := s.runtime.Acquire()
	if err != nil {
		return nil, err
	}
	defer release()

	unlock := swytchcomponent.LockKeys(engine, []string{req.ResourceID})
	defer unlock()

	for range maxUnlockAttempts {
		response, retry, unlockErr := unlockOnce(ctx, engine, req)
		if !retry {
			return response, unlockErr
		}
	}
	return &lock.UnlockResponse{Status: lock.InternalError}, effects.ErrTxnAborted
}

func unlockOnce(ctx context.Context, engine *effects.Engine, req *lock.UnlockRequest) (*lock.UnlockResponse, bool, error) {
	engineCtx := engine.NewContext()
	engineCtx.SetTraceCtx(ctx)
	engineCtx.BeginTx()
	snap, tips, err := engineCtx.GetSnapshot(req.ResourceID)
	if err != nil {
		engineCtx.Abort()
		return nil, false, err
	}
	if !swytchcomponent.SnapshotExists(snap) {
		engineCtx.Abort()
		return &lock.UnlockResponse{Status: lock.LockDoesNotExist}, false, nil
	}
	if !bytes.Equal(swytchcomponent.ScalarValue(snap), []byte(req.LockOwner)) {
		engineCtx.Abort()
		return &lock.UnlockResponse{Status: lock.LockBelongsToOthers}, false, nil
	}
	if err = swytchcomponent.EmitDelete(engineCtx, req.ResourceID, tips); err != nil {
		engineCtx.Abort()
		return nil, false, err
	}
	if err = ctx.Err(); err != nil {
		engineCtx.Abort()
		return nil, false, err
	}
	if err = engineCtx.Flush(); err != nil {
		if errors.Is(err, effects.ErrTxnAborted) {
			return nil, true, nil
		}
		return nil, false, err
	}
	return &lock.UnlockResponse{Status: lock.Success}, false, nil
}

func (s *LockStore) Close() error {
	if s.runtime == nil {
		return nil
	}
	return s.runtime.Close()
}

func (s *LockStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(swytchcomponent.Settings{}), &metadataInfo, metadata.LockStoreType)
	return metadataInfo
}

var _ lock.Store = (*LockStore)(nil)
