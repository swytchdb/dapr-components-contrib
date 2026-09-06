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
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dapr/components-contrib/state"
	stateutils "github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	"github.com/swytchdb/engine/beacon"
	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var errNotInitialized = errors.New("swytch state store is not initialized")

// SwytchStore is a Dapr state store backed by the Swytch effects engine.
type SwytchStore struct {
	state.BulkStore

	log logger.Logger

	runtimeMu sync.RWMutex
	runtime   *beacon.Runtime
}

// NewSwytchStateStore returns a Swytch state store as a Dapr state.Store.
func NewSwytchStateStore(log logger.Logger) state.Store {
	return NewSwytchStore(log)
}

// NewSwytchStore returns a new Swytch state store.
func NewSwytchStore(log logger.Logger) *SwytchStore {
	store := &SwytchStore{log: log}
	store.BulkStore = state.NewDefaultBulkStore(store)
	return store
}

// Init starts the embedded Swytch runtime.
func (s *SwytchStore) Init(ctx context.Context, metadata state.Metadata) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	cfg, err := parseMetadata(metadata)
	if err != nil {
		return err
	}

	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	if s.runtime != nil {
		return errors.New("swytch state store is already initialized")
	}

	// Engine v1.0.3 does not yet accept a context. Keep the join synchronous:
	// serving before convergence would violate Dapr transaction/ETag semantics.
	runtime, err := beacon.NewRuntime(cfg)
	if err != nil {
		return fmt.Errorf("start swytch runtime: %w", err)
	}
	if err = ctx.Err(); err != nil {
		return errors.Join(err, runtime.Stop())
	}
	s.runtime = runtime
	s.log.Infof("Swytch state store initialized with node ID %d", runtime.Engine.NodeID())
	return nil
}

// Features returns the capabilities implemented by this store.
func (s *SwytchStore) Features() []state.Feature {
	return []state.Feature{
		state.FeatureTransactional,
		state.FeatureETag,
		state.FeatureDeleteWithPrefix,
		state.FeatureKeysLike,
		state.FeatureQueryAPI,
		state.FeatureTTL,
	}
}

// Get returns the current scalar value for a key.
func (s *SwytchStore) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	if req == nil {
		return nil, errors.New("get request is nil")
	}
	if err := state.CheckRequestOptions(req.Options); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	if s.runtime == nil {
		return nil, errNotInitialized
	}

	lock := s.runtime.Engine.GetLock(req.Key)
	lock.Lock()
	defer lock.Unlock()

	snap, tips, err := s.runtime.Engine.NewReadOnlyContext().GetSnapshot(req.Key)
	if err != nil {
		return nil, err
	}
	return responseFromSnapshot(snap, tips), nil
}

// Set creates or replaces a scalar value.
func (s *SwytchStore) Set(ctx context.Context, req *state.SetRequest) error {
	if req == nil {
		return errors.New("set request is nil")
	}
	if err := state.CheckRequestOptions(req.Options); err != nil {
		return err
	}
	expiresAt, err := expirationFromMetadata(req.Metadata)
	if err != nil {
		return err
	}
	data, err := marshalValue(req.Value)
	if err != nil {
		return fmt.Errorf("marshal state value: %w", err)
	}
	if err = ctx.Err(); err != nil {
		return err
	}

	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	if s.runtime == nil {
		return errNotInitialized
	}

	unlock := lockEngineKeys(s.runtime.Engine, []string{req.Key})
	defer unlock()

	engineCtx := s.runtime.Engine.NewContext()
	engineCtx.SetTraceCtx(ctx)
	conditional := req.HasETag() || req.Options.Concurrency == state.FirstWrite
	if conditional {
		engineCtx.BeginTx()
	}

	snap, tips, err := engineCtx.GetSnapshot(req.Key)
	if err != nil {
		engineCtx.Abort()
		return err
	}
	if err = validateETag(req.Key, snap, tips, req.ETag, req.Options.Concurrency); err != nil {
		engineCtx.Abort()
		return err
	}
	if err = emitSet(engineCtx, req.Key, data, tips); err != nil {
		engineCtx.Abort()
		return err
	}
	if expiresAt != nil {
		if err = emitTTL(engineCtx, req.Key, expiresAt); err != nil {
			engineCtx.Abort()
			return err
		}
	}
	if err = ctx.Err(); err != nil {
		engineCtx.Abort()
		return err
	}
	if err = engineCtx.Flush(); err != nil {
		if conditional && errors.Is(err, effects.ErrTxnAborted) {
			return state.NewETagError(state.ETagMismatch, err)
		}
		return err
	}
	return nil
}

// Delete removes a scalar value. Deleting a missing key is idempotent unless
// the caller supplied an ETag that cannot match.
func (s *SwytchStore) Delete(ctx context.Context, req *state.DeleteRequest) error {
	if req == nil {
		return errors.New("delete request is nil")
	}
	if err := state.CheckRequestOptions(req.Options); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	if s.runtime == nil {
		return errNotInitialized
	}

	unlock := lockEngineKeys(s.runtime.Engine, []string{req.Key})
	defer unlock()

	engineCtx := s.runtime.Engine.NewContext()
	engineCtx.SetTraceCtx(ctx)
	conditional := req.HasETag() || req.Options.Concurrency == state.FirstWrite
	if conditional {
		engineCtx.BeginTx()
	}

	snap, tips, err := engineCtx.GetSnapshot(req.Key)
	if err != nil {
		engineCtx.Abort()
		return err
	}
	if err = validateETag(req.Key, snap, tips, req.ETag, req.Options.Concurrency); err != nil {
		engineCtx.Abort()
		return err
	}
	if !snapshotExists(snap) {
		engineCtx.Abort()
		return nil
	}
	if err = emitDelete(engineCtx, req.Key, tips); err != nil {
		engineCtx.Abort()
		return err
	}
	if err = ctx.Err(); err != nil {
		engineCtx.Abort()
		return err
	}
	if err = engineCtx.Flush(); err != nil {
		if conditional && errors.Is(err, effects.ErrTxnAborted) {
			return state.NewETagError(state.ETagMismatch, err)
		}
		return err
	}
	return nil
}

// Multi applies all requested operations in one Swytch transaction.
func (s *SwytchStore) Multi(ctx context.Context, request *state.TransactionalStateRequest) error {
	if request == nil {
		return errors.New("transaction request is nil")
	}
	operations, err := prepareOperations(request.Operations)
	if err != nil {
		return err
	}
	if len(operations) == 0 {
		return nil
	}
	if err = ctx.Err(); err != nil {
		return err
	}

	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	if s.runtime == nil {
		return errNotInitialized
	}

	keys := make([]string, len(operations))
	for i := range operations {
		keys[i] = operations[i].key
	}
	unlock := lockEngineKeys(s.runtime.Engine, keys)
	defer unlock()

	engineCtx := s.runtime.Engine.NewContext()
	engineCtx.SetTraceCtx(ctx)
	engineCtx.BeginTx()
	for i := range operations {
		if err = applyOperation(engineCtx, &operations[i]); err != nil {
			engineCtx.Abort()
			return fmt.Errorf("operation %d: %w", i, err)
		}
	}
	if err = ctx.Err(); err != nil {
		engineCtx.Abort()
		return err
	}
	return engineCtx.Flush()
}

// DeleteWithPrefix removes the direct children of an actor-style Dapr prefix.
func (s *SwytchStore) DeleteWithPrefix(ctx context.Context, req state.DeleteWithPrefixRequest) (state.DeleteWithPrefixResponse, error) {
	if err := req.Validate(); err != nil {
		return state.DeleteWithPrefixResponse{}, err
	}
	if err := ctx.Err(); err != nil {
		return state.DeleteWithPrefixResponse{}, err
	}

	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	if s.runtime == nil {
		return state.DeleteWithPrefixResponse{}, errNotInitialized
	}

	keys := s.runtime.Engine.MatchKeys("*")
	matched := make([]string, 0)
	for _, key := range keys {
		if !strings.HasPrefix(key, req.Prefix) || strings.Contains(key[len(req.Prefix):], "||") {
			continue
		}
		snap, _, err := s.runtime.Engine.NewReadOnlyContext().GetSnapshot(key)
		if err != nil {
			return state.DeleteWithPrefixResponse{}, err
		}
		if snapshotExists(snap) {
			matched = append(matched, key)
		}
	}
	if len(matched) == 0 {
		return state.DeleteWithPrefixResponse{}, nil
	}

	unlock := lockEngineKeys(s.runtime.Engine, matched)
	defer unlock()
	engineCtx := s.runtime.Engine.NewContext()
	engineCtx.SetTraceCtx(ctx)
	engineCtx.BeginTx()
	var count int64
	for _, key := range matched {
		snap, tips, err := engineCtx.GetSnapshot(key)
		if err != nil {
			engineCtx.Abort()
			return state.DeleteWithPrefixResponse{}, err
		}
		if !snapshotExists(snap) {
			continue
		}
		if err = emitDelete(engineCtx, key, tips); err != nil {
			engineCtx.Abort()
			return state.DeleteWithPrefixResponse{}, err
		}
		count++
	}
	if err := ctx.Err(); err != nil {
		engineCtx.Abort()
		return state.DeleteWithPrefixResponse{}, err
	}
	if err := engineCtx.Flush(); err != nil {
		return state.DeleteWithPrefixResponse{}, err
	}
	return state.DeleteWithPrefixResponse{Count: count}, nil
}

// KeysLike returns live keys matching a SQL LIKE pattern.
func (s *SwytchStore) KeysLike(ctx context.Context, req *state.KeysLikeRequest) (*state.KeysLikeResponse, error) {
	if req == nil {
		return nil, errors.New("keys-like request is nil")
	}
	if req.Pattern == "" {
		return nil, state.ErrKeysLikeEmptyPattern
	}
	glob, err := likeToGlob(req.Pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid pattern: %w", err)
	}

	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	if s.runtime == nil {
		return nil, errNotInitialized
	}

	keys := s.runtime.Engine.MatchKeys(glob)
	live := keys[:0]
	for _, key := range keys {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		snap, _, getErr := s.runtime.Engine.NewReadOnlyContext().GetSnapshot(key)
		if getErr != nil {
			return nil, getErr
		}
		if snapshotExists(snap) {
			live = append(live, key)
		}
	}
	keys = live

	start, err := parseOffsetToken(req.ContinuationToken, len(keys))
	if err != nil {
		return nil, err
	}
	end := len(keys)
	if req.PageSize != nil && *req.PageSize > 0 && uint64(start)+uint64(*req.PageSize) < uint64(end) {
		end = start + int(*req.PageSize)
	}

	var continuation *string
	if end < len(keys) {
		token := fmt.Sprintf("%d", end)
		continuation = &token
	}
	return &state.KeysLikeResponse{
		Keys:              keys[start:end],
		ContinuationToken: continuation,
	}, nil
}

// Close stops the embedded runtime after all active operations finish.
func (s *SwytchStore) Close() error {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	if s.runtime == nil {
		return nil
	}
	runtime := s.runtime
	s.runtime = nil
	return runtime.Stop()
}

type preparedOperation struct {
	kind        state.OperationType
	key         string
	data        []byte
	expiresAt   *timestamppb.Timestamp
	etag        *string
	concurrency string
}

func prepareOperations(operations []state.TransactionalStateOperation) ([]preparedOperation, error) {
	prepared := make([]preparedOperation, 0, len(operations))
	for i, operation := range operations {
		var item preparedOperation
		switch req := operation.(type) {
		case state.SetRequest:
			if err := state.CheckRequestOptions(req.Options); err != nil {
				return nil, fmt.Errorf("operation %d: %w", i, err)
			}
			expiresAt, err := expirationFromMetadata(req.Metadata)
			if err != nil {
				return nil, fmt.Errorf("operation %d: %w", i, err)
			}
			data, err := marshalValue(req.Value)
			if err != nil {
				return nil, fmt.Errorf("operation %d: marshal state value: %w", i, err)
			}
			item = preparedOperation{kind: state.OperationUpsert, key: req.Key, data: data, expiresAt: expiresAt, etag: req.ETag, concurrency: req.Options.Concurrency}
		case *state.SetRequest:
			if req == nil {
				return nil, fmt.Errorf("operation %d: set request is nil", i)
			}
			if err := state.CheckRequestOptions(req.Options); err != nil {
				return nil, fmt.Errorf("operation %d: %w", i, err)
			}
			expiresAt, err := expirationFromMetadata(req.Metadata)
			if err != nil {
				return nil, fmt.Errorf("operation %d: %w", i, err)
			}
			data, err := marshalValue(req.Value)
			if err != nil {
				return nil, fmt.Errorf("operation %d: marshal state value: %w", i, err)
			}
			item = preparedOperation{kind: state.OperationUpsert, key: req.Key, data: data, expiresAt: expiresAt, etag: req.ETag, concurrency: req.Options.Concurrency}
		case state.DeleteRequest:
			if err := state.CheckRequestOptions(req.Options); err != nil {
				return nil, fmt.Errorf("operation %d: %w", i, err)
			}
			item = preparedOperation{kind: state.OperationDelete, key: req.Key, etag: req.ETag, concurrency: req.Options.Concurrency}
		case *state.DeleteRequest:
			if req == nil {
				return nil, fmt.Errorf("operation %d: delete request is nil", i)
			}
			if err := state.CheckRequestOptions(req.Options); err != nil {
				return nil, fmt.Errorf("operation %d: %w", i, err)
			}
			item = preparedOperation{kind: state.OperationDelete, key: req.Key, etag: req.ETag, concurrency: req.Options.Concurrency}
		default:
			return nil, fmt.Errorf("operation %d has unsupported type %T", i, operation)
		}
		prepared = append(prepared, item)
	}
	return prepared, nil
}

func applyOperation(engineCtx *effects.Context, operation *preparedOperation) error {
	snap, tips, err := engineCtx.GetSnapshot(operation.key)
	if err != nil {
		return err
	}
	if err = validateETag(operation.key, snap, tips, operation.etag, operation.concurrency); err != nil {
		return err
	}
	switch operation.kind {
	case state.OperationUpsert:
		if err = emitSet(engineCtx, operation.key, operation.data, tips); err != nil {
			return err
		}
		if operation.expiresAt != nil {
			return emitTTL(engineCtx, operation.key, operation.expiresAt)
		}
		return nil
	case state.OperationDelete:
		if !snapshotExists(snap) {
			return nil
		}
		return emitDelete(engineCtx, operation.key, tips)
	default:
		return fmt.Errorf("unsupported operation %q", operation.kind)
	}
}

func emitSet(engineCtx *effects.Context, key string, data []byte, tips []effects.Tip) error {
	return engineCtx.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: data},
		}},
	}, tips)
}

func emitTTL(engineCtx *effects.Context, key string, expiresAt *timestamppb.Timestamp) error {
	return engineCtx.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{ExpiresAt: expiresAt}},
	})
}

func emitDelete(engineCtx *effects.Context, key string, tips []effects.Tip) error {
	return engineCtx.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
		}},
	}, tips)
}

func marshalValue(value any) ([]byte, error) {
	return stateutils.Marshal(value, json.Marshal)
}

func expirationFromMetadata(metadata map[string]string) (*timestamppb.Timestamp, error) {
	ttl, err := stateutils.ParseTTL(metadata)
	if err != nil {
		return nil, err
	}
	if ttl == nil || *ttl <= 0 {
		return nil, nil
	}
	return timestamppb.New(time.Now().Add(time.Duration(*ttl) * time.Second)), nil
}

func snapshotExists(snap *pb.ReducedEffect) bool {
	return snap != nil && snap.Scalar != nil
}

func responseFromSnapshot(snap *pb.ReducedEffect, tips []effects.Tip) *state.GetResponse {
	if !snapshotExists(snap) {
		return &state.GetResponse{}
	}
	data := snap.Scalar.Decompress()
	result := &state.GetResponse{Data: append([]byte(nil), data...)}
	if snap.ExpiresAt != nil {
		result.Metadata = map[string]string{
			state.GetRespMetaKeyTTLExpireTime: snap.ExpiresAt.AsTime().UTC().Format(time.RFC3339),
		}
	}
	if etag := snapshotETag(snap, tips); etag != "" {
		result.ETag = &etag
	}
	return result
}

func snapshotETag(snap *pb.ReducedEffect, tips []effects.Tip) string {
	if !snapshotExists(snap) {
		return ""
	}
	if len(tips) > 0 {
		ordered := append([]effects.Tip(nil), tips...)
		sort.Slice(ordered, func(i, j int) bool {
			if ordered[i][0] == ordered[j][0] {
				return ordered[i][1] < ordered[j][1]
			}
			return ordered[i][0] < ordered[j][0]
		})
		encoded := make([]byte, len(ordered)*16)
		for i, tip := range ordered {
			binary.BigEndian.PutUint64(encoded[i*16:], tip[0])
			binary.BigEndian.PutUint64(encoded[i*16+8:], tip[1])
		}
		return hex.EncodeToString(encoded)
	}
	if len(snap.ForkChoiceHash) > 0 {
		return hex.EncodeToString(snap.ForkChoiceHash)
	}
	if snap.Hlc != nil {
		return fmt.Sprintf("%d-%d-%d", snap.NodeId, snap.Hlc.Seconds, snap.Hlc.Nanos)
	}
	return ""
}

func validateETag(key string, snap *pb.ReducedEffect, tips []effects.Tip, requested *string, concurrency string) error {
	hasETag := requested != nil && *requested != ""
	if concurrency == state.FirstWrite && !hasETag && snapshotExists(snap) {
		return state.NewETagError(state.ETagMismatch, fmt.Errorf("state already exists for key %q and no ETag was supplied", key))
	}
	if !hasETag {
		return nil
	}
	actual := snapshotETag(snap, tips)
	if actual == "" || actual != *requested {
		return state.NewETagError(state.ETagMismatch, fmt.Errorf("ETag does not match state for key %q", key))
	}
	return nil
}

// lockEngineKeys takes each engine stripe at most once and in stripe order.
// Sorting by key alone is insufficient because distinct keys can hash to the
// same striped mutex.
func lockEngineKeys(engine *effects.Engine, keys []string) func() {
	stripeKeys := make(map[uint32]string, len(keys))
	for _, key := range keys {
		stripe := lockStripe(key)
		if _, ok := stripeKeys[stripe]; !ok {
			stripeKeys[stripe] = key
		}
	}
	stripes := make([]uint32, 0, len(stripeKeys))
	for stripe := range stripeKeys {
		stripes = append(stripes, stripe)
	}
	sort.Slice(stripes, func(i, j int) bool { return stripes[i] < stripes[j] })
	for _, stripe := range stripes {
		engine.GetLock(stripeKeys[stripe]).Lock()
	}
	return func() {
		for i := len(stripes) - 1; i >= 0; i-- {
			engine.GetLock(stripeKeys[stripes[i]]).Unlock()
		}
	}
}

func lockStripe(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return hash & 4095
}

func likeToGlob(pattern string) (string, error) {
	var result strings.Builder
	escaped := false
	for _, char := range pattern {
		if escaped {
			if strings.ContainsRune(`*?[\\`, char) {
				result.WriteByte('\\')
			}
			result.WriteRune(char)
			escaped = false
			continue
		}
		switch char {
		case '\\':
			escaped = true
		case '%':
			result.WriteByte('*')
		case '_':
			result.WriteByte('?')
		case '*', '?', '[':
			result.WriteByte('\\')
			result.WriteRune(char)
		default:
			result.WriteRune(char)
		}
	}
	if escaped {
		result.WriteString(`\\`)
	}
	return result.String(), nil
}

func parseOffsetToken(token *string, length int) (int, error) {
	if token == nil || *token == "" {
		return 0, nil
	}
	offset, err := strconv.Atoi(*token)
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("invalid continuation token %q", *token)
	}
	if offset > length {
		return length, nil
	}
	return offset, nil
}

var (
	_ state.Store              = (*SwytchStore)(nil)
	_ state.TransactionalStore = (*SwytchStore)(nil)
	_ state.DeleteWithPrefix   = (*SwytchStore)(nil)
	_ state.KeysLiker          = (*SwytchStore)(nil)
)
