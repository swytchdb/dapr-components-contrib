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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/query"
	"github.com/dapr/kit/logger"
)

func newTestStore(t *testing.T) *SwytchStore {
	t.Helper()
	store := NewSwytchStore(logger.NewLogger("swytch-test"))
	require.NoError(t, store.Init(t.Context(), state.Metadata{
		Base: metadata.Base{Properties: map[string]string{"memory-limit": "80%"}},
	}))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	return store
}

func TestCRUDAndETags(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	require.NoError(t, store.Set(ctx, &state.SetRequest{Key: "key", Value: []byte("one")}))
	first, err := store.Get(ctx, &state.GetRequest{Key: "key"})
	require.NoError(t, err)
	require.Equal(t, []byte("one"), first.Data)
	require.NotNil(t, first.ETag)

	require.NoError(t, store.Set(ctx, &state.SetRequest{
		Key:   "key",
		Value: []byte("two"),
		ETag:  first.ETag,
		Options: state.SetStateOption{
			Concurrency: state.FirstWrite,
		},
	}))
	second, err := store.Get(ctx, &state.GetRequest{Key: "key"})
	require.NoError(t, err)
	require.Equal(t, []byte("two"), second.Data)
	require.NotEqual(t, *first.ETag, *second.ETag)

	err = store.Set(ctx, &state.SetRequest{
		Key:   "key",
		Value: []byte("stale"),
		ETag:  first.ETag,
		Options: state.SetStateOption{
			Concurrency: state.FirstWrite,
		},
	})
	var etagErr *state.ETagError
	require.ErrorAs(t, err, &etagErr)
	assert.Equal(t, state.ETagMismatch, etagErr.Kind())

	require.NoError(t, store.Delete(ctx, &state.DeleteRequest{Key: "key", ETag: second.ETag}))
	deleted, err := store.Get(ctx, &state.GetRequest{Key: "key"})
	require.NoError(t, err)
	assert.Empty(t, deleted.Data)
	assert.Nil(t, deleted.ETag)
}

func TestTTL(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	require.True(t, state.FeatureTTL.IsPresent(store.Features()))
	require.Error(t, store.Set(ctx, &state.SetRequest{
		Key:      "invalid-ttl",
		Value:    []byte("value"),
		Metadata: map[string]string{"ttlInSeconds": "invalid"},
	}))
	require.Error(t, store.Set(ctx, &state.SetRequest{
		Key:      "negative-ttl",
		Value:    []byte("value"),
		Metadata: map[string]string{"ttlInSeconds": "-2"},
	}))
	err := store.Multi(ctx, &state.TransactionalStateRequest{
		Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "must-not-commit", Value: []byte("value")},
			state.SetRequest{
				Key:      "invalid-transaction-ttl",
				Value:    []byte("value"),
				Metadata: map[string]string{"ttlInSeconds": "invalid"},
			},
		},
	})
	require.Error(t, err)
	notCommitted, err := store.Get(ctx, &state.GetRequest{Key: "must-not-commit"})
	require.NoError(t, err)
	assert.Empty(t, notCommitted.Data)

	for _, key := range []string{"expires", "persist-minus-one", "persist-omitted"} {
		require.NoError(t, store.Set(ctx, &state.SetRequest{
			Key:      key,
			Value:    []byte("value"),
			Metadata: map[string]string{"ttlInSeconds": "1"},
		}))
	}
	require.NoError(t, store.Multi(ctx, &state.TransactionalStateRequest{
		Operations: []state.TransactionalStateOperation{
			state.SetRequest{
				Key:      "transaction-expires",
				Value:    []byte("value"),
				Metadata: map[string]string{"ttlInSeconds": "1"},
			},
		},
	}))

	responses, err := store.BulkGet(ctx, []state.GetRequest{
		{Key: "expires"},
		{Key: "transaction-expires"},
	}, state.BulkGetOpts{})
	require.NoError(t, err)
	require.Len(t, responses, 2)
	for _, response := range responses {
		expireText := response.Metadata[state.GetRespMetaKeyTTLExpireTime]
		require.NotEmpty(t, expireText)
		expiresAt, parseErr := time.Parse(time.RFC3339, expireText)
		require.NoError(t, parseErr)
		assert.WithinDuration(t, time.Now().Add(time.Second), expiresAt, 2*time.Second)
	}

	require.NoError(t, store.Set(ctx, &state.SetRequest{
		Key:      "persist-minus-one",
		Value:    []byte("value"),
		Metadata: map[string]string{"ttlInSeconds": "-1"},
	}))
	require.NoError(t, store.Set(ctx, &state.SetRequest{
		Key:   "persist-omitted",
		Value: []byte("value"),
	}))

	require.Eventually(t, func() bool {
		for _, key := range []string{"expires", "transaction-expires"} {
			response, getErr := store.Get(ctx, &state.GetRequest{Key: key})
			if getErr != nil || response.Data != nil {
				return false
			}
		}
		return true
	}, 3*time.Second, 25*time.Millisecond)

	for _, key := range []string{"persist-minus-one", "persist-omitted"} {
		response, getErr := store.Get(ctx, &state.GetRequest{Key: key})
		require.NoError(t, getErr)
		assert.Equal(t, []byte("value"), response.Data)
		assert.NotContains(t, response.Metadata, state.GetRespMetaKeyTTLExpireTime)
	}
}

func TestMultiIsAtomicOnValidationFailure(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	require.NoError(t, store.Set(ctx, &state.SetRequest{Key: "guard", Value: []byte("before")}))
	guard, err := store.Get(ctx, &state.GetRequest{Key: "guard"})
	require.NoError(t, err)
	require.NotNil(t, guard.ETag)

	badETag := "not-the-current-etag"
	err = store.Multi(ctx, &state.TransactionalStateRequest{
		Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "new", Value: []byte("must-not-commit")},
			state.SetRequest{
				Key:   "guard",
				Value: []byte("after"),
				ETag:  &badETag,
				Options: state.SetStateOption{
					Concurrency: state.FirstWrite,
				},
			},
		},
	})
	require.Error(t, err)

	missing, err := store.Get(ctx, &state.GetRequest{Key: "new"})
	require.NoError(t, err)
	assert.Empty(t, missing.Data)
	unchanged, err := store.Get(ctx, &state.GetRequest{Key: "guard"})
	require.NoError(t, err)
	assert.Equal(t, []byte("before"), unchanged.Data)
	assert.Equal(t, *guard.ETag, *unchanged.ETag)

	require.NoError(t, store.Multi(ctx, &state.TransactionalStateRequest{
		Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "first", Value: map[string]any{"value": 1}},
			state.SetRequest{Key: "second", Value: map[string]any{"value": 2}},
		},
	}))
	first, err := store.Get(ctx, &state.GetRequest{Key: "first"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"value":1}`, string(first.Data))
	second, err := store.Get(ctx, &state.GetRequest{Key: "second"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"value":2}`, string(second.Data))

	// Repeated keys share an engine lock stripe and must still execute without
	// deadlocking; later operations observe earlier ones in the transaction.
	require.NoError(t, store.Multi(ctx, &state.TransactionalStateRequest{
		Operations: []state.TransactionalStateOperation{
			state.SetRequest{Key: "same", Value: []byte("one")},
			state.SetRequest{Key: "same", Value: []byte("two")},
		},
	}))
	same, err := store.Get(ctx, &state.GetRequest{Key: "same"})
	require.NoError(t, err)
	assert.Equal(t, []byte("two"), same.Data)
}

func TestDeleteWithPrefixAndKeysLike(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	for _, key := range []string{"actor||a", "actor||b", "actor||nested||c", "other||a"} {
		require.NoError(t, store.Set(ctx, &state.SetRequest{Key: key, Value: []byte(key)}))
	}

	pageSize := uint32(1)
	firstPage, err := store.KeysLike(ctx, &state.KeysLikeRequest{Pattern: "actor||%", PageSize: &pageSize})
	require.NoError(t, err)
	require.Len(t, firstPage.Keys, 1)
	require.NotNil(t, firstPage.ContinuationToken)
	secondPage, err := store.KeysLike(ctx, &state.KeysLikeRequest{
		Pattern:           "actor||%",
		PageSize:          &pageSize,
		ContinuationToken: firstPage.ContinuationToken,
	})
	require.NoError(t, err)
	require.Len(t, secondPage.Keys, 1)

	result, err := store.DeleteWithPrefix(ctx, state.DeleteWithPrefixRequest{Prefix: "actor"})
	require.NoError(t, err)
	assert.EqualValues(t, 2, result.Count)

	for _, key := range []string{"actor||a", "actor||b"} {
		response, getErr := store.Get(ctx, &state.GetRequest{Key: key})
		require.NoError(t, getErr)
		assert.Empty(t, response.Data)
	}
	nested, err := store.Get(ctx, &state.GetRequest{Key: "actor||nested||c"})
	require.NoError(t, err)
	assert.NotEmpty(t, nested.Data)
}

func TestQueryFiltersSortsAndPaginatesJSON(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	documents := map[string]map[string]any{
		"a": {"group": "x", "score": 2},
		"b": {"group": "x", "score": 1},
		"c": {"group": "y", "score": 3},
	}
	for key, document := range documents {
		require.NoError(t, store.Set(ctx, &state.SetRequest{Key: key, Value: document}))
	}

	request := &state.QueryRequest{Query: query.Query{
		Filter: &query.EQ{Key: "group", Val: "x"},
		QueryFields: query.QueryFields{
			Sort: []query.Sorting{{Key: "score", Order: query.DESC}},
			Page: query.Pagination{Limit: 1},
		},
	}}
	first, err := store.Query(ctx, request)
	require.NoError(t, err)
	require.Len(t, first.Results, 1)
	assert.Equal(t, "a", first.Results[0].Key)
	require.NotEmpty(t, first.Token)

	request.Query.Page.Token = first.Token
	second, err := store.Query(ctx, request)
	require.NoError(t, err)
	require.Len(t, second.Results, 1)
	assert.Equal(t, "b", second.Results[0].Key)
	assert.Empty(t, second.Token)
}

func TestQueryFilterEvaluation(t *testing.T) {
	value := map[string]any{
		"name":   "Ada",
		"active": true,
		"score":  float64(42),
		"profile": map[string]any{
			"team": "engine",
		},
	}
	tests := []struct {
		name   string
		filter query.Filter
		match  bool
	}{
		{name: "nested equality", filter: &query.EQ{Key: "profile.team", Val: "engine"}, match: true},
		{name: "numeric greater than", filter: &query.GT{Key: "score", Val: 40}, match: true},
		{name: "in", filter: &query.IN{Key: "name", Vals: []any{"Grace", "Ada"}}, match: true},
		{name: "and", filter: &query.AND{Filters: []query.Filter{
			&query.EQ{Key: "active", Val: true},
			&query.LTE{Key: "score", Val: 42},
		}}, match: true},
		{name: "or miss", filter: &query.OR{Filters: []query.Filter{
			&query.EQ{Key: "name", Val: "Grace"},
			&query.NEQ{Key: "profile.team", Val: "engine"},
		}}, match: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			match, err := matchesFilter(value, test.filter)
			require.NoError(t, err)
			assert.Equal(t, test.match, match)
		})
	}
}

func TestParseMetadata(t *testing.T) {
	t.Run("defaults to local mode", func(t *testing.T) {
		cfg, err := parseMetadata(state.Metadata{})
		require.NoError(t, err)
		assert.Equal(t, 0.8, cfg.MemoryLimitPercent)
		assert.Zero(t, cfg.ClusterPort)
		assert.False(t, cfg.AsyncJoin)
	})

	t.Run("configures DNS cluster", func(t *testing.T) {
		cfg, err := parseMetadata(state.Metadata{Base: metadata.Base{Properties: map[string]string{
			"memory-limit":    "32mb",
			"discovery":       "swytch.default.svc.cluster.local",
			"clusterPassword": "secret",
			"clusterPort":     "9000",
			"compress":        "true",
		}}})
		require.NoError(t, err)
		assert.EqualValues(t, 32*1024*1024, cfg.MemoryLimit)
		assert.Equal(t, 9000, cfg.ClusterPort)
		assert.False(t, cfg.AsyncJoin)
		assert.True(t, cfg.CompressValues)
	})

	t.Run("rejects conflicting cloud credentials", func(t *testing.T) {
		_, err := parseMetadata(state.Metadata{Base: metadata.Base{Properties: map[string]string{
			"cloud":           "connection-secret",
			"clusterPassword": "cluster-secret",
		}}})
		assert.True(t, errors.Is(err, errCloudExclusive))
	})

	t.Run("requires a password for DNS discovery", func(t *testing.T) {
		_, err := parseMetadata(state.Metadata{Base: metadata.Base{Properties: map[string]string{
			"discovery": "swytch.default.svc.cluster.local",
		}}})
		assert.True(t, errors.Is(err, errDiscoveryRequiresPassword))
	})
}

func TestCanceledContextAndClose(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, store.Set(ctx, &state.SetRequest{Key: "key", Value: "value"}), context.Canceled)
}
