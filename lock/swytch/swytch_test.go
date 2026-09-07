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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	swytchcomponent "github.com/dapr/components-contrib/common/component/swytch"
	"github.com/dapr/components-contrib/lock"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func newTestLockStore(t *testing.T) *LockStore {
	t.Helper()
	store := NewSwytchLockStore(logger.NewLogger("swytch-lock-test")).(*LockStore)
	require.NoError(t, store.InitLockStore(t.Context(), lock.Metadata{
		Base: metadata.Base{Properties: map[string]string{"memory-limit": "80%"}},
	}))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	return store
}

func TestTryLockAndUnlock(t *testing.T) {
	store := newTestLockStore(t)

	acquired, err := store.TryLock(t.Context(), &lock.TryLockRequest{
		ResourceID:      "resource",
		LockOwner:       "owner",
		ExpiryInSeconds: 30,
	})
	require.NoError(t, err)
	require.True(t, acquired.Success)

	again, err := store.TryLock(t.Context(), &lock.TryLockRequest{
		ResourceID:      "resource",
		LockOwner:       "owner",
		ExpiryInSeconds: 30,
	})
	require.NoError(t, err)
	assert.False(t, again.Success)

	wrongOwner, err := store.Unlock(t.Context(), &lock.UnlockRequest{
		ResourceID: "resource",
		LockOwner:  "other",
	})
	require.NoError(t, err)
	assert.Equal(t, lock.LockBelongsToOthers, wrongOwner.Status)

	released, err := store.Unlock(t.Context(), &lock.UnlockRequest{
		ResourceID: "resource",
		LockOwner:  "owner",
	})
	require.NoError(t, err)
	assert.Equal(t, lock.Success, released.Status)

	missing, err := store.Unlock(t.Context(), &lock.UnlockRequest{
		ResourceID: "resource",
		LockOwner:  "owner",
	})
	require.NoError(t, err)
	assert.Equal(t, lock.LockDoesNotExist, missing.Status)
}

func TestLockExpires(t *testing.T) {
	store := newTestLockStore(t)
	first, err := store.TryLock(t.Context(), &lock.TryLockRequest{
		ResourceID:      "expiring",
		LockOwner:       "first",
		ExpiryInSeconds: 1,
	})
	require.NoError(t, err)
	require.True(t, first.Success)

	require.Eventually(t, func() bool {
		next, tryErr := store.TryLock(t.Context(), &lock.TryLockRequest{
			ResourceID:      "expiring",
			LockOwner:       "second",
			ExpiryInSeconds: 30,
		})
		return tryErr == nil && next != nil && next.Success
	}, 3*time.Second, 25*time.Millisecond)

	oldOwner, err := store.Unlock(t.Context(), &lock.UnlockRequest{
		ResourceID: "expiring",
		LockOwner:  "first",
	})
	require.NoError(t, err)
	assert.Equal(t, lock.LockBelongsToOthers, oldOwner.Status)
}

func TestConcurrentTryLockHasOneWinner(t *testing.T) {
	store := newTestLockStore(t)
	type result struct {
		owner   string
		success bool
		err     error
	}

	const contenders = 16
	start := make(chan struct{})
	results := make(chan result, contenders)
	for i := range contenders {
		owner := fmt.Sprintf("owner-%d", i)
		go func() {
			<-start
			response, err := store.TryLock(t.Context(), &lock.TryLockRequest{
				ResourceID:      "contended",
				LockOwner:       owner,
				ExpiryInSeconds: 30,
			})
			results <- result{owner: owner, success: response != nil && response.Success, err: err}
		}()
	}
	close(start)

	winners := make([]string, 0, 1)
	for range contenders {
		result := <-results
		require.NoError(t, result.err)
		if result.success {
			winners = append(winners, result.owner)
		}
	}
	require.Len(t, winners, 1)

	released, err := store.Unlock(t.Context(), &lock.UnlockRequest{
		ResourceID: "contended",
		LockOwner:  winners[0],
	})
	require.NoError(t, err)
	assert.Equal(t, lock.Success, released.Status)
}

func TestValidationCancellationAndClose(t *testing.T) {
	uninitialized := NewSwytchLockStore(logger.NewLogger("swytch-lock-test")).(*LockStore)
	_, err := uninitialized.TryLock(t.Context(), &lock.TryLockRequest{
		ResourceID:      "resource",
		LockOwner:       "owner",
		ExpiryInSeconds: 1,
	})
	require.ErrorIs(t, err, swytchcomponent.ErrNotInitialized)

	store := newTestLockStore(t)
	_, err = store.TryLock(t.Context(), nil)
	require.Error(t, err)
	_, err = store.TryLock(t.Context(), &lock.TryLockRequest{ResourceID: "resource", LockOwner: "owner"})
	require.Error(t, err)
	_, err = store.Unlock(t.Context(), nil)
	require.Error(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = store.TryLock(ctx, &lock.TryLockRequest{
		ResourceID:      "resource",
		LockOwner:       "owner",
		ExpiryInSeconds: 1,
	})
	require.ErrorIs(t, err, context.Canceled)

	require.NoError(t, uninitialized.Close())
	require.NoError(t, uninitialized.Close())
}

func TestComponentMetadata(t *testing.T) {
	store := NewSwytchLockStore(logger.NewLogger("swytch-lock-test")).(*LockStore)
	assert.NotEmpty(t, store.GetComponentMetadata())
	require.NoError(t, store.Close())
}
