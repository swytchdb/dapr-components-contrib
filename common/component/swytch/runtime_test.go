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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func TestMatchingComponentsShareRuntimeUntilFinalClose(t *testing.T) {
	meta := metadata.Base{Properties: map[string]string{"memory-limit": "80%"}}
	first := NewRuntime(logger.NewLogger("swytch-runtime-test"))
	second := NewRuntime(logger.NewLogger("swytch-runtime-test"))
	require.NoError(t, first.Start(t.Context(), meta))
	t.Cleanup(func() {
		require.NoError(t, first.Close())
	})
	require.NoError(t, second.Start(t.Context(), meta))
	t.Cleanup(func() {
		require.NoError(t, second.Close())
	})

	firstEngine, releaseFirst, err := first.Acquire()
	require.NoError(t, err)
	releaseFirst()
	secondEngine, releaseSecond, err := second.Acquire()
	require.NoError(t, err)
	releaseSecond()
	require.Same(t, firstEngine, secondEngine)
	write := firstEngine.NewContext()
	require.NoError(t, EmitScalar(write, "shared-runtime-probe", []byte("value"), nil))
	require.NoError(t, write.Flush())

	require.NoError(t, first.Close())
	remainingEngine, releaseRemaining, err := second.Acquire()
	require.NoError(t, err)
	releaseRemaining()
	require.Same(t, secondEngine, remainingEngine)
	snap, _, err := remainingEngine.NewReadOnlyContext().GetSnapshot("shared-runtime-probe")
	require.NoError(t, err)
	require.Equal(t, []byte("value"), ScalarValue(snap))

	require.NoError(t, second.Close())
	_, _, err = second.Acquire()
	require.ErrorIs(t, err, ErrNotInitialized)
}
