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
	"sort"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func SnapshotExists(snap *pb.ReducedEffect) bool {
	return snap != nil && snap.Scalar != nil
}

func ScalarValue(snap *pb.ReducedEffect) []byte {
	if !SnapshotExists(snap) {
		return nil
	}
	return append([]byte(nil), snap.Scalar.Decompress()...)
}

func EmitScalar(ctx *effects.Context, key string, data []byte, tips []effects.Tip) error {
	return ctx.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: data},
		}},
	}, tips)
}

func EmitExpiration(ctx *effects.Context, key string, expiresAt *timestamppb.Timestamp) error {
	return ctx.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{ExpiresAt: expiresAt}},
	})
}

func EmitDelete(ctx *effects.Context, key string, tips []effects.Tip) error {
	return ctx.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
		}},
	}, tips)
}

// LockKeys takes each engine stripe at most once and in stripe order.
func LockKeys(engine *effects.Engine, keys []string) func() {
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
