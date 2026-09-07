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
	"reflect"

	swytchcomponent "github.com/dapr/components-contrib/common/component/swytch"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/swytchdb/engine/beacon"
)

var (
	errCloudExclusive            = swytchcomponent.ErrCloudExclusive
	errDiscoveryRequiresPassword = swytchcomponent.ErrDiscoveryRequiresPassword
)

func parseMetadata(meta state.Metadata) (beacon.RuntimeConfig, error) {
	return swytchcomponent.ParseMetadata(meta.Base)
}

// GetComponentMetadata exposes the accepted metadata fields to Dapr's
// metadata analyzer.
func (s *SwytchStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(swytchcomponent.Settings{}), &metadataInfo, metadata.StateStoreType)
	return metadataInfo
}
