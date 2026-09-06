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
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/swytchdb/engine/beacon"
)

const (
	defaultMemoryLimit = "80%"
	defaultClusterPort = 7380
)

var (
	errCloudExclusive            = errors.New("cloud is exclusive with clusterPassword and discovery")
	errDiscoveryRequiresPassword = errors.New("discovery requires clusterPassword")
)

type swytchMetadata struct {
	MemoryLimit      string `mapstructure:"memory-limit"`
	Discovery        string `mapstructure:"discovery"`
	ClusterPassword  string `mapstructure:"clusterPassword"`
	Cloud            string `mapstructure:"cloud"`
	ClusterPort      int    `mapstructure:"clusterPort"`
	AdvertiseAddress string `mapstructure:"advertiseAddress"`
	Compress         bool   `mapstructure:"compress"`
}

func parseMetadata(meta state.Metadata) (beacon.RuntimeConfig, error) {
	settings := swytchMetadata{
		MemoryLimit: defaultMemoryLimit,
		ClusterPort: defaultClusterPort,
	}
	if value, ok := meta.GetProperty("memory-limit", "memoryLimit", "maxMemory"); ok && strings.TrimSpace(value) != "" {
		settings.MemoryLimit = value
	}
	settings.Discovery, _ = meta.GetProperty("discovery", "join")
	settings.ClusterPassword, _ = meta.GetProperty("clusterPassword", "clusterPassphrase")
	settings.Cloud, _ = meta.GetProperty("cloud", "connectionSecret")
	settings.AdvertiseAddress, _ = meta.GetProperty("advertiseAddress", "clusterAdvertise")

	if value, ok := meta.GetProperty("clusterPort"); ok && strings.TrimSpace(value) != "" {
		port, err := strconv.Atoi(value)
		if err != nil || port < 1 || port > 65535 {
			return beacon.RuntimeConfig{}, errors.New("clusterPort must be an integer between 1 and 65535")
		}
		settings.ClusterPort = port
	}
	if value, ok := meta.GetProperty("compress"); ok && strings.TrimSpace(value) != "" {
		compress, err := strconv.ParseBool(value)
		if err != nil {
			return beacon.RuntimeConfig{}, fmt.Errorf("compress must be a boolean: %w", err)
		}
		settings.Compress = compress
	}

	if settings.Cloud != "" && (settings.ClusterPassword != "" || settings.Discovery != "") {
		return beacon.RuntimeConfig{}, errCloudExclusive
	}
	if settings.Discovery != "" && settings.ClusterPassword == "" {
		return beacon.RuntimeConfig{}, errDiscoveryRequiresPassword
	}

	memoryLimit, memoryPercent, err := beacon.ParseMemoryLimit(settings.MemoryLimit)
	if err != nil {
		return beacon.RuntimeConfig{}, fmt.Errorf("invalid memory-limit %q: %w", settings.MemoryLimit, err)
	}
	clusterPort := 0
	if settings.ClusterPassword != "" || settings.Cloud != "" {
		clusterPort = settings.ClusterPort
	}
	return beacon.RuntimeConfig{
		MemoryLimit:        memoryLimit,
		MemoryLimitPercent: memoryPercent,
		ClusterPassphrase:  settings.ClusterPassword,
		ConnectionSecret:   settings.Cloud,
		JoinAddr:           settings.Discovery,
		ClusterPort:        clusterPort,
		AdvertiseAddr:      settings.AdvertiseAddress,
		CompressValues:     settings.Compress,
	}, nil
}

// GetComponentMetadata exposes the accepted metadata fields to Dapr's
// metadata analyzer.
func (s *SwytchStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(swytchMetadata{}), &metadataInfo, metadata.StateStoreType)
	return metadataInfo
}
