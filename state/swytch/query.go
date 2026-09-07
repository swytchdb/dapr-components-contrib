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
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/query"
)

type queryRow struct {
	key   string
	data  []byte
	etag  *string
	value any
}

// Query scans the store's point-in-time key index and evaluates Dapr query
// filters against JSON values.
func (s *SwytchStore) Query(ctx context.Context, req *state.QueryRequest) (*state.QueryResponse, error) {
	if req == nil {
		return nil, errors.New("query request is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	if s.runtime == nil {
		return nil, errNotInitialized
	}

	keys := s.runtime.Engine.MatchKeys("*")
	rows := make([]queryRow, 0, len(keys))
	for _, key := range keys {
		if strings.HasPrefix(key, "__swytch:") {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		snap, tips, err := s.runtime.Engine.NewReadOnlyContext().GetSnapshot(key)
		if err != nil {
			return nil, err
		}
		response := responseFromSnapshot(snap, tips)
		if response.ETag == nil {
			continue
		}
		var value any
		if err = json.Unmarshal(response.Data, &value); err != nil {
			// Non-JSON scalar values cannot satisfy document queries.
			continue
		}
		matches, err := matchesFilter(value, req.Query.Filter)
		if err != nil {
			return nil, err
		}
		if matches {
			rows = append(rows, queryRow{
				key:   key,
				data:  response.Data,
				etag:  response.ETag,
				value: value,
			})
		}
	}

	sort.SliceStable(rows, func(i, j int) bool {
		for _, sorting := range req.Query.Sort {
			left, leftOK := lookupPath(rows[i].value, sorting.Key)
			right, rightOK := lookupPath(rows[j].value, sorting.Key)
			cmp := compareValues(left, leftOK, right, rightOK)
			if cmp == 0 {
				continue
			}
			if strings.EqualFold(sorting.Order, query.DESC) {
				return cmp > 0
			}
			return cmp < 0
		}
		return rows[i].key < rows[j].key
	})

	start, err := parseQueryToken(req.Query.Page.Token, len(rows))
	if err != nil {
		return nil, err
	}
	end := len(rows)
	if req.Query.Page.Limit > 0 && start+req.Query.Page.Limit < end {
		end = start + req.Query.Page.Limit
	}

	results := make([]state.QueryItem, end-start)
	for i := start; i < end; i++ {
		results[i-start] = state.QueryItem{
			Key:  rows[i].key,
			Data: rows[i].data,
			ETag: rows[i].etag,
		}
	}
	response := &state.QueryResponse{Results: results}
	if end < len(rows) {
		response.Token = strconv.Itoa(end)
	}
	return response, nil
}

func matchesFilter(value any, filter query.Filter) (bool, error) {
	if filter == nil {
		return true, nil
	}
	switch f := filter.(type) {
	case *query.EQ:
		actual, ok := lookupPath(value, f.Key)
		return ok && equalValues(actual, f.Val), nil
	case *query.NEQ:
		actual, ok := lookupPath(value, f.Key)
		return ok && !equalValues(actual, f.Val), nil
	case *query.GT:
		return matchesComparison(value, f.Key, f.Val, func(cmp int) bool { return cmp > 0 }), nil
	case *query.GTE:
		return matchesComparison(value, f.Key, f.Val, func(cmp int) bool { return cmp >= 0 }), nil
	case *query.LT:
		return matchesComparison(value, f.Key, f.Val, func(cmp int) bool { return cmp < 0 }), nil
	case *query.LTE:
		return matchesComparison(value, f.Key, f.Val, func(cmp int) bool { return cmp <= 0 }), nil
	case *query.IN:
		actual, ok := lookupPath(value, f.Key)
		if !ok {
			return false, nil
		}
		for _, candidate := range f.Vals {
			if equalValues(actual, candidate) {
				return true, nil
			}
		}
		return false, nil
	case *query.AND:
		for _, child := range f.Filters {
			matches, err := matchesFilter(value, child)
			if err != nil || !matches {
				return matches, err
			}
		}
		return true, nil
	case *query.OR:
		for _, child := range f.Filters {
			matches, err := matchesFilter(value, child)
			if err != nil {
				return false, err
			}
			if matches {
				return true, nil
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("unsupported query filter type %T", filter)
	}
}

func matchesComparison(value any, key string, expected any, predicate func(int) bool) bool {
	actual, ok := lookupPath(value, key)
	if !ok {
		return false
	}
	cmp, comparable := compareScalarValues(actual, expected)
	return comparable && predicate(cmp)
}

func lookupPath(value any, path string) (any, bool) {
	current := value
	for part := range strings.SplitSeq(strings.TrimPrefix(path, "."), ".") {
		if part == "" {
			continue
		}
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func equalValues(left, right any) bool {
	if cmp, ok := compareScalarValues(left, right); ok {
		return cmp == 0
	}
	return reflect.DeepEqual(left, right)
}

func compareValues(left any, leftOK bool, right any, rightOK bool) int {
	if !leftOK && !rightOK {
		return 0
	}
	if !leftOK {
		return -1
	}
	if !rightOK {
		return 1
	}
	if cmp, ok := compareScalarValues(left, right); ok {
		return cmp
	}
	leftText := fmt.Sprint(left)
	rightText := fmt.Sprint(right)
	return strings.Compare(leftText, rightText)
}

func compareScalarValues(left, right any) (int, bool) {
	if leftNumber, ok := numericValue(left); ok {
		rightNumber, rightOK := numericValue(right)
		if !rightOK {
			return 0, false
		}
		switch {
		case leftNumber < rightNumber:
			return -1, true
		case leftNumber > rightNumber:
			return 1, true
		default:
			return 0, true
		}
	}
	switch leftValue := left.(type) {
	case string:
		rightValue, ok := right.(string)
		if !ok {
			return 0, false
		}
		return strings.Compare(leftValue, rightValue), true
	case bool:
		rightValue, ok := right.(bool)
		if !ok {
			return 0, false
		}
		switch {
		case leftValue == rightValue:
			return 0, true
		case !leftValue:
			return -1, true
		default:
			return 1, true
		}
	case nil:
		return 0, right == nil
	default:
		return 0, false
	}
}

func numericValue(value any) (float64, bool) {
	switch number := value.(type) {
	case float64:
		return number, true
	case float32:
		return float64(number), true
	case int:
		return float64(number), true
	case int8:
		return float64(number), true
	case int16:
		return float64(number), true
	case int32:
		return float64(number), true
	case int64:
		return float64(number), true
	case uint:
		return float64(number), true
	case uint8:
		return float64(number), true
	case uint16:
		return float64(number), true
	case uint32:
		return float64(number), true
	case uint64:
		return float64(number), true
	case json.Number:
		parsed, err := number.Float64()
		return parsed, err == nil
	default:
		return 0, false
	}
}

func parseQueryToken(token string, length int) (int, error) {
	if token == "" {
		return 0, nil
	}
	offset, err := strconv.Atoi(token)
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("invalid query continuation token %q", token)
	}
	if offset > length {
		return length, nil
	}
	return offset, nil
}

var _ state.Querier = (*SwytchStore)(nil)
