// Package checkpointredis persists Graph checkpoints in the service Redis
// deployment while isolating every tenant, configuration version, and declared
// checkpoint namespace.
package checkpointredis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"time"

	redisclient "github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/graph"
)

const keyPrefix = "trpc-agent:graph-checkpoint:v1"

// Resolver creates lightweight, isolated checkpoint savers over a shared Redis
// client. The caller retains ownership of the client.
type Resolver struct {
	Client redisclient.UniversalClient
	TTL    time.Duration
}

// ResolveCheckpointSaver implements agent.CheckpointResolver.
func (r Resolver) ResolveCheckpointSaver(ctx context.Context, tenantID, namespace string, configVersion int64) (graph.CheckpointSaver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.Client == nil || r.TTL <= 0 || tenantID == "" || namespace == "" || configVersion < 1 {
		return nil, errors.New("invalid graph checkpoint resolver configuration")
	}
	scope := digest(tenantID + "\x00" + strconv.FormatInt(configVersion, 10) + "\x00" + namespace)
	return &saver{client: r.Client, ttl: r.TTL, scope: scope}, nil
}

type saver struct {
	client redisclient.UniversalClient
	ttl    time.Duration
	scope  string
}

type storedWrite struct {
	TaskID   string          `json:"task_id"`
	Index    int             `json:"index"`
	Channel  string          `json:"channel"`
	Value    json.RawMessage `json:"value"`
	Sequence int64           `json:"sequence"`
}

func (s *saver) Get(ctx context.Context, config map[string]any) (*graph.Checkpoint, error) {
	tuple, err := s.GetTuple(ctx, config)
	if err != nil || tuple == nil {
		return nil, err
	}
	return tuple.Checkpoint, nil
}

func (s *saver) GetTuple(ctx context.Context, config map[string]any) (*graph.CheckpointTuple, error) {
	lineage, namespace, checkpointID, err := checkpointCoordinates(config, false)
	if err != nil {
		return nil, err
	}
	if checkpointID == "" {
		values, lookupErr := s.client.ZRevRange(ctx, s.timelineKey(lineage, namespace), 0, 0).Result()
		if lookupErr != nil {
			return nil, fmt.Errorf("find latest graph checkpoint: %w", lookupErr)
		}
		if len(values) == 0 {
			return nil, nil
		}
		checkpointID = values[0]
	}
	values, err := s.client.HGetAll(ctx, s.checkpointKey(lineage, namespace, checkpointID)).Result()
	if err != nil {
		return nil, fmt.Errorf("load graph checkpoint: %w", err)
	}
	if len(values) == 0 {
		return nil, nil
	}
	var checkpoint graph.Checkpoint
	if err := json.Unmarshal([]byte(values["checkpoint"]), &checkpoint); err != nil {
		return nil, fmt.Errorf("decode graph checkpoint: %w", err)
	}
	var metadata graph.CheckpointMetadata
	if err := json.Unmarshal([]byte(values["metadata"]), &metadata); err != nil {
		return nil, fmt.Errorf("decode graph checkpoint metadata: %w", err)
	}
	if raw := values["timestamp"]; raw != "" {
		nanoseconds, parseErr := strconv.ParseInt(raw, 10, 64)
		if parseErr != nil {
			return nil, fmt.Errorf("decode graph checkpoint timestamp: %w", parseErr)
		}
		checkpoint.Timestamp = time.Unix(0, nanoseconds).UTC()
	}
	writes, err := s.loadWrites(ctx, lineage, namespace, checkpointID)
	if err != nil {
		return nil, err
	}
	var parent map[string]any
	if parentID := values["parent_id"]; parentID != "" {
		parentNamespace, findErr := s.findNamespace(ctx, lineage, parentID)
		if findErr != nil {
			return nil, findErr
		}
		parent = graph.CreateCheckpointConfig(lineage, parentID, parentNamespace)
	}
	return &graph.CheckpointTuple{
		Config: graph.CreateCheckpointConfig(lineage, checkpointID, namespace), Checkpoint: &checkpoint,
		Metadata: &metadata, ParentConfig: parent, PendingWrites: writes,
	}, nil
}

func (s *saver) List(ctx context.Context, config map[string]any, filter *graph.CheckpointFilter) ([]*graph.CheckpointTuple, error) {
	lineage, namespace, _, err := checkpointCoordinates(config, false)
	if err != nil {
		return nil, err
	}
	maximum := "+inf"
	if filter != nil && filter.Before != nil {
		before := graph.GetCheckpointID(filter.Before)
		if before != "" {
			score, scoreErr := s.client.ZScore(ctx, s.timelineKey(lineage, namespace), before).Result()
			if scoreErr != nil && !errors.Is(scoreErr, redisclient.Nil) {
				return nil, fmt.Errorf("find graph checkpoint boundary: %w", scoreErr)
			}
			if scoreErr == nil {
				maximum = "(" + strconv.FormatFloat(score, 'f', -1, 64)
			}
		}
	}
	ids, err := s.client.ZRevRangeByScore(ctx, s.timelineKey(lineage, namespace), &redisclient.ZRangeBy{Min: "-inf", Max: maximum}).Result()
	if err != nil {
		return nil, fmt.Errorf("list graph checkpoints: %w", err)
	}
	result := make([]*graph.CheckpointTuple, 0, len(ids))
	for _, id := range ids {
		tuple, tupleErr := s.GetTuple(ctx, graph.CreateCheckpointConfig(lineage, id, namespace))
		if tupleErr != nil {
			return nil, tupleErr
		}
		if tuple == nil || !metadataMatches(tuple.Metadata, filter) {
			continue
		}
		result = append(result, tuple)
		if filter != nil && filter.Limit > 0 && len(result) >= filter.Limit {
			break
		}
	}
	return result, nil
}

func (s *saver) Put(ctx context.Context, req graph.PutRequest) (map[string]any, error) {
	return s.put(ctx, req.Config, req.Checkpoint, req.Metadata, nil)
}

func (s *saver) PutWrites(ctx context.Context, req graph.PutWritesRequest) error {
	lineage, namespace, checkpointID, err := checkpointCoordinates(req.Config, true)
	if err != nil {
		return err
	}
	key := s.writesKey(lineage, namespace, checkpointID)
	_, err = s.client.TxPipelined(ctx, func(pipe redisclient.Pipeliner) error {
		for index, pending := range req.Writes {
			value, marshalErr := json.Marshal(pending.Value)
			if marshalErr != nil {
				return fmt.Errorf("encode graph pending write: %w", marshalErr)
			}
			sequence := pending.Sequence
			if sequence == 0 {
				sequence = int64(index)
			}
			encoded, marshalErr := json.Marshal(storedWrite{TaskID: req.TaskID, Index: index, Channel: pending.Channel, Value: value, Sequence: sequence})
			if marshalErr != nil {
				return fmt.Errorf("encode graph pending write envelope: %w", marshalErr)
			}
			pipe.HSet(ctx, key, req.TaskID+":"+strconv.Itoa(index), encoded)
		}
		pipe.Expire(ctx, key, s.ttl)
		return nil
	})
	if err != nil {
		return fmt.Errorf("store graph pending writes: %w", err)
	}
	return nil
}

func (s *saver) PutFull(ctx context.Context, req graph.PutFullRequest) (map[string]any, error) {
	return s.put(ctx, req.Config, req.Checkpoint, req.Metadata, req.PendingWrites)
}

func (s *saver) put(ctx context.Context, config map[string]any, checkpoint *graph.Checkpoint,
	metadata *graph.CheckpointMetadata, writes []graph.PendingWrite,
) (map[string]any, error) {
	if checkpoint == nil || checkpoint.ID == "" {
		return nil, errors.New("graph checkpoint and checkpoint id are required")
	}
	lineage, namespace, _, err := checkpointCoordinates(config, false)
	if err != nil {
		return nil, err
	}
	if metadata == nil {
		metadata = graph.NewCheckpointMetadata(graph.CheckpointSourceUpdate, 0)
	}
	checkpointJSON, err := json.Marshal(checkpoint)
	if err != nil {
		return nil, fmt.Errorf("encode graph checkpoint: %w", err)
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("encode graph checkpoint metadata: %w", err)
	}
	timestamp := checkpoint.Timestamp.UnixNano()
	if timestamp <= 0 {
		timestamp = time.Now().UTC().UnixNano()
	}
	checkpointKey := s.checkpointKey(lineage, namespace, checkpoint.ID)
	timelineKey := s.timelineKey(lineage, namespace)
	namespaceKey := s.namespaceKey(lineage)
	writesKey := s.writesKey(lineage, namespace, checkpoint.ID)
	_, err = s.client.TxPipelined(ctx, func(pipe redisclient.Pipeliner) error {
		pipe.HSet(ctx, checkpointKey, map[string]any{"checkpoint": checkpointJSON, "metadata": metadataJSON,
			"parent_id": checkpoint.ParentCheckpointID, "timestamp": timestamp})
		pipe.ZAdd(ctx, timelineKey, redisclient.Z{Score: float64(timestamp), Member: checkpoint.ID})
		pipe.SAdd(ctx, namespaceKey, namespace)
		for index, pending := range writes {
			value, marshalErr := json.Marshal(pending.Value)
			if marshalErr != nil {
				return fmt.Errorf("encode graph pending write: %w", marshalErr)
			}
			sequence := pending.Sequence
			if sequence == 0 {
				sequence = timestamp + int64(index)
			}
			encoded, marshalErr := json.Marshal(storedWrite{TaskID: pending.TaskID, Index: index, Channel: pending.Channel, Value: value, Sequence: sequence})
			if marshalErr != nil {
				return fmt.Errorf("encode graph pending write envelope: %w", marshalErr)
			}
			pipe.HSet(ctx, writesKey, pending.TaskID+":"+strconv.Itoa(index), encoded)
		}
		for _, key := range []string{checkpointKey, timelineKey, namespaceKey, writesKey} {
			pipe.Expire(ctx, key, s.ttl)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("store graph checkpoint: %w", err)
	}
	return graph.CreateCheckpointConfig(lineage, checkpoint.ID, namespace), nil
}

func (s *saver) DeleteLineage(ctx context.Context, lineage string) error {
	if lineage == "" {
		return errors.New("graph lineage id is required")
	}
	namespaceKey := s.namespaceKey(lineage)
	namespaces, err := s.client.SMembers(ctx, namespaceKey).Result()
	if err != nil {
		return fmt.Errorf("list graph checkpoint namespaces: %w", err)
	}
	keys := []string{namespaceKey}
	for _, namespace := range namespaces {
		timeline := s.timelineKey(lineage, namespace)
		ids, listErr := s.client.ZRange(ctx, timeline, 0, -1).Result()
		if listErr != nil {
			return fmt.Errorf("list graph lineage checkpoints: %w", listErr)
		}
		keys = append(keys, timeline)
		for _, id := range ids {
			keys = append(keys, s.checkpointKey(lineage, namespace, id), s.writesKey(lineage, namespace, id))
		}
	}
	if len(keys) > 0 {
		if err := s.client.Del(ctx, keys...).Err(); err != nil {
			return fmt.Errorf("delete graph lineage: %w", err)
		}
	}
	return nil
}

// Close is intentionally a no-op because Resolver does not own the shared
// Redis client.
func (*saver) Close() error { return nil }

func (s *saver) loadWrites(ctx context.Context, lineage, namespace, checkpointID string) ([]graph.PendingWrite, error) {
	values, err := s.client.HGetAll(ctx, s.writesKey(lineage, namespace, checkpointID)).Result()
	if err != nil {
		return nil, fmt.Errorf("load graph pending writes: %w", err)
	}
	result := make([]graph.PendingWrite, 0, len(values))
	for _, value := range values {
		var stored storedWrite
		if err := json.Unmarshal([]byte(value), &stored); err != nil {
			return nil, fmt.Errorf("decode graph pending write: %w", err)
		}
		var decoded any
		if err := json.Unmarshal(stored.Value, &decoded); err != nil {
			return nil, fmt.Errorf("decode graph pending write value: %w", err)
		}
		result = append(result, graph.PendingWrite{TaskID: stored.TaskID, Channel: stored.Channel, Value: decoded, Sequence: stored.Sequence})
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].Sequence < result[j].Sequence })
	return result, nil
}

func (s *saver) findNamespace(ctx context.Context, lineage, checkpointID string) (string, error) {
	namespaces, err := s.client.SMembers(ctx, s.namespaceKey(lineage)).Result()
	if err != nil {
		return "", fmt.Errorf("list graph checkpoint namespaces: %w", err)
	}
	for _, namespace := range namespaces {
		exists, existsErr := s.client.Exists(ctx, s.checkpointKey(lineage, namespace, checkpointID)).Result()
		if existsErr != nil {
			return "", fmt.Errorf("find graph checkpoint namespace: %w", existsErr)
		}
		if exists > 0 {
			return namespace, nil
		}
	}
	return "", nil
}

func (s *saver) checkpointKey(lineage, namespace, checkpointID string) string {
	return s.base(lineage) + ":checkpoint:" + digest(namespace) + ":" + digest(checkpointID)
}

func (s *saver) timelineKey(lineage, namespace string) string {
	return s.base(lineage) + ":timeline:" + digest(namespace)
}

func (s *saver) writesKey(lineage, namespace, checkpointID string) string {
	return s.base(lineage) + ":writes:" + digest(namespace) + ":" + digest(checkpointID)
}

func (s *saver) namespaceKey(lineage string) string { return s.base(lineage) + ":namespaces" }

func (s *saver) base(lineage string) string {
	lineageDigest := digest(lineage)
	return keyPrefix + ":" + s.scope + ":{" + lineageDigest + "}"
}

func checkpointCoordinates(config map[string]any, requireCheckpoint bool) (string, string, string, error) {
	lineage := graph.GetLineageID(config)
	checkpointID := graph.GetCheckpointID(config)
	if lineage == "" || (requireCheckpoint && checkpointID == "") {
		return "", "", "", errors.New("graph checkpoint coordinates are incomplete")
	}
	return lineage, graph.GetNamespace(config), checkpointID, nil
}

func metadataMatches(metadata *graph.CheckpointMetadata, filter *graph.CheckpointFilter) bool {
	if filter == nil || len(filter.Metadata) == 0 {
		return true
	}
	if metadata == nil || metadata.Extra == nil {
		return false
	}
	for key, expected := range filter.Metadata {
		actual, ok := metadata.Extra[key]
		if !ok || !reflect.DeepEqual(actual, expected) {
			return false
		}
	}
	return true
}

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:16])
}
