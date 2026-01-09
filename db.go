// Copyright 2026 mlrd.tech, Inc.
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DDB struct {
	client   *dynamodb.Client
	endpoint string
}

type TableInfo struct {
	Name          string
	PartitionKey  string
	SortKey       string
	GlobalIndexes []IndexInfo
	LocalIndexes  []IndexInfo
}

type IndexInfo struct {
	Name         string
	PartitionKey string
	SortKey      string
}

func NewDB(endpoint string) (*DDB, error) {
	ctx := context.Background()

	// Use static credentials for local DynamoDB.
	// Doesn't work yet with real DynamoDB by design.
	staticCreds := credentials.NewStaticCredentialsProvider("local", "local", "")

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(staticCreds),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	return &DDB{
		client:   client,
		endpoint: endpoint,
	}, nil
}

func (db *DDB) ListTables(ctx context.Context) ([]string, error) {
	var tables []string
	var lastTable *string
	for {
		out, err := db.client.ListTables(ctx, &dynamodb.ListTablesInput{
			ExclusiveStartTableName: lastTable,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list tables: %w", err)
		}
		tables = append(tables, out.TableNames...)
		if out.LastEvaluatedTableName == nil {
			break
		}
		lastTable = out.LastEvaluatedTableName
	}
	return tables, nil
}

func (db *DDB) DescribeTable(ctx context.Context, tableName string) (*TableInfo, error) {
	out, err := db.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe table %s: %w", tableName, err)
	}

	info := &TableInfo{Name: tableName}

	// Get primary key schema
	for _, key := range out.Table.KeySchema {
		if key.KeyType == types.KeyTypeHash {
			info.PartitionKey = *key.AttributeName
		} else if key.KeyType == types.KeyTypeRange {
			info.SortKey = *key.AttributeName
		}
	}

	// Get global secondary indexes
	for _, gsi := range out.Table.GlobalSecondaryIndexes {
		idx := IndexInfo{Name: *gsi.IndexName}
		for _, key := range gsi.KeySchema {
			if key.KeyType == types.KeyTypeHash {
				idx.PartitionKey = *key.AttributeName
			} else if key.KeyType == types.KeyTypeRange {
				idx.SortKey = *key.AttributeName
			}
		}
		info.GlobalIndexes = append(info.GlobalIndexes, idx)
	}

	// Get local secondary indexes
	for _, lsi := range out.Table.LocalSecondaryIndexes {
		idx := IndexInfo{Name: *lsi.IndexName}
		for _, key := range lsi.KeySchema {
			if key.KeyType == types.KeyTypeHash {
				idx.PartitionKey = *key.AttributeName
			} else if key.KeyType == types.KeyTypeRange {
				idx.SortKey = *key.AttributeName
			}
		}
		info.LocalIndexes = append(info.LocalIndexes, idx)
	}

	return info, nil
}

func (db *DDB) Scan(ctx context.Context, tableName string, indexName string) ([]map[string]types.AttributeValue, error) {
	input := &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	}
	if indexName != "" {
		input.IndexName = aws.String(indexName)
	}

	var items []map[string]types.AttributeValue
	var lastKey map[string]types.AttributeValue

	for {
		input.ExclusiveStartKey = lastKey
		out, err := db.client.Scan(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		items = append(items, out.Items...)

		if out.LastEvaluatedKey == nil {
			break
		}
		lastKey = out.LastEvaluatedKey
	}

	return items, nil
}

func (db *DDB) Query(ctx context.Context, tableName string, indexName string, keyCondition string, exprValues map[string]types.AttributeValue) ([]map[string]types.AttributeValue, error) {
	input := &dynamodb.QueryInput{
		TableName:                 aws.String(tableName),
		KeyConditionExpression:    aws.String(keyCondition),
		ExpressionAttributeValues: exprValues,
	}
	if indexName != "" {
		input.IndexName = aws.String(indexName)
	}

	var items []map[string]types.AttributeValue
	var lastKey map[string]types.AttributeValue

	for {
		input.ExclusiveStartKey = lastKey
		out, err := db.client.Query(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("query failed: %w", err)
		}

		items = append(items, out.Items...)

		if out.LastEvaluatedKey == nil {
			break
		}
		lastKey = out.LastEvaluatedKey
	}

	return items, nil
}

func (db *DDB) GetItem(ctx context.Context, tableName string, key map[string]types.AttributeValue) (map[string]types.AttributeValue, error) {
	out, err := db.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       key,
	})
	if err != nil {
		return nil, fmt.Errorf("get item failed: %w", err)
	}
	return out.Item, nil
}

func (db *DDB) PutItem(ctx context.Context, tableName string, item map[string]types.AttributeValue) error {
	_, err := db.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("put item failed: %w", err)
	}
	return nil
}

func (db *DDB) DeleteItem(ctx context.Context, tableName string, key map[string]types.AttributeValue) error {
	_, err := db.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(tableName),
		Key:       key,
	})
	if err != nil {
		return fmt.Errorf("delete item failed: %w", err)
	}
	return nil
}

// ItemToJSON converts a DynamoDB item to JSON string
func ItemToJSON(item map[string]types.AttributeValue) string {
	simplified := attributeValueToInterface(item)
	data, err := json.Marshal(simplified)
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return string(data)
}

// ItemToPrettyJSON converts a DynamoDB item to pretty-printed JSON
func ItemToPrettyJSON(item map[string]types.AttributeValue) string {
	simplified := attributeValueToInterface(item)
	data, err := json.MarshalIndent(simplified, "", "  ")
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return string(data)
}

// JSONToItem converts a JSON string to DynamoDB item
func JSONToItem(jsonStr string) (map[string]types.AttributeValue, error) {
	var data map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	return interfaceToAttributeValue(data), nil
}

func attributeValueToInterface(item map[string]types.AttributeValue) map[string]any {
	result := make(map[string]any)
	for k, v := range item {
		result[k] = attrToInterface(v)
	}
	return result
}

func attrToInterface(av types.AttributeValue) any {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return v.Value
	case *types.AttributeValueMemberN:
		return json.Number(v.Value)
	case *types.AttributeValueMemberBOOL:
		return v.Value
	case *types.AttributeValueMemberNULL:
		return nil
	case *types.AttributeValueMemberL:
		list := make([]any, len(v.Value))
		for i, item := range v.Value {
			list[i] = attrToInterface(item)
		}
		return list
	case *types.AttributeValueMemberM:
		return attributeValueToInterface(v.Value)
	case *types.AttributeValueMemberSS:
		return v.Value
	case *types.AttributeValueMemberNS:
		return v.Value
	case *types.AttributeValueMemberB:
		return v.Value
	case *types.AttributeValueMemberBS:
		return v.Value
	default:
		return nil
	}
}

func interfaceToAttributeValue(data map[string]any) map[string]types.AttributeValue {
	result := make(map[string]types.AttributeValue)
	for k, v := range data {
		result[k] = valueToAttr(v)
	}
	return result
}

func valueToAttr(v any) types.AttributeValue {
	switch val := v.(type) {
	case string:
		return &types.AttributeValueMemberS{Value: val}
	case json.Number:
		return &types.AttributeValueMemberN{Value: string(val)}
	case float64:
		return &types.AttributeValueMemberN{Value: fmt.Sprintf("%v", val)}
	case int:
		return &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", val)}
	case bool:
		return &types.AttributeValueMemberBOOL{Value: val}
	case nil:
		return &types.AttributeValueMemberNULL{Value: true}
	case []any:
		list := make([]types.AttributeValue, len(val))
		for i, item := range val {
			list[i] = valueToAttr(item)
		}
		return &types.AttributeValueMemberL{Value: list}
	case map[string]any:
		return &types.AttributeValueMemberM{Value: interfaceToAttributeValue(val)}
	default:
		return &types.AttributeValueMemberS{Value: fmt.Sprintf("%v", val)}
	}
}

// GetKeyValue extracts the string value of a key from an item
func GetKeyValue(item map[string]types.AttributeValue, keyName string) string {
	if keyName == "" {
		return ""
	}
	av, ok := item[keyName]
	if !ok {
		return ""
	}
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return v.Value
	case *types.AttributeValueMemberN:
		return v.Value
	default:
		return fmt.Sprintf("%v", av)
	}
}

// ParseKeyValue parses a key=value string and returns an AttributeValue
func ParseKeyValue(keyValue string) (string, types.AttributeValue, error) {
	parts := strings.SplitN(keyValue, "=", 2)
	if len(parts) != 2 {
		return "", nil, fmt.Errorf("invalid key=value format: %s", keyValue)
	}
	key := strings.TrimSpace(parts[0])
	value := strings.TrimSpace(parts[1])

	// Try to determine if it's a number
	if _, err := fmt.Sscanf(value, "%f", new(float64)); err == nil && !strings.Contains(value, "\"") {
		return key, &types.AttributeValueMemberN{Value: value}, nil
	}

	// Default to string
	return key, &types.AttributeValueMemberS{Value: value}, nil
}

// BuildKey builds a DynamoDB key from partition and optional sort key
func BuildKey(tableInfo *TableInfo, pkValue string, skValue string) (map[string]types.AttributeValue, error) {
	key := make(map[string]types.AttributeValue)

	// Partition key always required
	key[tableInfo.PartitionKey] = &types.AttributeValueMemberS{Value: pkValue}

	// Add sort key if provided and table has one
	if tableInfo.SortKey != "" && skValue != "" {
		key[tableInfo.SortKey] = &types.AttributeValueMemberS{Value: skValue}
	}

	return key, nil
}

// AttributeValueToString converts an AttributeValue to a string representation
func AttributeValueToString(av types.AttributeValue) string {
	val := attrToInterface(av)
	if val == nil {
		return ""
	}

	// Convert the interface to string
	switch v := val.(type) {
	case string:
		return v
	case json.Number:
		return v.String()
	case bool:
		return fmt.Sprintf("%t", v)
	default:
		// For complex types (lists, maps, etc), use JSON representation
		jsonBytes, err := json.Marshal(val)
		if err != nil {
			return fmt.Sprintf("%v", val)
		}
		return string(jsonBytes)
	}
}
