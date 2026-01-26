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
	// Process type hints before conversion
	processedData, err := processTypeHints(data)
	if err != nil {
		return nil, err
	}
	return interfaceToAttributeValue(processedData), nil
}

// processTypeHints processes attribute names with type hints (e.g., "name<S>", "age<N>")
// and returns a new map with the type hints applied and removed from attribute names
func processTypeHints(data map[string]any) (map[string]any, error) {
	result := make(map[string]any)

	for key, value := range data {
		// Check if the key has a type hint suffix: <TYPE>
		if idx := strings.LastIndex(key, "<"); idx != -1 && strings.HasSuffix(key, ">") {
			// Extract the type hint
			typeHint := strings.TrimSuffix(key[idx+1:], ">")
			cleanKey := key[:idx]

			// Convert the value based on the type hint
			convertedValue, err := convertValueWithTypeHint(value, typeHint)
			if err != nil {
				return nil, fmt.Errorf("failed to convert %s with type %s: %w", cleanKey, typeHint, err)
			}

			result[cleanKey] = convertedValue
		} else {
			// No type hint, keep as-is
			result[key] = value
		}
	}

	return result, nil
}

// convertValueWithTypeHint converts a value to a specific format based on the DynamoDB type hint
func convertValueWithTypeHint(value any, typeHint string) (any, error) {
	switch strings.ToUpper(typeHint) {
	case "S":
		// String type
		return fmt.Sprintf("%v", value), nil

	case "N":
		// Number type - keep as json.Number or numeric type
		switch v := value.(type) {
		case json.Number:
			return v, nil
		case float64, int, int64:
			return v, nil
		case string:
			// Parse string as number
			return json.Number(v), nil
		default:
			return json.Number(fmt.Sprintf("%v", v)), nil
		}

	case "BOOL":
		// Boolean type
		switch v := value.(type) {
		case bool:
			return v, nil
		case string:
			return strings.ToLower(v) == "true", nil
		default:
			return false, fmt.Errorf("cannot convert %v to boolean", v)
		}

	case "NULL":
		// Null type
		return nil, nil

	case "L":
		// List type
		switch v := value.(type) {
		case []any:
			return v, nil
		case string:
			// Try to parse as JSON array
			var list []any
			if err := json.Unmarshal([]byte(v), &list); err != nil {
				return nil, fmt.Errorf("cannot parse list: %w", err)
			}
			return list, nil
		default:
			return []any{v}, nil
		}

	case "M":
		// Map type
		switch v := value.(type) {
		case map[string]any:
			// Recursively process nested maps for type hints
			return processTypeHints(v)
		case string:
			// Try to parse as JSON object
			var m map[string]any
			if err := json.Unmarshal([]byte(v), &m); err != nil {
				return nil, fmt.Errorf("cannot parse map: %w", err)
			}
			return processTypeHints(m)
		default:
			return nil, fmt.Errorf("cannot convert %v to map", v)
		}

	case "SS":
		// String Set
		switch v := value.(type) {
		case []any:
			ss := make([]string, len(v))
			for i, item := range v {
				ss[i] = fmt.Sprintf("%v", item)
			}
			return map[string]any{"__SS": ss}, nil
		case string:
			// Try to parse as JSON array
			var list []any
			if err := json.Unmarshal([]byte(v), &list); err != nil {
				// Treat as single-element set
				return map[string]any{"__SS": []string{v}}, nil
			}
			ss := make([]string, len(list))
			for i, item := range list {
				ss[i] = fmt.Sprintf("%v", item)
			}
			return map[string]any{"__SS": ss}, nil
		default:
			return map[string]any{"__SS": []string{fmt.Sprintf("%v", v)}}, nil
		}

	case "NS":
		// Number Set
		switch v := value.(type) {
		case []any:
			ns := make([]string, len(v))
			for i, item := range v {
				ns[i] = fmt.Sprintf("%v", item)
			}
			return map[string]any{"__NS": ns}, nil
		case string:
			// Try to parse as JSON array
			var list []any
			if err := json.Unmarshal([]byte(v), &list); err != nil {
				// Treat as single-element set
				return map[string]any{"__NS": []string{v}}, nil
			}
			ns := make([]string, len(list))
			for i, item := range list {
				ns[i] = fmt.Sprintf("%v", item)
			}
			return map[string]any{"__NS": ns}, nil
		default:
			return map[string]any{"__NS": []string{fmt.Sprintf("%v", v)}}, nil
		}

	case "B":
		// Binary type
		switch v := value.(type) {
		case []byte:
			return v, nil
		case string:
			// Assume base64 encoded
			return []byte(v), nil
		default:
			return []byte(fmt.Sprintf("%v", v)), nil
		}

	case "BS":
		// Binary Set
		switch v := value.(type) {
		case []any:
			bs := make([][]byte, len(v))
			for i, item := range v {
				if b, ok := item.([]byte); ok {
					bs[i] = b
				} else {
					bs[i] = []byte(fmt.Sprintf("%v", item))
				}
			}
			return map[string]any{"__BS": bs}, nil
		case string:
			// Try to parse as JSON array
			var list []any
			if err := json.Unmarshal([]byte(v), &list); err != nil {
				// Treat as single-element set
				return map[string]any{"__BS": [][]byte{[]byte(v)}}, nil
			}
			bs := make([][]byte, len(list))
			for i, item := range list {
				if b, ok := item.([]byte); ok {
					bs[i] = b
				} else {
					bs[i] = []byte(fmt.Sprintf("%v", item))
				}
			}
			return map[string]any{"__BS": bs}, nil
		default:
			return map[string]any{"__BS": [][]byte{[]byte(fmt.Sprintf("%v", v))}}, nil
		}

	default:
		return nil, fmt.Errorf("unknown type hint: %s", typeHint)
	}
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
	case []byte:
		return &types.AttributeValueMemberB{Value: val}
	case []any:
		list := make([]types.AttributeValue, len(val))
		for i, item := range val {
			list[i] = valueToAttr(item)
		}
		return &types.AttributeValueMemberL{Value: list}
	case map[string]any:
		// Check for special set type markers
		if ss, ok := val["__SS"]; ok {
			if ssSlice, ok := ss.([]string); ok {
				return &types.AttributeValueMemberSS{Value: ssSlice}
			}
		}
		if ns, ok := val["__NS"]; ok {
			if nsSlice, ok := ns.([]string); ok {
				return &types.AttributeValueMemberNS{Value: nsSlice}
			}
		}
		if bs, ok := val["__BS"]; ok {
			if bsSlice, ok := bs.([][]byte); ok {
				return &types.AttributeValueMemberBS{Value: bsSlice}
			}
		}
		// Regular map
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

// ItemToDataTypes converts a DynamoDB item to a pretty-printed JSON-like structure showing data types
func ItemToDataTypes(item map[string]types.AttributeValue) string {
	typeMap := attributeValueToTypeMap(item)
	data, err := json.MarshalIndent(typeMap, "", "  ")
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return string(data)
}

func attributeValueToTypeMap(item map[string]types.AttributeValue) map[string]any {
	result := make(map[string]any)
	for k, v := range item {
		result[k] = attrToType(v)
	}
	return result
}

func attrToType(av types.AttributeValue) any {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return "S"
	case *types.AttributeValueMemberN:
		return "N"
	case *types.AttributeValueMemberBOOL:
		return "BOOL"
	case *types.AttributeValueMemberNULL:
		return "NULL"
	case *types.AttributeValueMemberL:
		list := make([]any, len(v.Value))
		for i, item := range v.Value {
			list[i] = attrToType(item)
		}
		return map[string]any{
			"type":     "L",
			"elements": list,
		}
	case *types.AttributeValueMemberM:
		return map[string]any{
			"type":       "M",
			"attributes": attributeValueToTypeMap(v.Value),
		}
	case *types.AttributeValueMemberSS:
		return "SS"
	case *types.AttributeValueMemberNS:
		return "NS"
	case *types.AttributeValueMemberB:
		return "B"
	case *types.AttributeValueMemberBS:
		return "BS"
	default:
		return "Unknown"
	}
}
