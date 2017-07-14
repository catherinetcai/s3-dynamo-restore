package restore

import "github.com/aws/aws-sdk-go/service/dynamodb"

type RecordWrapper interface {
	CreateWriteRequest() *dynamodb.WriteRequest
	KeyName() string
	IsDupe(RecordWrapper) bool
}

// DynamoRecord just wraps a map[string]interface{}
type DynamoRecord map[string]interface{}
