package restore

import "github.com/goamz/goamz/dynamodb"

type DynamoRecords []*DynamoRecord

// DynamoRecord wraps each backup entry
type DynamoRecord struct {
	Keys                        map[string]*dynamodb.Attribute `json:"Keys"`
	NewImage                    map[string]*dynamodb.Attribute `json:"NewImage,omitempty"`
	OldImage                    map[string]*dynamodb.Attribute `json:"OldImage,omitempty"`
	SequenceNumber              string                         `json:"SequenceNumber"`
	SizeBytes                   int                            `json:"SizeBytes"`
	ApproximateCreationDateTime int64                          `json:"ApproximateCreationDateTime"`
	EventName                   string                         `json:"eventName"`
}
