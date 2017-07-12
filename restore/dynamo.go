package restore

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

// Terrible hack because time doesn't marshal properly from StreamRecord
type StreamRecordWrapper struct {
	ApproximateCreationDateTime *time.Time                          `json:"ApproximateCreationDateTime"`
	Keys                        map[string]*dynamodb.AttributeValue `json:"Keys"`
	NewImage                    map[string]*dynamodb.AttributeValue `json:"NewImage,omitempty"`
	OldImage                    map[string]*dynamodb.AttributeValue `json:"OldImage,omitempty"`
	SequenceNumber              *string                             `json:"SequenceNumber"`
	SizeBytes                   *int64                              `json:"SizeBytes"`
	StreamViewType              *string                             `json:"StreamViewType,omitempty"`
	EventName                   string                              `json:"eventName"`
}

func (s *StreamRecordWrapper) UnmarshalJSON(b []byte) error {
	type Alias StreamRecordWrapper
	aux := &struct {
		ApproximateCreationDateTime int64 `json:"ApproximateCreationDateTime"`
		*Alias
	}{
		Alias: (*Alias)(s),
	}
	if err := json.Unmarshal(b, &aux); err != nil {
		fmt.Println("Error in the streamrecordwrapper unmarshal: ", err.Error())
		return err
	}
	converted := time.Unix(aux.ApproximateCreationDateTime, 0)
	s.ApproximateCreationDateTime = &converted
	return nil
}

func (s *StreamRecordWrapper) CreateWriteRequest() *dynamodb.WriteRequest {
	// Insert & modify are both put requests
	if s.EventName == dynamodbstreams.OperationTypeInsert || s.EventName == dynamodbstreams.OperationTypeModify {
		return &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: s.NewImage,
			},
		}
	}
	return &dynamodb.WriteRequest{
		DeleteRequest: &dynamodb.DeleteRequest{
			Key: s.OldImage,
		},
	}
}

// GetTable gets Dynamo Table
func (a *AWS) getTable(name string) (*dynamodb.TableDescription, error) {
	res, err := a.Dynamo.DescribeTable(&dynamodb.DescribeTableInput{TableName: &name})
	if err != nil {
		return nil, err
	}
	return res.Table, nil
}
