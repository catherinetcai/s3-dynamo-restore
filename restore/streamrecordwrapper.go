package restore

import (
	"encoding/json"
	"fmt"
	"reflect"
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

type StreamRecordWrappers []*StreamRecordWrapper

func (s *StreamRecordWrappers) Remove(i int) {
	c := *s
	c = append(c[:i], c[i+1:]...)
	*s = c
}

func (s StreamRecordWrappers) Len() int {
	return len(s)
}

func (s StreamRecordWrappers) Less(i, j int) bool {
	return s[i].ApproximateCreationDateTime.Before(*s[j].ApproximateCreationDateTime)
}

func (s StreamRecordWrappers) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s *StreamRecordWrappers) RemoveDupes() {
	c := *s
	dupeKey := map[string]int{}
	for i, rec := range c {
		// Check for a duplicate key
		attr := *rec.GetImage()[rec.KeyName()].S
		if j, ok := dupeKey[attr]; ok {
			c.Remove(j)
			dupeKey[attr] = i - 1
		} else {
			dupeKey[attr] = i
		}
	}
	*s = c
}

type WriteRequests []*dynamodb.WriteRequest

func (w *WriteRequests) Remove(i int) {
	c := *w
	c = append(c[:i], c[i+1:]...)
	*w = c
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
	if s.isInsertOrModifyOperation() {
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

func (s *StreamRecordWrapper) isInsertOrModifyOperation() bool {
	return s.EventName == dynamodbstreams.OperationTypeInsert || s.EventName == dynamodbstreams.OperationTypeModify
}

func (s *StreamRecordWrapper) KeyName() (keyName string) {
	// Ghetto hack to fetch back the key
	for k, _ := range s.Keys {
		keyName = k
		break
	}
	return keyName
}

func (s *StreamRecordWrapper) IsDupe(os *StreamRecordWrapper) bool {
	own := s.GetImage()
	other := os.GetImage()
	return reflect.DeepEqual(own[s.KeyName()], other[s.KeyName()])
}

func (s *StreamRecordWrapper) GetImage() map[string]*dynamodb.AttributeValue {
	if s.isInsertOrModifyOperation() {
		return s.NewImage
	}
	return s.OldImage
}
