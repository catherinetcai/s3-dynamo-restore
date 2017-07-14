package restore

import "github.com/aws/aws-sdk-go/service/dynamodb"

type PipelineRecords []*PipelineRecord
type PipelineRecord map[string]*dynamodb.AttributeValue

func (p *PipelineRecord) CreateWriteRequest() *dynamodb.WriteRequest {
	return &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{
			Item: *p,
		},
	}
}
