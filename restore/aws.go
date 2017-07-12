package restore

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	gaws "github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/s3"
)

// AWS wraps s3 and dynamo
type AWS struct {
	Bucket *s3.Bucket
	Dynamo *dynamodb.DynamoDB
	Config *AWSConfig
}

// AWSConfig wraps s3 bucket and dynamo tables
type AWSConfig struct {
	Bucket string
	Prefix string
	Tables []string
	Region gaws.Region
}

// NewAWS creates struct that wraps AWS configs
func NewAWS(cfg *AWSConfig) (*AWS, error) {
	s3Svc, err := s3Svc(cfg)
	if err != nil {
		return nil, err
	}
	return &AWS{s3Svc.Bucket(cfg.Bucket), dynamo(cfg), cfg}, nil
}
