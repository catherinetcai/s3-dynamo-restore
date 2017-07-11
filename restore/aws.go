package restore

import (
	"bufio"
	"encoding/json"
	"fmt"

	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/dynamodb"
	"github.com/goamz/goamz/s3"
)

const (
	s3Max = 1000
)

// AWS wraps s3 and dynamo
type AWS struct {
	Bucket *s3.Bucket
	Dynamo *dynamodb.Server
	Config *AWSConfig
}

// AWSConfig wraps s3 bucket and dynamo tables
type AWSConfig struct {
	Bucket string
	Prefix string
	Tables []string
	Region aws.Region
}

// NewAWS creates struct that wraps AWS configs
func NewAWS(cfg *AWSConfig) (*AWS, error) {
	auth, err := aws.SharedAuth()
	if err != nil {
		return nil, err
	}
	s3svc := s3.New(auth, cfg.Region)
	return &AWS{s3svc.Bucket(cfg.Bucket), &dynamodb.Server{auth, cfg.Region}, cfg}, nil
}

// Lists bucket contents as a slice of strings
func (a *AWS) List() ([]string, error) {
	objs, err := a.Bucket.GetBucketContents()
	if err != nil {
		return nil, err
	}
	return convertkeys(objs), nil
}

func convertKeys(contents *map[string]s3.Key) []string {
	var keys []string
	for key := range *contents {
		keys = append(keys, key)
	}
	return keys
}

func (a *AWS) ListWithPrefixAndTable(table string) ([]string, error) {
	bucketContents := map[string]s3.Key{}
	separator := ""
	marker := ""
	for {
		contents, err := a.Bucket.List(a.Config.Prefix+table+"/", separator, marker, 1000)
		if err != nil {
			return []string{}, err
		}
		for _, key := range contents.Contents {
			bucketContents[key.Key] = key
		}
		if contents.IsTruncated {
			marker = contents.NextMarker
		} else {
			break
		}
	}
	return convertKeys(&bucketContents), nil
}

func (a *AWS) Get(key string) ([]*DynamoRecord, error) {
	ioreader, err := a.Bucket.GetReader(key)
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReader(ioreader)
	var recs []*DynamoRecord
	for {
		entry, err := reader.ReadBytes('\n')
		if err != nil {
			break
		}
		rec := &DynamoRecord{}
		err = json.Unmarshal(entry, rec)
		if err != nil {
			fmt.Println("Error unmarshalling entry: ", err.Error())
			continue
		}
		recs = append(recs, rec)
	}
	return recs, nil
}
