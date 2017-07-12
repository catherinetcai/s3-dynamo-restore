package restore

import (
	"bufio"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/davecgh/go-spew/spew"
	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/s3"
)

const (
	s3Max = 1000
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
	Region aws.Region
}

// NewAWS creates struct that wraps AWS configs
func NewAWS(cfg *AWSConfig) (*AWS, error) {
	s3Svc, err := s3Svc(cfg)
	if err != nil {
		return nil, err
	}
	return &AWS{s3Svc.Bucket(cfg.Bucket), dynamo(), cfg}, nil
}

func s3Svc(cfg *AWSConfig) (*s3.S3, error) {
	auth, err := aws.SharedAuth()
	if err != nil {
		return nil, err
	}
	return s3.New(auth, cfg.Region), nil
}

func dynamo() *dynamodb.DynamoDB {
	sess := session.Must(session.NewSession())
	return dynamodb.New(sess)
}

// Lists bucket contents as a slice of strings
func (a *AWS) List() ([]string, error) {
	objs, err := a.Bucket.GetBucketContents()
	if err != nil {
		return nil, err
	}
	return convertKeys(objs), nil
}

func convertKeys(contents *map[string]s3.Key) []string {
	var keys []string
	for key := range *contents {
		keys = append(keys, key)
	}
	return keys
}

func (a *AWS) ListWithPrefix(prefix string) ([]string, error) {
	bucketContents := map[string]s3.Key{}
	separator := ""
	marker := ""
	for {
		contents, err := a.Bucket.List(prefix, separator, marker, 1000)
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

// BatchGet from S3 bucket
func (a *AWS) BatchGet(keys []string) ([]*StreamRecordWrapper, error) {
	var recs []*StreamRecordWrapper
	for _, key := range keys {
		rec, err := a.Get(key)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		recs = append(recs, rec...)
	}
	return recs, nil
}

// Get from S3
func (a *AWS) Get(key string) ([]*StreamRecordWrapper, error) {
	ioreader, err := a.Bucket.GetReader(key)
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReader(ioreader)
	var recs []*StreamRecordWrapper
	for {
		entry, err := reader.ReadBytes('\n')
		if err != nil {
			break
		}
		rec := &StreamRecordWrapper{}
		err = json.Unmarshal(entry, rec)
		if err != nil {
			fmt.Println("Error unmarshalling entry: ", err.Error())
			continue
		}
		recs = append(recs, rec)
	}
	return recs, nil
}

// BatchWrite to Dynamo
func (a *AWS) BatchWrite(tableName string, recs []*StreamRecordWrapper) error {
	// Structure is map[tablename][]WriteRequest{PutRequest/DeleteRequest}
	// Just does a quick check to ensure the table exists
	_, err := a.getTable(tableName)
	if err != nil {
		return err
	}
	var wrs []*dynamodb.WriteRequest
	for _, rec := range recs {
		wrs = append(wrs, rec.CreateWriteRequest())
	}
	req := map[string][]*dynamodb.WriteRequest{tableName: wrs}
	res, err := a.Dynamo.BatchWriteItem(&dynamodb.BatchWriteItemInput{RequestItems: req})
	if err != nil {
		return err
	}
	spew.Dump(res)
	return nil
}

// CreateTable creates a table
func (a *AWS) CreateTableFrom(original, target string) error {
	res, err := a.Dynamo.DescribeTable(&dynamodb.DescribeTableInput{TableName: &original})
	if err != nil {
		return err
	}
	originalTd := res.Table
	_, err = a.createEmptyTableClone(originalTd)
	return err
}

func (a *AWS) createEmptyTableClone(td *dynamodb.TableDescription) (*dynamodb.TableDescription, error) {
	var globalSecondaryIndexes []*dynamodb.GlobalSecondaryIndex
	var localSecondaryIndexes []*dynamodb.LocalSecondaryIndex
	globalSecondaryIndexDescs := td.GlobalSecondaryIndexes
	for _, globalSecondaryIndexDesc := range globalSecondaryIndexDescs {
		globalSecondaryIndex := createGlobalSecondaryIndexFromDescription(globalSecondaryIndexDesc)
		globalSecondaryIndexes = append(globalSecondaryIndexes, globalSecondaryIndex)
	}
	for _, localSecondaryIndexDesc := range td.LocalSecondaryIndexes {
		localSecondaryIndex := createLocalSecondaryIndexFromDescription(localSecondaryIndexDesc)
		localSecondaryIndexes = append(localSecondaryIndexes, localSecondaryIndex)
	}

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions:   td.AttributeDefinitions,
		GlobalSecondaryIndexes: globalSecondaryIndexes,
		KeySchema:              td.KeySchema,
		LocalSecondaryIndexes:  localSecondaryIndexes,
		ProvisionedThroughput:  createProvisionedThroughputFromDescription(td.ProvisionedThroughput),
		StreamSpecification:    td.StreamSpecification,
		TableName:              td.TableName,
	}
	res, err := a.Dynamo.CreateTable(input)
	if err != nil {
		return nil, err
	}
	return res.TableDescription, nil
}

func createGlobalSecondaryIndexFromDescription(gsid *dynamodb.GlobalSecondaryIndexDescription) *dynamodb.GlobalSecondaryIndex {
	return &dynamodb.GlobalSecondaryIndex{
		IndexName:             gsid.IndexName,
		KeySchema:             gsid.KeySchema,
		Projection:            gsid.Projection,
		ProvisionedThroughput: createProvisionedThroughputFromDescription(gsid.ProvisionedThroughput),
	}
}

func createLocalSecondaryIndexFromDescription(lsid *dynamodb.LocalSecondaryIndexDescription) *dynamodb.LocalSecondaryIndex {
	return &dynamodb.LocalSecondaryIndex{
		IndexName:  lsid.IndexName,
		KeySchema:  lsid.KeySchema,
		Projection: lsid.Projection,
	}
}

func createProvisionedThroughputFromDescription(ptd *dynamodb.ProvisionedThroughputDescription) *dynamodb.ProvisionedThroughput {
	return &dynamodb.ProvisionedThroughput{
		ReadCapacityUnits:  ptd.ReadCapacityUnits,
		WriteCapacityUnits: ptd.WriteCapacityUnits,
	}
}
