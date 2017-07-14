package restore

import (
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	BatchWriteItemSizeLimit = 25
)

func dynamo(cfg *AWSConfig) *dynamodb.DynamoDB {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: &cfg.Region.Name,
	}))
	return dynamodb.New(sess)
}

// GetTable gets Dynamo Table
func (a *AWS) getTable(name string) (*dynamodb.TableDescription, error) {
	res, err := a.Dynamo.DescribeTable(&dynamodb.DescribeTableInput{TableName: &name})
	if err != nil {
		return nil, err
	}
	return res.Table, nil
}

// BatchWrite to Dynamo
func (a *AWS) BatchWriteStreamRecordWrappers(targetTable string, recs StreamRecordWrappers) error {
	_, err := a.getTable(targetTable)
	if err != nil {
		return err
	}
	var wrs WriteRequests
	sort.Sort(recs)
	fmt.Println("Checking and removing any dupes...")
	recs.RemoveDupes()
	for _, rec := range recs {
		wr := rec.CreateWriteRequest()
		wrs = append(wrs, wr)
	}
	a.pushBatchWriteRequests(targetTable, wrs)
	return nil
}

func (a *AWS) BatchWritePipelineRecords(targetTable string, recs PipelineRecords) error {
	_, err := a.getTable(targetTable)
	if err != nil {
		return err
	}
	var wrs WriteRequests
	for _, rec := range recs {
		wr := rec.CreateWriteRequest()
		wrs = append(wrs, wr)
	}
	a.pushBatchWriteRequests(targetTable, wrs)
	return nil
}

func (a *AWS) pushBatchWriteRequests(targetTable string, wrs WriteRequests) {
	for i := 0; i < len(wrs); i += BatchWriteItemSizeLimit {
		var writeItem WriteRequests
		if i+BatchWriteItemSizeLimit > len(wrs) {
			writeItem = wrs[i:]
		} else {
			writeItem = wrs[i : i+BatchWriteItemSizeLimit]
		}
		req := map[string][]*dynamodb.WriteRequest{targetTable: writeItem}
		res, err := a.Dynamo.BatchWriteItem(&dynamodb.BatchWriteItemInput{RequestItems: req})
		if err != nil {
			fmt.Println("Error posting batch request...", err)
			continue
		}
		fmt.Println("Batch write successful: ", res)
	}
}

// CreateTableFrom clones a table's attributes
func (a *AWS) CreateTableFrom(original, target string) error {
	res, err := a.Dynamo.DescribeTable(&dynamodb.DescribeTableInput{TableName: &original})
	if err != nil {
		return err
	}
	originalTd := res.Table
	_, err = a.createEmptyTableClone(originalTd, target)
	return err
}

func (a *AWS) createEmptyTableClone(td *dynamodb.TableDescription, target string) (*dynamodb.TableDescription, error) {
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
		TableName:              &target,
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
