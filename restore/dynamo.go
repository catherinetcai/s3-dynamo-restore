package restore

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/davecgh/go-spew/spew"
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
func (a *AWS) BatchWrite(targetTable string, recs []*StreamRecordWrapper) error {
	_, err := a.getTable(targetTable)
	if err != nil {
		return err
	}
	dupeCheck := map[string]int{}
	var wrs StreamRecordWrappers
	for _, rec := range recs {
		// If dupe, then remove the record at that index, append the new rec, and update the index
		k := rec.KeyName()
		wr := rec.CreateWriteRequest()
		spew.Dump("Dupe check ", dupeCheck)
		if idx, ok := dupeCheck[k]; ok {
			fmt.Println("Found dupe. Removing from slice...")
			wrs.Remove(idx)
		}
		if rec.insertOrModifyOperation() {
			wrs = append(wrs, wr)
		}
		dupeCheck[k] = len(wrs) - 1
	}
	fmt.Println("Table to write write request to: ", targetTable)
	req := map[string][]*dynamodb.WriteRequest{targetTable: wrs}
	bwi := &dynamodb.BatchWriteItemInput{RequestItems: req}
	spew.Dump(bwi)
	res, err := a.Dynamo.BatchWriteItem(bwi)
	if err != nil {
		return err
	}
	spew.Dump(res)
	return nil
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
