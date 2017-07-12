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
func (a *AWS) BatchWrite(sourceTable, targetTable string, recs []*StreamRecordWrapper) error {
	// Structure is map[tablename][]WriteRequest{PutRequest/DeleteRequest}
	// Just does a quick check to ensure the table exists
	_, err := a.getTable(sourceTable)
	if err != nil {
		return err
	}
	var wrs []*dynamodb.WriteRequest
	for _, rec := range recs {
		wrs = append(wrs, rec.CreateWriteRequest())
	}
	writeTable := sourceTable
	if targetTable != "" {
		err := a.CreateTableFrom(sourceTable, targetTable)
		if err != nil {
			return err
		}
		writeTable = targetTable
	}
	fmt.Println("Table to write write request to: ", writeTable)
	req := map[string][]*dynamodb.WriteRequest{writeTable: wrs}
	spew.Dump("Dynamo batch write ", req)
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
