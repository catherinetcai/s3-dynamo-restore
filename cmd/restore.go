package cmd

import (
	"fmt"

	"github.com/catherinetcai/s3-dynamo-restore/restore"
	"github.com/davecgh/go-spew/spew"
	"github.com/goamz/goamz/aws"
	"github.com/spf13/cobra"
)

/*
Prereqs:
- S3 bucket of backups + prefix of where backups are written
- Format of S3 is s3://bucket-name/prefix/YYYY/MM/DD/HH/file-as.gz
- DynamoDB tables to replay/restore backups
- Time to replay backups (maybe default is last 24h, but you can specify specific time ranges?)

Limitations:
- 1 bucket for all dynamo names
*/

var (
	bucketName   string
	bucketPrefix string
	endTime      string
	startTime    string
	sourceTable  string
	targetBucket string
	targetTable  string
)

var restoreCmd = &cobra.Command{
	Use:     "restore",
	PreRunE: checkRequiredRestoreFlags,
	RunE:    restoreFromBackup,
}

var restorePipelineCmd = &cobra.Command{
	Use:  "pipeline",
	RunE: restorePipeline,
}

func restorePipeline(cmd *cobra.Command, args []string) error {
	a := newAws()
	fmt.Println("Listing all keys from ", bucketName)
	keys, err := a.ListWithPrefix(a.Config.Prefix + sourceTable + "/")
	if err != nil {
		return err
	}
	fmt.Println("Batch getting all keys...")
	recs, err := a.BatchGetPipelineRecords(keys)
	if err != nil {
		return err
	}
	fmt.Println("Batch writing...")
	err = a.BatchWritePipelineRecords(targetTable, recs)
	if err != nil {
		return err
	}
	return nil
}

var s3Cmd = &cobra.Command{
	Use: "s3",
}

var getCmd = &cobra.Command{
	Use:   "get",
	Short: "Get and attempt to marshal into dynamo streamrecord",
	Run:   s3Get,
}

func init() {
	s3Cmd.AddCommand(getCmd)
}

func checkRequiredRestoreFlags(cmd *cobra.Command, args []string) error {
	if sourceTable == "" {
		return flagError("sourceTable")
	}
	if targetTable == "" {
		return flagError("targetTable")
	}
	if bucketName == "" {
		return flagError("bucket")
	}
	return nil
}

func restoreFromBackup(cmd *cobra.Command, args []string) error {
	a := newAws()
	// Gets back all keys associated with the Table name
	fmt.Println("Listing all keys from ", bucketName)
	keys, err := a.ListWithPrefix(a.Config.Prefix + sourceTable + "/")
	if err != nil {
		return err
	}
	fmt.Println("Batch getting all keys...")
	recs, err := a.BatchGetStreamRecordWrappers(keys)
	if err != nil {
		return err
	}
	fmt.Println("Batch writing...")
	err = a.BatchWriteStreamRecordWrappers(targetTable, recs)
	if err != nil {
		return err
	}
	return nil
}

func newAws() *restore.AWS {
	cfg := &restore.AWSConfig{
		Bucket: bucketName,
		Prefix: bucketPrefix,
		Tables: []string{sourceTable},
		Region: aws.USWest2,
	}
	a, _ := restore.NewAWS(cfg)
	return a
}

func s3Get(cmd *cobra.Command, args []string) {
	a := newAws()
	list, err := a.List()
	if err != nil {
		fmt.Println(err)
		return
	}
	get, _ := a.GetStreamRecordWrappers(list[0])
	spew.Dump(get)
}
