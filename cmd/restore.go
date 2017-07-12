package cmd

import (
	"errors"
	"fmt"

	"github.com/catherinetcai/s3-dynamo-restore/restore"
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
	dynamoTables []string
	endTime      string
	startTime    string
	targetBucket string
)

var restoreCmd = &cobra.Command{
	Use:     "restore",
	PreRunE: checkRequiredFlags,
}

var s3Cmd = &cobra.Command{
	Use: "s3",
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List from S3",
	Run:   s3List,
}

var getCmd = &cobra.Command{
	Use:   "get",
	Short: "Get and attempt to marshal into dynamo streamrecord",
	Run:   s3Get,
}

func init() {
	s3Cmd.AddCommand(listCmd)
	s3Cmd.AddCommand(getCmd)
	RootCmd.PersistentFlags().StringVarP(&dynamoTable, "table", "", "", "Dynamo table to get backups from")
	RootCmd.PersistentFlags().StringVarP(&targetTable, "targetTable", "", "", "Dynamo table to write backups to")
	RootCmd.PersistentFlags().StringVarP(&bucketName, "bucket", "b", "", "Bucket name to read backups from")
	RootCmd.PersistentFlags().StringVarP(&bucketPrefix, "prefix", "p", "/", "Bucket prefix that backups are written to")
	RootCmd.PersistentFlags().StringVarP(&startTime, "startTime", "s", "", "Time point to restore backups from. Format: YYYY-MM-DD-HH:MM")
	RootCmd.PersistentFlags().StringVarP(&endTime, "endTime", "e", "", "Time point to restore backups from. Format: YYYY-MM-DD-HH:MM")
}

func checkRequiredFlags(cmd *cobra.Command, args []string) error {
	if len(dynamoTables) == 0 {
		return flagError("tables")
	}
	if bucketName == "" {
		return flagError("bucket")
	}
	return nil
}

func flagError(flag string) error {
	return errors.New("Error: Missing required flag " + flag)
}

func restoreFromBackup(cmd *cobra.Command, args []string) {
	a := newAws()
	// Gets back all keys associated with the Table name
	keys, err := a.ListWithPrefix(a.Config.Prefix + dynamoTable + "/")
	if err != nil {
		panic(err)
	}
	recs, err := a.BatchGet(keys)
	if err != nil {
		panic(err)
	}
	err = a.BatchWrite(dynamoTable, recs)
}

func s3List(cmd *cobra.Command, args []string) {
	//a := newAws()
}

func newAws() *restore.AWS {
	cfg := &restore.AWSConfig{
		Bucket: "fair-dynamo-backup-test",
		Prefix: "dynamodb/backup/",
		Tables: []string{"dynamo-backup-test"},
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
	get, _ := a.Get(list[0])
	fmt.Println(get)
}
