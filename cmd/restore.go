package cmd

import (
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

var dynamoTables []string
var bucketName string
var bucketPrefix string
var startTime string
var endTime string

var restoreCmd = &cobra.Command{
	Use: "restore",
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
	RootCmd.PersistentFlags().StringSliceVarP(&dynamoTables, "tables", "", []string{}, "List of dynamo tables to apply backups to")
	RootCmd.PersistentFlags().StringVarP(&bucketName, "bucket", "b", "", "Bucket name to read backups from")
	RootCmd.PersistentFlags().StringVarP(&bucketPrefix, "prefix", "p", "/", "Bucket prefix that backups are written to")
	RootCmd.PersistentFlags().StringVarP(&startTime, "startTime", "s", "", "Time point to restore backups from. Format: YYYY-MM-DD-HH:MM")
	RootCmd.PersistentFlags().StringVarP(&endTime, "endTime", "e", "", "Time point to restore backups from. Format: YYYY-MM-DD-HH:MM")
}

func restoreFromBackup(cmd *cobra.Command, args []string) {
	a := newAws()

}

func s3List(cmd *cobra.Command, args []string) {
	a := newAws()
	spew.Dump(a.List())
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
	list, _ := a.List()
	entry := list[0]
	spew.Dump(entry)
	get, _ := a.Get(list[0])
	spew.Dump(get)
}
