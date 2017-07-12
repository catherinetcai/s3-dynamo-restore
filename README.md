## S3 to Dynamo Restore

This tool takes dumps from the [DynamoDB Continuous Backup](https://github.com/awslabs/dynamodb-continuous-backup) tool and dumps them into a Dynamo table of your choice.

### Caveats
- String-type attributes only (for now)
- No time ranges, will batch write an entire S3 backup to the target Dynamo table
