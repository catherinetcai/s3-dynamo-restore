SOURCE ?= dynamo-backup-test
TARGET ?= test-table
BUCKET ?= your-backup-table-here
PREFIX ?= dynamodb/backup/

.PHONY: restore

clone:
	go run main.go clone --sourceTable ${SOURCE} --targetTable ${TARGET}
restore:
	go run main.go restore --sourceTable ${SOURCE} --targetTable ${TARGET} -b ${BUCKET} -p ${PREFIX}
