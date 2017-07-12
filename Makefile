SOURCE ?= dynamo-backup-test
TARGET ?= test-table
BUCKET ?= fair-dynamo-backup-test
clone:
	go run main.go clone --sourceTable ${SOURCE} --targetTable ${TARGET}
restore:
	go run main.go restore --sourceTable ${SOURCE} --targetTable ${TARGET} --bucket ${BUCKET}
