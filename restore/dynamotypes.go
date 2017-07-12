package restore

import (
	"encoding/json"
	"errors"
	"fmt"
)

const (
	DynamoBinaryType    DynamoType = "B"
	DynamoBoolType      DynamoType = "BOOL"
	DynamoBinarySetType DynamoType = "BS"
	DynamoListType      DynamoType = "L"
	DynamoMapType       DynamoType = "M"
	DynamoNumberType    DynamoType = "N"
	DynamoNumberSetType DynamoType = "NS"
	DynamoNullType      DynamoType = "NULL"
	DynamoStringType    DynamoType = "S"
	DynamoStringSetType DynamoType = "SS"
)

func IsValidDynamoType(dtype string) bool {
	switch DynamoType(dtype) {
	case
		DynamoBinaryType, DynamoBoolType, DynamoBinarySetType, DynamoListType, DynamoMapType, DynamoNumberType, DynamoNumberSetType, DynamoNullType, DynamoStringType, DynamoStringSetType:
		return true
	}
	return false
}

// DynamoType wraps a DynamoType
type DynamoType string

func (d DynamoType) UnmarshalJSON(b []byte) error {
	var valid *DynamoType
	if IsValidDynamoType(string(b)) {
		return json.Unmarshal(b, valid)
	}
	return errors.New(fmt.Sprintf("Error: unable to marshal %v into DynamoType", b))
}
