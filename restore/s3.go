package restore

import (
	"bufio"
	"encoding/json"
	"fmt"

	gaws "github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/s3"
)

const (
	s3Max = 99999
)

func s3Svc(cfg *AWSConfig) (*s3.S3, error) {
	auth, err := gaws.SharedAuth()
	if err != nil {
		return nil, err
	}
	return s3.New(auth, cfg.Region), nil
}

// Lists bucket contents as a slice of strings
func (a *AWS) List() ([]string, error) {
	objs, err := a.Bucket.GetBucketContents()
	if err != nil {
		return nil, err
	}
	return convertKeys(objs), nil
}

func convertKeys(contents *map[string]s3.Key) []string {
	var keys []string
	for key := range *contents {
		keys = append(keys, key)
	}
	return keys
}

// ListWithPrefix from S3 bucket
func (a *AWS) ListWithPrefix(prefix string) ([]string, error) {
	bucketContents := map[string]s3.Key{}
	separator := ""
	marker := ""
	for {
		res, err := a.Bucket.List(prefix, separator, marker, s3Max)
		if err != nil {
			return []string{}, err
		}
		for _, key := range res.Contents {
			bucketContents[key.Key] = key
		}
		if res.IsTruncated {
			marker = res.NextMarker
		} else {
			break
		}
	}
	return convertKeys(&bucketContents), nil
}

// BatchGet from S3 bucket
func (a *AWS) BatchGet(keys []string) (StreamRecordWrappers, error) {
	var recs StreamRecordWrappers
	for _, key := range keys {
		rec, err := a.Get(key)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		recs = append(recs, rec...)
	}
	return recs, nil
}

// Get from S3
func (a *AWS) Get(key string) (StreamRecordWrappers, error) {
	ioreader, err := a.Bucket.GetReader(key)
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReader(ioreader)
	var recs StreamRecordWrappers
	for {
		entry, err := reader.ReadBytes('\n')
		if err != nil {
			break
		}
		rec := &StreamRecordWrapper{}
		err = json.Unmarshal(entry, rec)
		if err != nil {
			fmt.Println("Error unmarshalling entry: ", err.Error())
			continue
		}
		recs = append(recs, rec)
	}
	return recs, nil
}
