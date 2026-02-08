package ddbsdk

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func ttlDDB(expiry time.Time) *types.AttributeValueMemberN {
	return &types.AttributeValueMemberN{
		Value: fmt.Sprintf("%d", expiry.Unix()),
	}
}
