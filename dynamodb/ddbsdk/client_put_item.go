package ddbsdk

import (
	"context"
	"fmt"

	dynamodbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func (c *Client) PutItem(ctx context.Context, p PutItemAction) error {
	put, err := p.ToPutItem()
	if err != nil {
		return fmt.Errorf("failed to convert put to put item: %w", err)
	}
	_, err = c.awsddb.PutItem(ctx, put)
	if err != nil {
		return fmt.Errorf("failed to put item: %w", err)
	}
	return nil
}

// PutItemAction is implemented by both Put and PutWithCondition.
type PutItemAction interface {
	ToPutItem() (*dynamodbv2.PutItemInput, error)
}

var _ PutItemAction = &Put{}
