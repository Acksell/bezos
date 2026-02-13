package ddbsdk

import (
	"context"
	"fmt"

	dynamodbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func (c *Client) UpdateItem(ctx context.Context, u UpdateItemAction) error {
	update, err := u.ToUpdateItem()
	if err != nil {
		return fmt.Errorf("failed to convert update to update item: %w", err)
	}
	_, err = c.awsddb.UpdateItem(ctx, update)
	if err != nil {
		return fmt.Errorf("failed to update item: %w", err)
	}
	return nil
}

type UpdateItemAction interface {
	ToUpdateItem() (*dynamodbv2.UpdateItemInput, error)
}

var _ UpdateItemAction = &UnsafeUpdate{}
