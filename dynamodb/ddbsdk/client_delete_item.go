package ddbsdk

import (
	"context"
	"fmt"

	dynamodbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func (c *Client) DeleteItem(ctx context.Context, d DeleteItemAction) error {
	del, err := d.ToDeleteItem()
	if err != nil {
		return fmt.Errorf("failed to convert delete to delete item: %w", err)
	}
	_, err = c.awsddb.DeleteItem(ctx, del)
	if err != nil {
		return fmt.Errorf("failed to delete item: %w", err)
	}
	return nil
}

// DeleteAction is implemented by both Delete and DeleteWithCondition.
type DeleteItemAction interface {
	ToDeleteItem() (*dynamodbv2.DeleteItemInput, error)
}

var _ DeleteItemAction = &Delete{}
