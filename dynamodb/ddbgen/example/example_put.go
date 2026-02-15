package example

import (
	"context"
	"fmt"

	"github.com/acksell/bezos/dynamodb/ddbsdk"
)

func main() {
	// Create a new user entity
	user := &User{
		UserID: "123",
		Email:  "admin@example.com",
		Name:   "John Doe",
	}
	var aws ddbsdk.AWSDynamoClientV2
	db := ddbsdk.New(aws) // won't work if ran, but illustrates usage of the generated code

	ctx := context.Background()

	// Put the user into DynamoDB
	putOp := UserIndex.UnsafePut(user)
	if err := db.PutItem(ctx, putOp); err != nil {
		panic(fmt.Sprintf("failed to put item: %v", err))
	}

	// Update the user's name
	updateOp := UserIndex.UnsafeUpdate(user.UserID)
	updateOp.AddOp(ddbsdk.SetFieldOp("name", "Jane Doe"))
	if err := db.UpdateItem(ctx, updateOp); err != nil {
		panic(fmt.Sprintf("failed to update item: %v", err))
	}

	// Delete the user
	deleteOp := UserIndex.Delete(user.UserID)
	if err := db.DeleteItem(ctx, deleteOp); err != nil {
		panic(fmt.Sprintf("failed to delete item: %v", err))
	}

	// tx
	tx := db.NewTx()
	tx.AddAction(
		UserIndex.UnsafePut(user),
		UserIndex.UnsafeUpdate(user.UserID).AddOp(ddbsdk.SetFieldOp("name", "Jane Doe")),
		UserIndex.Delete(user.UserID),
	)
	if err := tx.Commit(context.Background()); err != nil {
		panic(fmt.Sprintf("failed to commit transaction: %v", err))
	}

	// query - using generated GSI builder
	_ = db.NewQuery(
		UserIndexGSI1.QueryPartition("admin@example.com").IdEquals(user.UserID),
	).Descending().PageSize(20)

	// query - using primary index builder
	_ = db.NewQuery(
		OrderIndex.QueryPartition("tenant-123").OrderIDBeginsWith("2024"),
	).Descending()

	batch := db.NewBatch()
	batch.AddAction(
		UserIndex.UnsafePut(user),
		UserIndex.UnsafePut(user),
		UserIndex.Delete(user.UserID))

	_, _ = db.NewQuery(
		RandomEntityIndex.QueryPartition(),
	).Descending().Next(ctx)
}
