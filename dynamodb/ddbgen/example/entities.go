package example

// User represents a user entity stored in DynamoDB.
type User struct {
	UserID string `dynamodbav:"id"`
	Email  string `dynamodbav:"email"`
	Name   string `dynamodbav:"name"`
}

// IsValid implements ddbsdk.DynamoEntity.
func (u *User) IsValid() error {
	return nil
}

// Order represents an order entity stored in DynamoDB.
type Order struct {
	TenantID string `dynamodbav:"tenantID"`
	OrderID  string `dynamodbav:"orderID"`
	Amount   int    `dynamodbav:"amount"`
}

// IsValid implements ddbsdk.DynamoEntity.
func (o *Order) IsValid() error {
	return nil
}
