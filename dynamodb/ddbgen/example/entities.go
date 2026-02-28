package example

import "time"

// User represents a user entity stored in DynamoDB.
type User struct {
	UserID    string    `dynamodbav:"id"`
	Email     string    `dynamodbav:"email"`
	Name      string    `dynamodbav:"name"`
	UpdatedAt time.Time `dynamodbav:"updatedAt"`
}

// IsValid implements ddbsdk.DynamoEntity.
func (u *User) IsValid() error {
	return nil
}

// VersionField implements ddbsdk.VersionedDynamoEntity.
func (u *User) VersionField() (string, any) {
	return "updatedAt", u.UpdatedAt
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

// Message represents a chat message in a project's conversation history.
type Message struct {
	ChatID      string    `dynamodbav:"chatID"`
	SequenceNum int64     `dynamodbav:"sequenceNum"`
	Content     string    `dynamodbav:"content"`
	CreatedAt   time.Time `dynamodbav:"createdAt"`
}

// IsValid implements ddbsdk.DynamoEntity.
func (m *Message) IsValid() error {
	return nil
}
