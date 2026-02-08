package ddbsdk

// We encourage you to implement IsValid for every entity struct.
// We will call it before committing.
type DynamoEntity interface {
	IsValid() error
}
