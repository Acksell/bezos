package normddb

type QueryPattern string

const (
	PartitionKeyPagination = "pkPagination"
	SortedPagination       = "skPagination"
	DirectLookup           = "directLookup"
)

// import (
// 	"context"

// 	dynamodbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
// )

// type Getter struct {
// 	ddb *dynamodbv2.Client
// }

// func NewGet(table TableDescription, pk PrimaryKey) *Getter {
// 	return &Getter{
// 		ddb: ddb,
// 	}
// }

// func (g *Getter) Lookup(ctx context.Context) (DynamoEntity, error) {
// 	//? implement this
// 	return nil, nil
// }

// func (g *Getter) LookupMany(ctx context.Context) (DynamoEntity, error) {
// 	//? implement this
// 	return nil, nil
// }

// type Querier struct {
// 	ddb *dynamodbv2.Client //? separate the client from this type and instead use different custom ddb client?
// }

// func NewQuery(table TableDescription, pk PrimaryKey) *Querier {
// 	return &Querier{
// 		ddb: ddb,
// 	}
// }

// func (q *Querier) Query(ctx context.Context) (QueryResult, error) {
// 	//? implement this
// 	return QueryResult{}, nil
// }

// func (q *Querier) Next(ctx context.Context) (QueryResult, error) {
// 	//? implement this
// 	return QueryResult{}, nil
// }

// func main() {
// 	// q := normddb.NewQuery(PrimaryKey{Names: Table.KeyNames, Values: KeyValues{PartitionKey: "TEST_INDEX"}})
// 	// var e []DynamoEntity
// 	// for res, err := q.Next(ctx) {
// 	// 	e = append(e, res...)
// 	// }
// }
