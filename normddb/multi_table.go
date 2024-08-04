package normddb

import "time"

type MultiTableSetup struct {
}

type SingleTableSetup struct {
}

// DynamoDB transactions can span multiple tables, but single-table designs are also common.
// This interface is a way to abstract over the two different design patterns, and to standardize
// how to write code that interacts with the underlying table infrastructure. On top of this, the
// act of writing a specification for your tables that's on a higher level than the infrastructure
// allows metadata information about the tables to be used for validation, code generation, and
// prevent mistakes.
//
// The primary purpose of a table interface is to produce a client that is able to interact with the
// table infrastructure.
// The table spec is passed to another library producing clients. This client will be able to create
// transactions against the table(s), while all underlying projections/index updates are automated.
// This is done by having the indices be responsible for creating all primarykeys in-line with spec.
//
// Indices take in user-defined operations on entities, and expose various queries.
// Projections are indices, but the operations are inferred (projected) from operations on a base table.
// So a projection is an index that just exposes queries, not operations. They can be used to implement a GSI that is consistent.
// A GSI is thus a projection that is eventually consistent.
// Indices, projections and GSI's can be used to decouple the write model from the read model.
//
// A table interface is thus a collection of operations and queries.
//
// Furthermore, in the norm framework, we can use custom projections to implement an event sourced state machine.
// This works because the event log is append only, so we can hook into the Put action, and access the event that was appended.
//
// Using event sourced projections you also get backfills and replays for free. You just need an event log and a starting state.
// That's the nice part about separating the write-model from the read-model: The read model should always be consistent with the write model,
// and updating read-models does not produce new side effects.
// todo: What if the read model is used in the write model? How to replay?
// todo: Is this interface really needed? Isn't the index(<-underlyingtable) abstraction enough? The tables are needed in order to fit the infra to the client, but the client only needs the indices. The client's job is to make sure that it will work with the underlying infra. Thus this interface should expose the necessary methods to instantiate the client.
type TableInterface interface {
	RegisterIndex(Index)
	Indices() []Index

	// RegisterEntity(EntitySchema)
	// Entity name -> index
	Entities() map[string]Index
}

// !leftoff TODO:
// ! 1. Should attempt to program a generic Index keyformat, and then make prefixformat.
// ! The reason is optimisation and backward compatibility: Using prefixes for the constant part of the key is not the same as using const#dynval1#const#dynval2,
// ! because you can extend the index without changing the keyformat - in a non-breaking way. For example, adding const#dynval1#const#dynval2#const#dynval3
// ! This would mean you don't have to store a new document.
// ! Would also allow users to define their own custom table format in order to migrate to this solution.
//
// ! 2. GSI keys/extra keys, how to handle these? Specified on the entity or the index? Probably on the index, and then decide which entities go into this index.
// ! 2.(related) idea: every entity in an index should get extra keys such that you can at any moment turn on a GSI for that index.
// todo how to represent GSI and LSI? eventual consistency etc.
//
// secondary index projections can be automatic - operations would be intercepted at the top table level.
type Index interface {
	// Each index should know about its spec
	Spec() *IndexSpec

	// ----Operation methods
	// NewTTL() time.Time
	// Put returns Put operation with some prefilled information, like the tablename, default table ttl key, etc.
	// todo: What is a DynamoEntity?
	//? : Is it generated or defined by user? I think the index logic should be able to generate wrapper structs for the entities. So dynamo lib allows user-defined ones, but index lib provides generated ones.
	// todo Should the gsikeys be on the entity or the index?
	// ? The index should be an abstraction on top of this, so it should be both. The index/subtable knows about all GSI's, so it will generate GSIKeys method.
	KeyFromEntity(DynamoEntity) PrimaryKey
	NewPut(DynamoEntity) *Put
	NewUpdate(PrimaryKey) *UnsafeUpdate
	NewDelete(PrimaryKey) *Delete
}

type IndexSpec struct {
	Name            string
	UnderlyingTable TableDescription
	// All entities put into this table get this TTL unless overridden by the Put operation.
	// Zero duration means indefinite storage
	DefaultTTL time.Duration

	Projections []ProjectionSpec
	// Queries
	// NewQuery(DynamoEntity) *Query
}

// type KeyStrategy string

// const (
// 	// just an example
// 	observed_timestamp_history = "observed_timestamp_history"
// 	// todo add more
// )

// func TimestampedHistoryKey(e DynamoEntity)  {

// }

// type CustomIndex struct {
// }

// func t() {
// 	var idx Index
// 	idx.NewPut().WithTTL()
// }

type TableDescription struct {
	Name           string
	KeyDefinitions PrimaryKeyDefinition
	TimeToLiveKey  string
	GSIKeys        []PrimaryKeyDefinition

	// Optional field for registering entity schemas.
	// Allows for validation of database operations.
	// Entities map[string]EntitySchema
	Indicies []Index
}

// type KeyPrefix struct {
// 	PartitionKeyPrefix string
// 	SortKeyPrefix      string
// }

// type SingleTableIndex struct {
// 	KeyPrefix KeyPrefix
// 	KeyNames  KeyNames

// 	// If true, the index is eventually consistent.
// 	// This means that the index may not reflect the latest data.
// 	// This also implies that there is an eventconsumer ingesting the data.
// 	//
// 	// If false, the index is strongly consistent and will be updated
// 	// in every write automatically.
// 	EventuallyConsistent bool // todo disallow breaking change.

// 	// Optional field for registering entity schemas.
// 	// Allows for validation of database operations.
// 	Entities map[string]EntitySchema
// }

// func (i SingleTableIndex) NewPrimaryKey(pk, sk string) PrimaryKey {
// 	return PrimaryKey{
// 		Names: i.KeyNames,
// 		Values: KeyValues{
// 			PartitionKey: pk,
// 			SortKey:      sk,
// 		},
// 	}
// }

// func (i Index) PrimaryKey() PrimaryKey {
// 	skfmt
// 	return PrimaryKey{
// 		Names: i.KeyNames,
// 	}
// }

// SingeTableIndex

// type IndexType string

// const (
// 	PrimaryIndex         IndexType = "Primary"
// 	GlobalSecondaryIndex IndexType = "GSI"
// 	LocalSecondaryIndex  IndexType = "LSI"
// )

// Projections are attached to an index where the source data is stored.
// Initially we only implement eventually consistent projections, aka GSIs.
// But some services use "consistent projections", this will be added later.
type ProjectionSpec struct {
	Name string
	// Used for knowing the GSI's, and also for temporal logic checks warning of potential race conditions.
	// When true, the index
	EventuallyConsistent bool

	// KeyNames defines the primary key of the new document stored.
	KeyDefinitions PrimaryKeyDefinition

	// todo: implement
	Strategy ProjectionStrategy

	Supports []QueryPattern
}

type ProjectionStrategy string

const (
	KEYS_ONLY ProjectionStrategy = "KEYS_ONLY"
	INCLUDE   ProjectionStrategy = "INCLUDE"
	ALL       ProjectionStrategy = "ALL"
)

/*
Table:
CustomTable (in reality it's an index with a special pk sk format)
SingleTable (pk sk)

Index:
Index within SingleTable
(GSI - infra managed)
(LSI - infra managed)

Projections:
- Event sourced projection
- Action -> Action (event sourcing)
- 1-1 projection to index (like GSI and LSI)
======================================
Table is just the base dynamo structure.
A table needs to have at least one index.

	(if it's a CustomTable it's just the canonical single index) (should maybe be called SinglePurposeTable, singleentitytable)
	(if it's a SingleTable it can have multiple indices, but needs at least one subindex) (funnily should maybe be called MultiPurposeTable, multientity table)

That index can be the *state* stream or the *event* stream.

It can also be a subentity of a root aggregate, or an index for read-queries.
Projections are attached to an index.
Projections define how you write to the index - projections define the process for putting entities into the index.
An index (?projection) can autogenerate queries for it, based on the key structure and entities.

In order to write to an index you need a projection defined.

# Event sourced projections can be materialized

Need to define adapter types: These are injected in the gencode via adapter plugins?
Is it async or sync?

projections are just different types of *state machines* - each subentity is a state machine.
State machine types:
  - Current state + command (left fold, this is a projection)
  - Pure command (just persist events, )

Root:

	root:Command -> modify multiple entities -> CommandEvent ->


	(Event,State,Actions) covers all inputs, maybe just Actions covers all?


	or Command -> Event -> State (for event sourcing)

	CommandSpec
	CommandPerformed {
		CommandContext
		[]EntityEvents
	}

======================================
All messages that pass an async adapter are marked in their metadata as "async-source".
The data persisted and sent is then also marked as "async-source".
If data passed to or read by a sync adapter is "async-source" - a warning will be raised that can be mitigated by defining an Await method.

	An await method determines if the data is up to date or not - it returns an error if it's not, allowing client to retry until successful.
	This is in order to avoid potential race conditions without being explicit about it.
	This situation can be avoided with static analysis/linting
*/
