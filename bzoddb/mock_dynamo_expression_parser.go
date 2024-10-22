package bzoddb

/*
The approach of this mock code is to avoid having to mock DDB internals as much as possible.
This is mostly possible because the package itself wraps/hides a lot of the DDB internals.
However, in some places it exposes the raw DDB API, namely the condition expression API
is exposed in the Put and UnsafeUpdate actions.

The reason we expose the raw DDB API was done to avoid providing a thin wrapper on top of
the ddb-sdk with very little value provided. The only value it would provide is the ability
to mock it, but we would still have to write a parser in order to evaluate the condition.

So, in order to correctly apply the condition expressions in our mock store, we either have to:
* Wrap the DDB condition expression API to make it mockable/allow introspection, and then write a parser.
* Or just parse the resulting expression.

The second alternative seems easier, especially since AWS documentation provides a rough
syntax for the ConditionBuilder on their documentation, which allows us to autogenerate a parser
given that we have the correct grammar defined for it. We still have to write the logic to evaluate
each individual operation defined though, but the parser does the heavy lifting of recursion and lexical lookups.
To be frank, it also seems like the more fun approach :)

https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html#Expressions.OperatorsAndFunctions.Syntax

Any open source library for generating parsers for different grammars can be used, for example
* BNF: gocc (https://github.com/goccmack/gocc?tab=readme-ov-file)
* BNF: antlr (https://github.com/antlr/antlr4/blob/master/doc/go-target.md)
* PEG: pigeon https://pkg.go.dev/github.com/mna/pigeon
* PEG: peg (https://github.com/pointlander/peg)

After some googling there exists an open source library called 'dynalite' which has already done this,
but in Javascript (not typescript ;-;). It used the library PEG.js to generate its parser, which means we could use
port dynalite's .pegjs files to and easily copy the PEG to any Go-PEG generator library.
The pigeon library is heavily inspired by PEG.js so it should be even easier to port to Go.

Pigeon allows you to do a lot, thus it's important to keep your project structured. We will not bloat
the peg-file with in-line logic, we should instead create a separate package for the AST. The peg-file
should only be responsible for defining the grammar and creating the AST. We will also create utility
functions to easily convert byte arrays into to an AST node. A good example of the target project structure
 can be found in the BNF alternative gocc: https://github.com/goccmack/gocc/blob/master/example/bools/example.bnf.
However, when evaluating BNF vs PEG I found that PEG is easier for our use-case, since we want to
use negative lookahead to exclude reserved words, among other features that are not available in BNF.

The dynalite .pegjs files are defined here: https://github.com/architect/dynalite/tree/main/db.

We don't want to use dynalite directly because it's written in Javascript, and we want the ability
to write and run tests in Go.

There's also the 'dynamodb-local' project, which is closed source, written in Java, requires AWS credentials,
and requires you to start a docker container. Suffice to say it's too heavyweight

Our mock library also doesn't aim to persist data to disk, but this can be added later if desired. However,
I would try to keep the side effects minimal. Note: In order to make integration tests behave the same as
unit tests, we have to clean up the data after each test.
*/
