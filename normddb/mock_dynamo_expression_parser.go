package normddb

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
grammar for the ConditionBuilder on their documentation, which allows us to autogenerate a parser
given that we have the correct grammar defined for it. We still have to write the logic to evaluate
each individual operation defined in the AST though, but the parser does the heavy lifting of the evaluation.
To be frank, it also seems like the more fun approach :)

https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html#Expressions.OperatorsAndFunctions.Syntax

Any open source library can be used, for example
* gocc (https://github.com/goccmack/gocc?tab=readme-ov-file)
* antlr (https://github.com/antlr/antlr4/blob/master/doc/go-target.md)

The Query API uses filter expressions, which also uses the same syntax as the condition expressions.
It also has "projection expressions" which we will simply wrap & mock since they are not as complex as condition expressions.
*/
