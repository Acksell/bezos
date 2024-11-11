package conditionast

// TODO: Implement the Visitor pattern?

// Node interface
type Node interface {
	Accept(visitor Visitor, input Input)
}

// Visitor interface
type Visitor interface {
	VisitCondition(*Condition)
	VisitExpression(*Expression)
	// VisitComparison(*Comparison)
	// VisitLogicalOp(*LogicalOp)
	// VisitAttributePath(*AttributePath)
	// VisitFunctionCall(*FunctionCall)
	// VisitBetweenExpr(*BetweenExpr)
	// VisitContainsExpr(*ContainsExpr)
	// VisitExpressionAttributeName(*ExpressionAttributeName)
	// VisitExpressionAttributeValue(*ExpressionAttributeValue)
}
