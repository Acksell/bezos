package bezos

// type CommandEvent struct {
// 	Meta     EventMeta
// 	Entities []EntityID
// }

type EventMeta struct {
	// Type is the name of the event.
	Type string
	// CausationID should provide an ID that can be used to trace
	// back to the command that caused the event to be dispatched.
	CausationID string
	// CorrelationID should provide an ID that can be used to group together
	// all the events that are part of the same logical operation.
	CorrelationID string
}

type Event interface {
	// the UUID of the command causing an event, or the event causing a command dispatch.
	GetMeta() EventMeta
}
