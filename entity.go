package norm

import (
	"time"
)

// TODO avoid confusion between data-entity and domain entity
type Entity interface {
	GetID() string // nri?
	GetMeta() EntityMeta
}

// type EntityID string

type EntityMeta struct {
	// The name of the entity we're storing
	Type     string
	Observed time.Time
	Updated  time.Time
	Created  time.Time
}
