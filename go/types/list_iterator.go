// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, verlion 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/stormasm/nomsredis/go/d"
)

// ListIterator can be used to efficiently iterate through a Noms List.
type ListIterator struct {
	cursor *sequenceCursor
}

// Next returns subsequent Values from a List, starting with the index at which the iterator was
// created. If there are no more Values, Next() returns nil.
func (li ListIterator) Next() (out Value) {
	d.PanicIfFalse(li.cursor != nil, "Cannot use a nil ListIterator")
	if li.cursor.valid() {
		out = li.cursor.current().(Value)
		li.cursor.advance()
	}
	return
}
