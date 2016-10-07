// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"github.com/attic-labs/testify/suite"
)

type BoltStoreBatchTestSuite struct {
	suite.Suite
	Store      ChunkStore
	putCountFn func() int
}

func (suite *BoltStoreBatchTestSuite) TestChunkStorePutMany() {
	input1, input2 := "abc", "def"
	c1, c2 := NewChunk([]byte(input1)), NewChunk([]byte(input2))
	suite.Store.PutMany([]Chunk{c1, c2})

	suite.Store.UpdateRoot(c1.Hash(), suite.Store.Root()) // Commit writes

	// And reading it via the API should work...
	assertInputInStore(input1, c1.Hash(), suite.Store, suite.Assert())
	assertInputInStore(input2, c2.Hash(), suite.Store, suite.Assert())
	if suite.putCountFn != nil {
		suite.Equal(2, suite.putCountFn())
	}
}
