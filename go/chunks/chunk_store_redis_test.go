// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"github.com/attic-labs/testify/suite"

	//"github.com/stormasm/nomsredis/go/constants"
	"github.com/stormasm/nomsredis/go/hash"
)

type ChunkStoreRedisTestSuite struct {
	suite.Suite
	Store      ChunkStore
	putCountFn func() int
}

func (suite *ChunkStoreRedisTestSuite) TestChunkStorePut() {
	input := "abc"
	c := NewChunk([]byte(input))
	suite.Store.Put(c)
	h := c.Hash()

	// See http://www.di-mgt.com.au/sha_testvectors.html
	suite.Equal("rmnjb8cjc5tblj21ed4qs821649eduie", h.String())

	suite.Store.UpdateRoot(h, suite.Store.Root()) // Commit writes

	// And reading it via the API should work...
	assertInputInStore(input, h, suite.Store, suite.Assert())
	if suite.putCountFn != nil {
		suite.Equal(1, suite.putCountFn())
	}

	// Re-writing the same data should cause a second put
	c = NewChunk([]byte(input))
	suite.Store.Put(c)
	suite.Equal(h, c.Hash())
	assertInputInStore(input, h, suite.Store, suite.Assert())
	suite.Store.UpdateRoot(h, suite.Store.Root()) // Commit writes

	if suite.putCountFn != nil {
		suite.Equal(2, suite.putCountFn())
	}
}

func (suite *ChunkStoreRedisTestSuite) TestChunkStoreRoot() {
	oldRoot := suite.Store.Root()
	//suite.True(oldRoot.IsEmpty())

	bogusRoot := hash.Parse("8habda5skfek1265pc5d5l1orptn5dr0")
	newRoot := hash.Parse("8la6qjbh81v85r6q67lqbfrkmpds14lg")

	// Try to update root with bogus oldRoot
	result := suite.Store.UpdateRoot(newRoot, bogusRoot)
	suite.False(result)

	// Now do a valid root update
	result = suite.Store.UpdateRoot(newRoot, oldRoot)
	suite.True(result)
}

func (suite *ChunkStoreRedisTestSuite) TestChunkStorePutMany() {
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
