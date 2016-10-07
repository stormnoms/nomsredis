// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/attic-labs/testify/suite"
)

func TestRedisStoreTestSuite(t *testing.T) {
	suite.Run(t, &RedisStoreTestSuite{})
}

type RedisStoreTestSuite struct {
	ChunkStoreRedisTestSuite
	factory Factory
	dir     string
}

func (suite *RedisStoreTestSuite) SetupTest() {
	var err error
	suite.dir, err = ioutil.TempDir(os.TempDir(), "")
	suite.NoError(err)
	suite.factory = NewRedisStoreFactory(suite.dir, 24, false)
	store := suite.factory.CreateStore("name").(*RedisStore)
	suite.putCountFn = func() int {
		return int(store.putCount)
	}

	suite.Store = store
}

func (suite *RedisStoreTestSuite) TearDownTest() {
	suite.Store.Close()
	suite.factory.Shutter()
	os.Remove(suite.dir)
}
