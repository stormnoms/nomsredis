// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"io/ioutil"
	//"os"
	"testing"

	"github.com/attic-labs/testify/suite"
)

func TestBoltDBStoreBasicTestSuite(t *testing.T) {
	suite.Run(t, &BoltDBStoreBasicTestSuite{})
}

type BoltDBStoreBasicTestSuite struct {
	BoltStoreTestSuite
	factory Factory
	dir     string
}

func (suite *BoltDBStoreBasicTestSuite) SetupTest() {
	var err error
	//suite.dir, err = ioutil.TempDir(os.TempDir(), "")
	suite.dir, err = ioutil.TempDir("/tmp67", "")
	suite.NoError(err)
	suite.factory = NewBoltDBStoreFactory(suite.dir, false)
	store := suite.factory.CreateStore("name").(*BoltDBStore)
	suite.putCountFn = func() int {
		return int(store.putCount)
	}

	suite.Store = store
}

func (suite *BoltDBStoreBasicTestSuite) TearDownTest() {
	suite.Store.Close()
	suite.factory.Shutter()
	//os.Remove(suite.dir)
}
