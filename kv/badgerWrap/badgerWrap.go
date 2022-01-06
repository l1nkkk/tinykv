package badgerwrap

import (
	"sync"

	badger "github.com/dgraph-io/badger/v3"
)

var badgerMgr *badger.DB
var once sync.Once

func GetBargerMgr() badger.DB {
	once.Do(InitDB)
}

func InitDB() {
	var err error
	if badgerMgr, err = badger.Open(badger.DefaultOptions("/tmp/badger")); err != nil {
		panic(err)
	}
}
