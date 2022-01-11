package bgwrap

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
)

type BadgerMgr struct {
	db *badger.DB
}

// func GetBargerMgr() *BadgerMgr {
// 	bgMgr.once.Do(initDB)
// 	return &bgMgr
// }

// func initDB() {
// 	var err error
// 	opts := badger.DefaultOptions
// 	opts.Dir =
// 	if bgMgr.db, err = badger.Open(badger.DefaultOptions("/tmp/badger")); err != nil {
// 		panic(err)
// 	}
// }

func (b *BadgerMgr) Init(cfg *config.Config) error {
	var (
		err error
		opt badger.Options
	)
	opt = badger.DefaultOptions
	opt.Dir = cfg.DBPath
	opt.ValueDir = cfg.DBPath

	if b.db, err = badger.Open(opt); err != nil {
		return err
	}
	return nil
}

func (b *BadgerMgr) Get(k []byte) ([]byte, error) {
	var val []byte
	err := b.db.View(func(txn *badger.Txn) error {
		if item, err := txn.Get(k); err != nil {
			return err
		} else {
			if val,err = item.ValueCopy(val); err != nil{
				return err
			}
			return nil
		}
	})
	if err != nil {
		return nil, err
	}
	return val, err
}

func (b *BadgerMgr) GetStr(k string) (string, error) {
	if v, err := b.Get([]byte(k)); err != nil {
		return "", err
	} else {
		return string(v), nil
	}
}

func (b *BadgerMgr) Set(k []byte, v []byte) error {
	err := b.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(k, v); err != nil {
			return err
		} else {
			return nil
		}

	})
	return err
}

func (b *BadgerMgr) SetStr(k string, v string) error {
	if err := b.Set([]byte(k), []byte(v)); err != nil {
		return err
	}
	return nil
}

func (b *BadgerMgr) Del(k []byte) error {
	err := b.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(k); err != nil {
			return err
		} else {
			return nil
		}
	})
	return err
}

func (b *BadgerMgr) DelStr(k string) error {
	return b.Del([]byte(k))
}

func (b *BadgerMgr) GetDB() *badger.DB {
	return b.db
}

func (b *BadgerMgr) Close() error {
	return b.db.Close()
}
