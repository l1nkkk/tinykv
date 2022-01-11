package standalone_storage

import (
	"log"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/bgwrap"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	dbMgr *bgwrap.BadgerMgr
	conf  *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	var rtn StandAloneStorage
	rtn.conf = conf
	rtn.dbMgr = &bgwrap.BadgerMgr{}
	return &rtn
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	// 打开badgerDB
	s.dbMgr.Init(s.conf)

	log.Println("badgerDB connect succeed")
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	// 关闭badgerDB
	if err := s.dbMgr.Close(); err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return CreateStandAloneReader(s), nil
}

// 批量写入
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, b := range batch {
		switch b.Data.(type) {
		case storage.Delete:
			delEvent := b.Data.(storage.Delete)
			s.dbMgr.Del(engine_util.KeyWithCF(delEvent.Cf, delEvent.Key))
		case storage.Put:
			putEvent := b.Data.(storage.Put)
			s.dbMgr.Set(engine_util.KeyWithCF(putEvent.Cf, putEvent.Key), putEvent.Value)
		default:
			log.Println("UNDEFINE")
		}
	}
	return nil
}

// write by l1nkkk
type StandAloneReader struct {
	sas  *StandAloneStorage
	txn  *badger.Txn // only use by iterator
	iter engine_util.DBIterator
}

func CreateStandAloneReader(s *StandAloneStorage) storage.StorageReader {
	return &StandAloneReader{
		sas: s,
	}
}

func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	if rtn, err := s.sas.dbMgr.Get(engine_util.KeyWithCF(cf, key)); err != nil{
		if err == badger.ErrKeyNotFound{
			return nil, nil
		}
		return nil, err
	}else{
		return rtn, nil
	}
}
func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	s.txn = s.sas.dbMgr.GetDB().NewTransaction(false)
	s.iter = engine_util.NewCFIterator(cf, s.txn)
	return s.iter
}
func (s *StandAloneReader) Close() {
	if s.iter != nil{
		s.iter.Close()
	}
	if s.txn != nil{
		s.txn.Discard()
	}
}
