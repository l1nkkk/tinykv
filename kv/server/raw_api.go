package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	var (
		stRead storage.StorageReader
		rtn    kvrpcpb.RawGetResponse
		v      []byte
		err    error
	)
	if stRead, err = server.storage.Reader(nil); err != nil {
		return nil, err
	}
	defer stRead.Close()

	if v, err = stRead.GetCF(req.Cf, req.Key); err != nil {
		return &rtn, err
	}
	if v == nil{
		rtn.NotFound = true
	}
	rtn.Value = v
	return &rtn, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	var (
		rtn kvrpcpb.RawPutResponse
	)

	var mdf = storage.Modify{
		Data: storage.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		},
	}

	server.storage.Write(nil, []storage.Modify{mdf})
	return &rtn, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	var (
		rtn kvrpcpb.RawDeleteResponse
	)
	var mdf = storage.Modify{
		Data: storage.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		},
	}

	server.storage.Write(nil, []storage.Modify{mdf})
	return &rtn, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	var (
		err    error
		stRead storage.StorageReader
		iter   engine_util.DBIterator
		rtn    kvrpcpb.RawScanResponse
		times  uint32
		k      []byte
		v      []byte
	)
	if stRead, err = server.storage.Reader(nil); err != nil {
		return nil, err
	}
	defer stRead.Close()

	iter = stRead.IterCF(req.Cf)

	times = req.Limit
	for iter.Seek(req.StartKey); iter.Valid() && times != 0; iter.Next() {
		// TODO: 效率待优化
		k = iter.Item().KeyCopy(k)
		if v, err = iter.Item().ValueCopy(v); err != nil {
			return nil, err
		}

		rtn.Kvs = append(rtn.Kvs, &kvrpcpb.KvPair{
			Key:   k,
			Value: v,
		})
		// 重要，解除依赖，不然返回的结果都是最后一个
		k = nil
		v = nil
		times--
	}

	return &rtn, nil
}
