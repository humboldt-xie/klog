package klog

import (
	"github.com/golang/protobuf/proto"
	ldb "github.com/syndtr/goleveldb/leveldb"
	util "github.com/syndtr/goleveldb/leveldb/util"

	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"reflect"
	"sync"
)

func Join(a []byte, b []byte) []byte {
	b_buf := bytes.NewBuffer([]byte{})
	b_buf.Write(a)
	b_buf.Write(b)
	return b_buf.Bytes()
}

type ILog interface {
	Get(key []byte, seq int64, out proto.Message) error
}

type KLog struct {
	DB  *ldb.DB
	seq map[string]*Sequence
	mu  sync.Mutex
}

func (s *KLog) OpenDB(db *ldb.DB) error {
	s.DB = db
	s.seq = make(map[string]*Sequence)
	return nil
}

func (s *KLog) Open(path string) error {
	db, err := ldb.OpenFile(path, nil)
	if err != nil {
		return err
	}
	return s.OpenDB(db)
}

func (s *KLog) Sequence(prefix []byte) *Sequence {
	s.mu.Lock()
	defer s.mu.Unlock()
	seq := s.seq[string(prefix)]
	if seq == nil {
		seq = &Sequence{}
		seq.Open(prefix, s)
		s.seq[string(prefix)] = seq
	}
	return seq
}

func (s *KLog) ToByte(chain_address []byte, height int64) []byte {
	b_buf := bytes.NewBuffer([]byte{})
	b_buf.Write(chain_address)
	binary.Write(b_buf, binary.BigEndian, height)
	return b_buf.Bytes()
}

func (s *KLog) ToSequence(key []byte, prefix []byte) (height int64) {
	b_buf := bytes.NewBuffer(key[len(prefix):])
	binary.Read(b_buf, binary.BigEndian, &height)
	return height
}

func (s *KLog) Put(address []byte, height int64, in proto.Message) error {
	key := s.ToByte(address, height)
	data, err := proto.Marshal(in)
	if err != nil {
		return err
	}

	err = s.DB.Put(key, data, nil)
	return err
}

func (s *KLog) GetRange(prefix []byte, start int64, end int64, intype interface{}) ([]proto.Message, error) {
	typeof := reflect.TypeOf(intype)
	if typeof.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("not suport")
	}
	ret := make([]proto.Message, end-start)

	//e := reflect.New(typeof.Elem())

	iter := s.DB.NewIterator(&util.Range{Start: s.ToByte(prefix, start), Limit: s.ToByte(prefix, end)}, nil)
	num := 0
	log.Printf("KLog GetRange %s %s", iter, prefix)
	for iter.Next() && num < len(ret) {
		log.Printf("KLog GetRange %x", iter.Key())
		if !bytes.Contains(iter.Key(), prefix) {
			log.Printf("KLog GetRange %x not contains %s", iter.Key(), prefix)
			break
		}
		retImpl := reflect.New(typeof.Elem())
		msg := retImpl.Interface().(proto.Message)
		if msg == nil {
			return ret[0:0], fmt.Errorf("intype no impl of proto.Message")
		}
		err := s.GetProto(iter.Key(), msg)
		if err != nil {
			return ret[0:num], err
		}
		log.Printf("type %T %#v", retImpl.Interface(), retImpl.Interface())
		ret[num] = retImpl.Interface().(proto.Message)
		num += 1
	}
	return ret[0:num], nil
}

func (s *KLog) LastSequence(address []byte) (int64, error) {
	start := s.ToByte(address, 0)
	end := s.ToByte(address, math.MaxInt64)
	iter := s.DB.NewIterator(&util.Range{Start: start, Limit: end}, nil)

	var height int64
	height = -1
	if ok := iter.Last(); ok {
		key := iter.Key()
		height = s.ToSequence(key, address)
	}
	return height, nil
}
func (s *KLog) Get(prefix []byte, seq int64, out proto.Message) error {
	key := s.ToByte(prefix, seq)
	return s.GetProto(key, out)
}

func (s *KLog) GetProto(key []byte, out proto.Message) error {
	bys, err := s.DB.Get(key, nil)
	if err != nil {
		return err
	}
	err = proto.Unmarshal(bys, out)
	if err != nil {
		return err
	}
	return nil
}

func (s *KLog) Remove(path string) {
	if len(path) <= 3 {
		panic("file name too short")
	}
	defer os.RemoveAll(path)
}
