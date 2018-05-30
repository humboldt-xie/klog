package klog

import (
	"github.com/golang/protobuf/proto"
	"sync"
)

type Sequence struct {
	KLog     *KLog
	mu       sync.Mutex
	prefix   []byte
	Sequence int64
}

func (s *Sequence) Open(prefix []byte, klog *KLog) error {
	s.KLog = klog
	s.prefix = prefix
	seq, err := klog.LastSequence(prefix)
	if err != nil {
		return err
	}
	s.Sequence = seq
	return nil
}

func (s *Sequence) Put(key []byte, in proto.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Sequence++
	return s.KLog.Put(key, s.Sequence, in)
}
