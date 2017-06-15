package badger

import (
	"github.com/dgraph-io/badger/y"
)

type ValueLog interface {
	Open(kv *KV, opt *Options) error
	Close() error
	Read(p valuePointer, s *y.Slice, hint int) (e Entry, err error)
	write(reqs []*request) error
	sync() error
	Replay(ptr valuePointer, fn logEntry) error
	runGCInLoop(lc *y.LevelCloser)
}
