package badger

import (
	"bytes"
	"fmt"
	"sync"

	"golang.org/x/net/trace"

	"github.com/bkaradzic/go-lz4"
	"github.com/dgraph-io/badger/y"
	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

var memcachelog MemcacheLog

type MemcacheLog struct {
	mc          []*memcached.Client
	curKey      uint32
	curKeyMutex sync.Mutex
	opt         Options
	kv          *KV
	elog        trace.EventLog
	encoder     *entryEncoder
	writeCh     chan []*Entry
}

func (l *MemcacheLog) Open(kv *KV, opt *Options) error {
	l.mc = make([]*memcached.Client, opt.MemcacheConnections)
	for i := 0; i < opt.MemcacheConnections; i++ {
		l.mc[i], _ = memcached.Connect("tcp", opt.MemcacheServer)
	}
	l.curKey = 0
	l.opt = *opt
	l.kv = kv

	l.elog = trace.NewEventLog("Badger", "MemcacheLog")

	l.encoder = &entryEncoder{
		opt:          l.opt,
		decompressed: bytes.NewBuffer(make([]byte, 1<<20)),
		compressed:   make([]byte, 1<<20),
	}

	l.writeCh = make(chan []*Entry, 1000)

	for idx := 0; idx < 10; idx++ {
		go l.writeLoop(idx)
	}

	return nil
}

func (l *MemcacheLog) Close() error {
	l.elog.Printf("Stopping garbage collection of values.")
	defer l.elog.Finish()
	return nil
}

func (l *MemcacheLog) Read(p valuePointer, s *y.Slice, hint int) (e Entry, err error) {
	if s == nil {
		s = new(y.Slice)
	}
	resp, err := l.mc[hint].Get(0, fmt.Sprint(p.Key))
	if err != nil {
		return e, err
	}
	buf := s.Resize(int(p.Len))

	var h header
	buf, _ = h.Decode(resp.Body)

	if h.meta&BitCompressed > 0 {
		// TODO: reuse generated buffer
		y.AssertTrue(uint32(len(buf)) == h.vlen)
		decoded, err := lz4.Decode(nil, buf)
		y.Check(err)

		y.AssertTrue(len(decoded) > int(h.klen))
		h.vlen = uint32(len(decoded)) - h.klen
		buf = decoded
	}

	e.Key = buf[0:h.klen]
	e.Meta = h.meta
	e.casCounter = h.casCounter
	e.CASCounterCheck = h.casCounterCheck
	e.Value = buf[h.klen : h.klen+h.vlen]

	return e, nil
}

func (l *MemcacheLog) write(reqs []*request) error {
	for i := range reqs {
		b := reqs[i]
		b.Ptrs = b.Ptrs[:0]
		for j := range b.Entries {
			e := b.Entries[j]
			y.AssertTruef(e.Meta&BitCompressed == 0, "Cannot set BitCompressed outside value log")

			var p valuePointer
			if !l.opt.SyncWrites && len(e.Value) < l.opt.ValueThreshold {
				// No need to write to value log.
				b.Ptrs = append(b.Ptrs, p)
				continue
			}

			var lbuf bytes.Buffer
			plen, err := l.encoder.Encode(e, &lbuf) // Now encode the entry into buffer.
			if err != nil {
				return err
			}
			p.Len = uint32(plen)
			l.curKeyMutex.Lock()
			p.Key = l.curKey
			e.mcKey = l.curKey
			l.curKey++
			l.curKeyMutex.Unlock()
			b.Ptrs = append(b.Ptrs, p)
			l.writeCh <- b.Entries
		}
	}

	return nil
}

func (l *MemcacheLog) writeLoop(idx int) {
	for {
		select {
		case reqs := <-l.writeCh:
			l.doWrite(reqs, idx)
		}
	}
}

// write is thread-unsafe by design and should not be called concurrently.
func (l *MemcacheLog) doWrite(reqs []*Entry, idx int) error {
	for i := range reqs {
		e := reqs[i]
		if !l.opt.SyncWrites && len(e.Value) < l.opt.ValueThreshold {
			// No need to write to value log.
			continue
		}

		var lbuf bytes.Buffer
		_, err := l.encoder.Encode(e, &lbuf) // Now encode the entry into buffer.
		if err != nil {
			return err
		}

		key := e.mcKey
		req := &gomemcached.MCRequest{
			Opcode:  gomemcached.SETQ,
			VBucket: 0,
			Key:     []byte(fmt.Sprint(key)),
			Cas:     0,
			Opaque:  0,
			Extras:  []byte{0, 0, 0, 0, 0, 0, 0, 0},
			Body:    lbuf.Bytes()}

		l.mc[idx].Transmit(req)
		if err != nil {
			fmt.Println(err)
		}
	}

	return nil
}

func (l *MemcacheLog) sync() error {
	return nil
}

func (l *MemcacheLog) Replay(ptr valuePointer, fn logEntry) error {
	return nil
}

func (l *MemcacheLog) runGCInLoop(lc *y.LevelCloser) {
	defer lc.Done()
	if l.opt.ValueGCThreshold == 0.0 {
		return
	}

	for {
		select {
		case <-lc.HasBeenClosed():
			return
		}
	}
}
