package readerat

import (
	"container/ring"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
)

const bufferChunkSize = 1024

type ReaderAt struct {
	position int64
	Reader   io.Reader
	buffer   *ring.Ring
}

type NoBufferError struct {
}

type ringItem struct {
	Pos  int64
	Byte byte
}

func New(r io.Reader, bufferSize int) *ReaderAt {
	return &ReaderAt{
		Reader: r,
		buffer: ring.New(bufferSize),
	}
}

func (r *ReaderAt) Read(b []byte) (int, error) {
	n, err := r.Reader.Read(b)
	if err != nil {
		return 0, err
	}
	if n > 0 {
		if r.buffer == nil {
			r.position += int64(n)
		} else {
			for i := 0; i < n; i++ {
				r.buffer.Value = ringItem{
					Pos:  r.position,
					Byte: b[i],
				}
				r.buffer = r.buffer.Next()
				r.position++
			}
		}
	}
	return n, nil
}

// [1,2,3,4,5,6,7,8,9]
//              ^
//      3,4,5,6
func (r *ReaderAt) ReadAt(b []byte, off int64) (int, error) {
	if off < r.position {
		// go back in time
		// see if we have this offset
		if r.buffer == nil {
			return 0, errors.New("cannot seek back no buffer present")
		}
		return r.readFromBuffer(b, off)
	}
	if off == r.position {
		// read at current position
		return r.Read(b)
	}

	// in the future
	if _, err := io.CopyN(ioutil.Discard, r, off-r.position); err != nil {
		return 0, err
	}

	return r.Read(b)
}

func loopForward(r *ring.Ring, f func(*ring.Ring, interface{}) bool) {
	if r != nil {
		if !f(r, r.Value) {
			return
		}
		startItem := r
		item := r.Next()
		for {
			if item == startItem {
				break
			}
			if !f(item, item.Value) {
				return
			}

			item = item.Next()
		}
	}
}

func loopBackward(r *ring.Ring, f func(*ring.Ring, interface{}) bool) {
	if r != nil {
		if !f(r, r.Value) {
			return
		}
		startItem := r
		item := r.Prev()
		for {
			if item == startItem {
				break
			}
			if !f(item, item.Value) {
				return
			}

			item = item.Prev()
		}
	}
}

func readFromRing(r *ring.Ring, b []byte, off int64) (int, error) {
	i := 0
	size := len(b)
	loopForward(r, func(_ *ring.Ring, v interface{}) bool {
		if item, ok := v.(ringItem); ok {
			if item.Pos >= off && i < size {
				b[i] = item.Byte
				i++
				return true
			}
		}
		return false
	})
	return i, nil
}

func (r *ReaderAt) readFromBuffer(b []byte, off int64) (int, error) {
	item, ok := r.buffer.Value.(ringItem)
	if !ok {
		item, ok = r.buffer.Prev().Value.(ringItem)
		if !ok {
			return 0, errors.New("invalid ring buffer item")
		}
	}
	// we are already at the right position
	if off == item.Pos {
		return readFromRing(r.buffer, b, off)
	}

	if off < item.Pos {
		var ringPos *ring.Ring
		loopBackward(r.buffer, func(rng *ring.Ring, v interface{}) bool {
			if item, ok := v.(ringItem); ok {
				// we found the correct position
				if item.Pos == off {
					ringPos = rng
					return false
				}
				return true
			}
			return true
		})
		if ringPos == nil {
			return 0, fmt.Errorf("buffer to small, unable to read from offset %d", off)
		}
		return readFromRing(ringPos, b, off)
	}

	// go forwards
	var ringPos *ring.Ring
	loopForward(r.buffer, func(rng *ring.Ring, v interface{}) bool {
		if item, ok := v.(ringItem); ok {
			// we found the correct position
			if item.Pos == off {
				ringPos = rng
				return false
			}
			return true
		}
		return true
	})
	if ringPos == nil {
		return 0, fmt.Errorf("buffer to small, unable to read from offset %d", off)
	}
	return readFromRing(ringPos, b, off)
}

func (r *ReaderAt) Position() int64 {
	return r.position
}
