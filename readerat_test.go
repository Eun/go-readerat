package readerat

import (
	"bytes"
	"container/ring"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

type TestStream struct {
	Buffer *bytes.Buffer
}

func (ts *TestStream) Read(b []byte) (int, error) {
	return ts.Buffer.Read(b)
}

func TestLinearRead(t *testing.T) {
	ts := TestStream{
		Buffer: bytes.NewBufferString("Hello World"),
	}

	reader := ReaderAt{
		Reader: &ts,
	}
	var readBuffer [4]byte
	n, err := reader.ReadAt(readBuffer[:], 0)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, readBuffer[:n], []byte("Hell"))

	n, err = reader.ReadAt(readBuffer[:], 4)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, readBuffer[:n], []byte("o Wo"))

	n, err = reader.ReadAt(readBuffer[:], 8)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, readBuffer[:n], []byte("rld"))
}

func TestHistoryRead(t *testing.T) {
	t.Run("No Buffer", func(t *testing.T) {
		ts := TestStream{
			Buffer: bytes.NewBufferString("Hello World"),
		}

		reader := New(&ts, 0)
		// read 4 bytes from the front
		var readBuffer [4]byte
		n, err := reader.ReadAt(readBuffer[:], 0)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, readBuffer[:n], []byte("Hell"))

		// try to read 4 bytes from the front again
		_, err = reader.ReadAt(readBuffer[:], 0)
		require.Error(t, err)
		// require.EqualError(t, err, NoBufferError{})

		// continue the read
		n, err = reader.ReadAt(readBuffer[:], 4)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, readBuffer[:n], []byte("o Wo"))
	})

	t.Run("With Buffer", func(t *testing.T) {
		ts := TestStream{
			Buffer: bytes.NewBufferString("Hello World"),
		}

		reader := New(&ts, 16)
		// read 4 bytes from the front
		var readBuffer [4]byte
		n, err := reader.ReadAt(readBuffer[:], 0)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, readBuffer[:n], []byte("Hell"))

		// try to read 4 bytes from the front again
		n, err = reader.ReadAt(readBuffer[:], 0)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, readBuffer[:n], []byte("Hell"))

		// continue the read
		n, err = reader.ReadAt(readBuffer[:], 4)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, readBuffer[:n], []byte("o Wo"))
	})

	t.Run("Fill Buffer", func(t *testing.T) {
		ts := TestStream{
			Buffer: bytes.NewBufferString("Hello World"),
		}

		reader := New(&ts, 8)
		// read 4 bytes from the front
		var readBuffer [4]byte
		n, err := reader.ReadAt(readBuffer[:], 0)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, readBuffer[:n], []byte("Hell"))

		// try to read 4 bytes from the front again
		n, err = reader.ReadAt(readBuffer[:], 0)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, readBuffer[:n], []byte("Hell"))

		// continue the read
		buf, err := ioutil.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, buf, []byte("o World"))

		// try to read 4 bytes from the front again
		_, err = reader.ReadAt(readBuffer[:], 0)
		require.EqualError(t, err, "buffer to small, unable to read from offset 0")

		// try to read 4 bytes from the front again
		n, err = reader.ReadAt(readBuffer[:], 4)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, readBuffer[:n], []byte("o Wo"))
	})
}

func TestFutureRead(t *testing.T) {
	t.Run("No Buffer", func(t *testing.T) {
		ts := TestStream{
			Buffer: bytes.NewBufferString("Hello World"),
		}

		reader := New(&ts, 0)
		var readBuffer [4]byte
		n, err := reader.ReadAt(readBuffer[:], 6)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, readBuffer[:n], []byte("Worl"))

		_, err = reader.ReadAt(readBuffer[:], 0)
		require.Error(t, err)
		// require.EqualError(t, err, NoBufferError{})
	})

	t.Run("With Buffer", func(t *testing.T) {
		ts := TestStream{
			Buffer: bytes.NewBufferString("Hello World"),
		}

		reader := New(&ts, 16)
		var readBuffer [4]byte
		n, err := reader.ReadAt(readBuffer[:], 6)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, readBuffer[:n], []byte("Worl"))

		n, err = reader.ReadAt(readBuffer[:], 0)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, readBuffer[:n], []byte("Hell"))
	})
}

func TestLoopForward(t *testing.T) {
	t.Run("", func(t *testing.T) {
		r := ring.New(10)
		size := r.Len()
		for i := 0; i < size; i++ {
			r.Value = i
			r = r.Next()
		}

		var actual []int
		loopForward(r, func(_ *ring.Ring, v interface{}) bool {
			actual = append(actual, v.(int))
			return true
		})
		require.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, actual)
	})

	t.Run("", func(t *testing.T) {
		r := ring.New(10)
		size := r.Len()
		for i := 0; i < size; i++ {
			r.Value = i
			r = r.Next()
		}

		// move the ring a bit forward
		for i := 0; i < 5; i++ {
			r = r.Next()
		}

		var actual []int
		loopForward(r, func(_ *ring.Ring, v interface{}) bool {
			actual = append(actual, v.(int))
			return true
		})
		require.Equal(t, []int{5, 6, 7, 8, 9, 0, 1, 2, 3, 4}, actual)
	})

	t.Run("", func(t *testing.T) {
		r := ring.New(10)
		size := r.Len()
		for i := 0; i < size; i++ {
			r.Value = i
			r = r.Next()
		}

		// move the ring to the end
		for i := 0; i < 9; i++ {
			r = r.Next()
		}

		var actual []int
		loopForward(r, func(_ *ring.Ring, v interface{}) bool {
			actual = append(actual, v.(int))
			return true
		})
		require.Equal(t, []int{9, 0, 1, 2, 3, 4, 5, 6, 7, 8}, actual)
	})

	t.Run("break", func(t *testing.T) {
		r := ring.New(10)
		size := r.Len()
		for i := 0; i < size; i++ {
			r.Value = i
			r = r.Next()
		}
		j := 0
		loopForward(r, func(_ *ring.Ring, v interface{}) bool {
			if j == 5 {
				return false
			}
			j++
			return true
		})

		var actual []int
		loopForward(r, func(_ *ring.Ring, v interface{}) bool {
			actual = append(actual, v.(int))
			return true
		})
		require.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, actual)
	})
}

func TestLoopBackward(t *testing.T) {
	t.Run("", func(t *testing.T) {
		r := ring.New(10)
		size := r.Len()
		for i := 0; i < size; i++ {
			r.Value = i
			r = r.Next()
		}

		var actual []int
		loopBackward(r, func(_ *ring.Ring, v interface{}) bool {
			actual = append(actual, v.(int))
			return true
		})
		require.Equal(t, []int{0, 9, 8, 7, 6, 5, 4, 3, 2, 1}, actual)
	})

	t.Run("", func(t *testing.T) {
		r := ring.New(10)
		size := r.Len()
		for i := 0; i < size; i++ {
			r.Value = i
			r = r.Next()
		}

		// move the ring a bit forward
		for i := 0; i < 10; i++ {
			r = r.Next()
		}

		var actual []int
		loopBackward(r, func(_ *ring.Ring, v interface{}) bool {
			actual = append(actual, v.(int))
			return true
		})
		require.Equal(t, []int{0, 9, 8, 7, 6, 5, 4, 3, 2, 1}, actual)
	})

	t.Run("", func(t *testing.T) {
		r := ring.New(10)
		size := r.Len()
		for i := 0; i < size; i++ {
			r.Value = i
			r = r.Next()
		}

		// move the ring to the end
		for i := 0; i < 9; i++ {
			r = r.Next()
		}

		var actual []int
		loopBackward(r, func(_ *ring.Ring, v interface{}) bool {
			actual = append(actual, v.(int))
			return true
		})
		require.Equal(t, []int{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}, actual)
	})

	t.Run("break", func(t *testing.T) {
		r := ring.New(10)
		size := r.Len()
		for i := 0; i < size; i++ {
			r.Value = i
			r = r.Next()
		}
		j := 0
		loopBackward(r, func(_ *ring.Ring, v interface{}) bool {
			if j == 5 {
				return false
			}
			j++
			return true
		})

		var actual []int
		loopBackward(r, func(_ *ring.Ring, v interface{}) bool {
			actual = append(actual, v.(int))
			return true
		})
		require.Equal(t, []int{0, 9, 8, 7, 6, 5, 4, 3, 2, 1}, actual)
	})
}
