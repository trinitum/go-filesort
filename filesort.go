// Package filesort provides methods for sorting records that can store the
// data being sorted to disk if the volume is too big.
package filesort

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"sync/atomic"
)

// Less is a function that compares two values. Should return true if a should
// come in the output before b
type Less func(a, b interface{}) bool

// Encoder is an interface that can encode records and write them out
type Encoder interface {
	// Encode encodes the argument and writes it out
	Encode(v interface{}) error
	// Close flushes buffers and closes the output handler
	Close() error
}

// EncoderConstructor is a function that creates Encoder that outputs encoded
// data to specified io.WriteCloser
type EncoderConstructor func(w io.WriteCloser) Encoder

// Decoder is an interface that can decode records.
type Decoder interface {
	// Decode record
	Decode() (interface{}, error)
}

// DecoderConstructor is a function that creates Decoder that reads from the
// specified io.Reader and decodes records
type DecoderConstructor func(r io.Reader) Decoder

// FileSort represents a single sort pipe to which you first write all the
// records, and then reading them sorted.
type FileSort struct {
	in         chan interface{}
	out        chan interface{}
	less       Less
	buffer     []interface{}
	bufferLen  int
	bufferMax  int
	files      []string
	newEncoder EncoderConstructor
	newDecoder DecoderConstructor
	err        atomic.Value
}

// Option represents various options for FileSort
type Option func(ps *FileSort)

// WithLess specifies comparison function
func WithLess(less Less) Option {
	return func(ps *FileSort) {
		ps.less = less
	}
}

// WithEncoderNew specifies the funcion to create the Encoder
func WithEncoderNew(ec EncoderConstructor) Option {
	return func(ps *FileSort) {
		ps.newEncoder = ec
	}
}

// WithDecoderNew specifies the funciton to create the Decoder
func WithDecoderNew(dc DecoderConstructor) Option {
	return func(ps *FileSort) {
		ps.newDecoder = dc
	}
}

// WithMaxMemoryBuffer specifies the maximum number of records that can be held
// in memory. When this limit has been reached the records are sorted and
// flushed to temporary file on disk.
func WithMaxMemoryBuffer(size int) Option {
	return func(ps *FileSort) {
		ps.bufferMax = size
	}
}

// New creates a new FileSort object based on specified options
func New(opts ...Option) (*FileSort, error) {
	ps := &FileSort{
		in:        make(chan interface{}, 4096),
		out:       make(chan interface{}, 4096),
		bufferMax: 1048576,
	}
	for _, o := range opts {
		o(ps)
	}
	if ps.less == nil || ps.newDecoder == nil || ps.newEncoder == nil {
		return nil, fmt.Errorf("less, decoder and encoder constructors are required")
	}
	go ps.sort()
	return ps, nil
}

func (ps *FileSort) sort() {
	tempDir, err := ioutil.TempDir("", "filesort")
	if err != nil {
		ps.err.Store(fmt.Errorf("couldn't create temporary directory: %v", err))
	}
	for v := range ps.in {
		// if there was en error just drain the channel
		if err != nil {
			continue
		}
		ps.buffer = append(ps.buffer, v)
		ps.bufferLen++
		if ps.bufferLen >= ps.bufferMax {
			sort.SliceStable(ps.buffer, func(i, j int) bool { return ps.less(ps.buffer[i], ps.buffer[j]) })
			err = ps.flushBuffer(tempDir)
			if err != nil {
				ps.err.Store(err)
			}
		}
	}
	if err != nil {
		close(ps.out)
		return
	}
	sort.SliceStable(ps.buffer, func(i, j int) bool { return ps.less(ps.buffer[i], ps.buffer[j]) })
	if err := ps.merge(); err != nil {
		ps.err.Store(err)
	}
}

func (ps *FileSort) flushBuffer(tempDir string) error {
	file, err := ioutil.TempFile(tempDir, "i")
	ps.files = append(ps.files, file.Name())
	if err != nil {
		return fmt.Errorf("couldn't create a temporary file: %v", err)
	}
	enc := ps.newEncoder(file)
	for _, v := range ps.buffer {
		if err := enc.Encode(v); err != nil {
			return fmt.Errorf("couldn't encode a value: %v", err)
		}
	}
	ps.buffer = nil
	ps.bufferLen = 0
	if err := enc.Close(); err != nil {
		return fmt.Errorf("error when closing encoder: %v", err)
	}
	return nil
}

type reader interface {
	Next() (interface{}, error)
}

type sliceReader struct {
	n     int
	slice []interface{}
}

func (sr *sliceReader) Next() (interface{}, error) {
	if sr.n == len(sr.slice) {
		sr.slice = nil
		sr.n = 0
		return nil, nil
	}
	sr.n++
	return sr.slice[sr.n-1], nil
}

type fileReader struct {
	file *os.File
	dec  Decoder
}

func (ps *FileSort) makeFileReader(name string) (*fileReader, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	dec := ps.newDecoder(file)
	return &fileReader{
		file: file,
		dec:  dec,
	}, nil
}

func (fr *fileReader) Next() (interface{}, error) {
	if fr.file == nil {
		return nil, nil
	}
	res, err := fr.dec.Decode()
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("error while decoding a record: %v", err)
	}
	if res == nil {
		fr.file.Close()
		fr.file = nil
	}
	return res, nil
}

type mergeReader struct {
	next func() (interface{}, error)
}

func (mr *mergeReader) Next() (interface{}, error) {
	return mr.next()
}

func newMergeReader(less func(a, b interface{}) bool, rs []reader) (reader, error) {
	n := len(rs)
	if n == 1 {
		return rs[0], nil
	}
	var rs0, rs1 reader
	var err error
	if n == 2 {
		rs0 = rs[0]
		rs1 = rs[1]
	} else {
		n = n / 2
		if rs0, err = newMergeReader(less, rs[:n]); err != nil {
			return nil, err
		}
		if rs1, err = newMergeReader(less, rs[n:]); err != nil {
			return nil, err
		}
	}
	n0, err := rs0.Next()
	if err != nil {
		return nil, err
	}
	if n0 == nil {
		return rs1, nil
	}
	n1, err := rs1.Next()
	if err != nil {
		return nil, err
	}
	next := func() (interface{}, error) {
		var err error
		if n0 == nil {
			return nil, nil
		}
		if n1 == nil {
			res := n0
			if n0, err = rs0.Next(); err != nil {
				return nil, err
			}
			return res, nil
		}
		if less(n0, n1) {
			res := n0
			if n0, err = rs0.Next(); err != nil {
				return nil, err
			}
			if n0 == nil {
				n0 = n1
				n1 = nil
				rs0 = rs1
			}
			return res, nil
		}
		res := n1
		if n1, err = rs1.Next(); err != nil {
			return nil, err
		}
		return res, nil
	}
	return &mergeReader{next: next}, nil
}

func (ps *FileSort) merge() error {
	defer close(ps.out)
	var readers []reader
	if len(ps.buffer) > 0 {
		readers = append(readers, &sliceReader{slice: ps.buffer})
	}
	for _, file := range ps.files {
		fr, err := ps.makeFileReader(file)
		if err != nil {
			panic(err)
		}
		readers = append(readers, fr)
	}
	mr, err := newMergeReader(ps.less, readers)
	if err != nil {
		return err
	}
	for {
		next, err := mr.Next()
		if err != nil {
			return err
		}
		if next == nil {
			break
		}
		ps.out <- next
	}
	return nil
}

// Close closes input of the FileSort. After that you can start reading sorted
// records using the Read method.
func (ps *FileSort) Close() error {
	close(ps.in)
	return nil
}

// Sort writes a record for sorting to FileSort.
func (ps *FileSort) Sort(v interface{}) error {
	if err := ps.err.Load(); err != nil {
		return err.(error)
	}
	ps.in <- v
	return nil
}

// Read returns the next sorted record or nil in the end of the stream. Note,
// that if input hasn't been closed yet, the method will block till it will be
// closed.
func (ps *FileSort) Read() (interface{}, error) {
	val := <-ps.out
	if val == nil {
		if err := ps.err.Load(); err != nil {
			return nil, err.(error)
		}
	}
	return val, nil
}
