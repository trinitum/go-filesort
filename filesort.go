package filesort

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
)

type Less func(a, b interface{}) bool

type Encoder interface {
	Encode(v interface{}) error
	Close() error
}

type EncoderConstructor func(w io.WriteCloser) Encoder

type Decoder interface {
	Decode() (interface{}, error)
}

type DecoderConstructor func(r io.Reader) Decoder

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
}

type Option func(ps *FileSort)

func WithLess(less Less) Option {
	return func(ps *FileSort) {
		ps.less = less
	}
}

func WithEncoderNew(ec EncoderConstructor) Option {
	return func(ps *FileSort) {
		ps.newEncoder = ec
	}
}

func WithDecoderNew(dc DecoderConstructor) Option {
	return func(ps *FileSort) {
		ps.newDecoder = dc
	}
}

func WithMaxMemoryBuffer(size int) Option {
	return func(ps *FileSort) {
		ps.bufferMax = size
	}
}

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
		panic("couldn't create a temporary directory: " + err.Error())
	}
	for v := range ps.in {
		ps.buffer = append(ps.buffer, v)
		ps.bufferLen++
		if ps.bufferLen >= ps.bufferMax {
			sort.SliceStable(ps.buffer, func(i, j int) bool { return ps.less(ps.buffer[i], ps.buffer[j]) })
			ps.flushBuffer(tempDir)
		}
	}
	sort.SliceStable(ps.buffer, func(i, j int) bool { return ps.less(ps.buffer[i], ps.buffer[j]) })
	ps.merge()
}

func (ps *FileSort) flushBuffer(tempDir string) {
	file, err := ioutil.TempFile(tempDir, "i")
	ps.files = append(ps.files, file.Name())
	if err != nil {
		panic("couldn't create a temporary file: " + err.Error())
	}
	enc := ps.newEncoder(file)
	for _, v := range ps.buffer {
		if err := enc.Encode(v); err != nil {
			panic("couldn't encode a value: " + err.Error())
		}
	}
	ps.buffer = nil
	ps.bufferLen = 0
	if err := enc.Close(); err != nil {
		panic("error when closing encoder: " + err.Error())
	}
}

type reader interface {
	Next() interface{}
}

type sliceReader struct {
	n     int
	slice []interface{}
}

func (sr *sliceReader) Next() interface{} {
	if sr.n == len(sr.slice) {
		sr.slice = nil
		sr.n = 0
		return nil
	}
	sr.n++
	return sr.slice[sr.n-1]
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

func (fr *fileReader) Next() interface{} {
	if fr.file == nil {
		return nil
	}
	res, err := fr.dec.Decode()
	if err != nil && err != io.EOF {
		panic("error while decoding a record: " + err.Error())
	}
	if res == nil {
		fr.file.Close()
		fr.file = nil
	}
	return res
}

type mergeReader struct {
	next func() interface{}
}

func (mr *mergeReader) Next() interface{} {
	return mr.next()
}

func newMergeReader(less func(a, b interface{}) bool, rs []reader) reader {
	n := len(rs)
	if n == 1 {
		return rs[0]
	}
	var rs0, rs1 reader
	if n == 2 {
		rs0 = rs[0]
		rs1 = rs[1]
	} else {
		n = n / 2
		rs0 = newMergeReader(less, rs[:n])
		rs1 = newMergeReader(less, rs[n:])
	}
	n0 := rs0.Next()
	if n0 == nil {
		return rs1
	}
	n1 := rs1.Next()
	next := func() interface{} {
		if n0 == nil {
			return nil
		}
		if n1 == nil {
			res := n0
			n0 = rs0.Next()
			return res
		}
		if less(n0, n1) {
			res := n0
			n0 = rs0.Next()
			if n0 == nil {
				n0 = n1
				n1 = nil
				rs0 = rs1
			}
			return res
		}
		res := n1
		n1 = rs1.Next()
		return res
	}
	return &mergeReader{next: next}
}

func (ps *FileSort) merge() {
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
	mr := newMergeReader(ps.less, readers)
	for {
		next := mr.Next()
		if next == nil {
			break
		}
		ps.out <- next
	}
	close(ps.out)
}

func (ps *FileSort) Close() error {
	close(ps.in)
	return nil
}

func (ps *FileSort) Sort(v interface{}) {
	ps.in <- v
}

func (ps *FileSort) Read() interface{} {
	return <-ps.out
}
