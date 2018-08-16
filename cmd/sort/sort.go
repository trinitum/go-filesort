package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	filesort "github.com/trinitum/go-filesort"
)

func testLessLine(a, b interface{}) bool { return a.(string) < b.(string) }

type testLineEncoder struct {
	w io.WriteCloser
}

func newTestLineEncoder(w io.WriteCloser) filesort.Encoder {
	return &testLineEncoder{w: w}
}

func (le *testLineEncoder) Encode(a interface{}) error {
	if _, err := le.w.Write([]byte(a.(string) + "\n")); err != nil {
		return err
	}
	return nil
}

func (le *testLineEncoder) Close() error {
	return le.w.Close()
}

type testLineDecoder struct {
	r *bufio.Reader
}

func newTestLineDecoder(r io.Reader) filesort.Decoder {
	return &testLineDecoder{r: bufio.NewReader(r)}
}

func (ld *testLineDecoder) Decode() (interface{}, error) {
	val, err := ld.r.ReadString(0xa)
	if err != nil {
		return nil, err
	}
	return strings.TrimRight(val, "\n"), nil
}

func main() {
	src := os.Args[1]
	in, err := os.Open(src)
	if err != nil {
		panic(err)
	}
	bin := bufio.NewReader(in)
	sort, err := filesort.New(
		filesort.WithLess(testLessLine),
		filesort.WithEncoderNew(newTestLineEncoder),
		filesort.WithDecoderNew(newTestLineDecoder),
		filesort.WithMaxMemoryBuffer(1024*1024),
	)
	if err != nil {
		panic(err)
	}
	for {
		line, err := bin.ReadString('\n')
		if err != nil {
			break
		}
		if err := sort.Write(line); err != nil {
			panic(err)
		}
	}
	sort.Close()
	bout := bufio.NewWriter(os.Stdout)
	for {
		out, err := sort.Read()
		if err != nil {
			panic(err)
		}
		if out == nil {
			break
		}
		fmt.Fprint(bout, out)
	}
	bout.Flush()
}
