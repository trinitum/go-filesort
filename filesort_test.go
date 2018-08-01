package filesort

import (
	"bufio"
	"io"
	"strings"
	"testing"
)

func testLessLine(a, b interface{}) bool { return a.(string) < b.(string) }

type testLineEncoder struct {
	w io.WriteCloser
}

func newTestLineEncoder(w io.WriteCloser) Encoder {
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

func newTestLineDecoder(r io.Reader) Decoder {
	return &testLineDecoder{r: bufio.NewReader(r)}
}

func (ld *testLineDecoder) Decode() (interface{}, error) {
	val, err := ld.r.ReadString(0xa)
	if err != nil {
		return nil, err
	}
	return strings.TrimRight(val, "\n"), nil
}

func TestSort(t *testing.T) {
	sort, err := New(WithLess(testLessLine), WithEncoderNew(newTestLineEncoder), WithDecoderNew(newTestLineDecoder), WithMaxMemoryBuffer(3))
	if err != nil {
		t.Fatal(err)
	}
	lines := []string{
		"aaaa",
		"zzzz",
		"yyyy",
		"iiii",
		"ffff",
		"kkkk",
		"qqqq",
		"tttt",
	}
	for _, l := range lines {
		if err := sort.Sort(l); err != nil {
			t.Fatal(err)
		}
	}
	if err := sort.Close(); err != nil {
		t.Fatal(err)
	}
	var n int
	prev := ""
	for {
		out, err := sort.Read()
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			break
		}
		n++
		str := out.(string)
		if len(str) != 4 || str <= prev {
			t.Errorf("%s came after %s", str, prev)
		}
		prev = str
	}
	if n != len(lines) {
		t.Errorf("expected to read %d values, but got %d", len(lines), n)
	}
}
