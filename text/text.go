// Package text implements methods that enable filesort to sort strings of
// text.
package text

import (
	"bufio"
	"io"
	"strings"

	filesort "gitlab.com/shaydo/go-filesort"
)

// Less compares two strings and returns true if the first one should come
// earlier in the sorted output than the second one
func Less(a, b interface{}) bool {
	return a.(string) < b.(string)
}

type textEncoder struct {
	w io.WriteCloser
}

// NewEncoder returns filesort.Encoder that encodes strings for storing into
// file by simply separating them with newlines. Sorted strings must not
// contain LF character, otherwise the data will be corrupted on decoding.
func NewEncoder(w io.WriteCloser) filesort.Encoder {
	return &textEncoder{w: w}
}

func (te *textEncoder) Encode(line interface{}) error {
	_, err := te.w.Write([]byte(line.(string) + "\n"))
	return err
}

func (te *textEncoder) Close() error {
	return te.w.Close()
}

type textDecoder struct {
	r *bufio.Reader
}

// NewDecoder returns filesort.Decoder that reads LF separated strings from
// the input.
func NewDecoder(r io.Reader) filesort.Decoder {
	return &textDecoder{r: bufio.NewReader(r)}
}

func (td *textDecoder) Decode() (interface{}, error) {
	val, err := td.r.ReadString(0xa)
	if err != nil {
		return nil, err
	}
	return strings.TrimRight(val, "\n"), nil
}
