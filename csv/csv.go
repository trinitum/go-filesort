// Package csv implements methods that enable filesort to sort slices of strings that can be encoded as csv for storing to disk.
package csv

import (
	"encoding/csv"
	"io"

	filesort "gitlab.com/shaydo/go-filesort"
)

type csvEncoder struct {
	w  io.Closer
	cw *csv.Writer
}

// NewEncoder returns filesort.Encoder that encodes slices of strings as CSV
// for storing into file.
func NewEncoder(w io.WriteCloser) filesort.Encoder {
	return &csvEncoder{w: w, cw: csv.NewWriter(w)}
}

func (ce *csvEncoder) Encode(row interface{}) error {
	return ce.cw.Write(row.([]string))
}

func (ce *csvEncoder) Close() error {
	ce.cw.Flush()
	if err := ce.cw.Error(); err != nil {
		ce.w.Close()
		return err
	}
	return ce.w.Close()
}

type csvDecoder struct {
	r *csv.Reader
}

// NewDecoder returns filesort.Decoder that reads CSV encoded slices of strings
func NewDecoder(r io.Reader) filesort.Decoder {
	c := csv.NewReader(r)
	c.FieldsPerRecord = -1
	return &csvDecoder{r: csv.NewReader(r)}
}

func (cd *csvDecoder) Decode() (interface{}, error) {
	s, err := cd.r.Read()
	if err != nil {
		return nil, err
	}
	return s, nil
}
