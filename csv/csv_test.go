package csv

import (
	"fmt"
	"strings"
	"testing"

	filesort "github.com/trinitum/go-filesort"
)

func Example() {
	less := func(a, b interface{}) bool {
		sa, sb := a.([]string), b.([]string)
		if sa[1] < sb[1] || (sa[1] == sb[1] && sa[0] < sb[0]) {
			return true
		}
		return false
	}
	sort, err := filesort.New(
		filesort.WithLess(less),
		filesort.WithEncoderNew(NewEncoder),
		filesort.WithDecoderNew(NewDecoder),
	)
	if err != nil {
		panic(err)
	}
	sort.Write([]string{"Danny", "35", "66"})
	sort.Write([]string{"Alice", "35", "70"})
	sort.Write([]string{"Charly", "35", "93"})
	sort.Write([]string{"Bob", "7", "84"})
	sort.Close()
	for {
		res, err := sort.Read()
		if err != nil {
			panic(err)
		}
		if res == nil {
			// end of output
			break
		}
		fmt.Println(strings.Join(res.([]string), ","))
	}
	// Output:
	// Alice,35,70
	// Charly,35,93
	// Danny,35,66
	// Bob,7,84
}

func TestCSVSort(t *testing.T) {
	less := func(a, b interface{}) bool {
		sa, sb := a.([]string), b.([]string)
		if sa[1] < sb[1] || (sa[1] == sb[1] && sa[0] < sb[0]) {
			return true
		}
		return false
	}
	sort, err := filesort.New(
		filesort.WithLess(less),
		filesort.WithEncoderNew(NewEncoder),
		filesort.WithDecoderNew(NewDecoder),
		filesort.WithMaxMemoryBuffer(3),
	)
	if err != nil {
		t.Fatal(err)
	}
	input := [][]string{
		[]string{"one", "d", "horse"},
		[]string{"two", "c", "rhinoceros"},
		[]string{"three", "a", ",\nStickly-Prickly,Hedgehog\n"},
		[]string{"one", "c", "cat"},
		[]string{"two", "a", "elefant"},
		[]string{"three", "b", "dog"},
		[]string{"one", "d", "cow"},
		[]string{"two", "b", `"Slow-Solid",Tortoise`},
		[]string{"three", "d", "armadillo"},
	}
	for _, str := range input {
		if err := sort.Write(str); err != nil {
			t.Fatalf("write has failed: %v", err)
		}
	}
	sort.Close()
	expected := [][]string{
		[]string{"three", "a", ",\nStickly-Prickly,Hedgehog\n"},
		[]string{"two", "a", "elefant"},
		[]string{"three", "b", "dog"},
		[]string{"two", "b", `"Slow-Solid",Tortoise`},
		[]string{"one", "c", "cat"},
		[]string{"two", "c", "rhinoceros"},
		[]string{"one", "d", "horse"},
		[]string{"one", "d", "cow"},
		[]string{"three", "d", "armadillo"},
	}
	for _, e := range expected {
		str := strings.Join(e, "/")
		s, err := sort.Read()
		if err != nil {
			t.Fatalf("couldn't read: %v", err)
		}
		if got := strings.Join(s.([]string), "/"); got != str {
			t.Errorf("expected %s but got %s", str, got)
		}
	}
	s, err := sort.Read()
	if s != nil || err != nil {
		t.Errorf("expected EOF, but got: %v %v", s, err)
	}
}
