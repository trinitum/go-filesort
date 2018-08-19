package text

import (
	"fmt"
	"testing"

	filesort "github.com/trinitum/go-filesort"
)

func Example() {
	sort, err := filesort.New(
		filesort.WithLess(Less),
		filesort.WithEncoderNew(NewEncoder),
		filesort.WithDecoderNew(NewDecoder),
	)
	if err != nil {
		panic(err)
	}
	sort.Write("Alice")
	sort.Write("Charly")
	sort.Write("Bob")
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
		fmt.Println(res.(string))
	}
	// Output:
	// Alice
	// Bob
	// Charly
}

func TestTextSort(t *testing.T) {
	sort, err := filesort.New(
		filesort.WithLess(Less),
		filesort.WithEncoderNew(NewEncoder),
		filesort.WithDecoderNew(NewDecoder),
		filesort.WithMaxMemoryBuffer(3),
	)
	if err != nil {
		t.Fatal(err)
	}
	input := []string{
		"one",
		"two",
		"three",
		"four",
		"five",
		"six",
		"seven",
		"eight",
		"nine",
		"ten",
	}
	for _, str := range input {
		if err := sort.Write(str); err != nil {
			t.Fatalf("write has failed: %v", err)
		}
	}
	sort.Close()
	expected := []string{
		"eight",
		"five",
		"four",
		"nine",
		"one",
		"seven",
		"six",
		"ten",
		"three",
		"two",
	}
	for _, str := range expected {
		s, err := sort.Read()
		if err != nil {
			t.Fatalf("couldn't read: %v", err)
		}
		if s.(string) != str {
			t.Errorf("expected %s but got %s", str, s)
		}
	}
	s, err := sort.Read()
	if s != nil || err != nil {
		t.Errorf("expected EOF, but got: %v %v", s, err)
	}
}
