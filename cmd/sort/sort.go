package main

import (
	"bufio"
	"fmt"
	"os"

	filesort "gitlab.com/shaydo/go-filesort"
	"gitlab.com/shaydo/go-filesort/text"
)

func main() {
	src := os.Args[1]
	in, err := os.Open(src)
	if err != nil {
		panic(err)
	}
	bin := bufio.NewReader(in)
	sort, err := filesort.New(
		filesort.WithLess(text.Less),
		filesort.WithEncoderNew(text.NewEncoder),
		filesort.WithDecoderNew(text.NewDecoder),
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
