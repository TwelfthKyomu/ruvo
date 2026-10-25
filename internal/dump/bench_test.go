package dump

import (
	"bytes"
	"fmt"
	"testing"
)

var sizes = []int{1_000, 10_000, 100_000}

func fullDump(n int) []byte {
	var b bytes.Buffer

	for i := range n {
		fmt.Fprintf(&b, "goroutine %d [chan receive, %d minutes]:\n", i+10, i%9)
		b.WriteString("main.(*pipeline).worker(0x3af5423e43c0)\n")
		fmt.Fprintf(&b, "\t/home/dev/leaky/pipeline.go:%d +0xb8\n", 100+i%50)
		b.WriteString("created by main.newPipeline in goroutine 1\n")
		b.WriteString("\t/home/dev/leaky/pipeline.go:64 +0x19c\n\n")
	}

	return b.Bytes()
}

func stacksDump(n int) []byte {
	var b bytes.Buffer

	fmt.Fprintf(&b, "goroutine profile: total %d\n", n)

	for i := range n {
		b.WriteString("1 @ 0x1042c8708 0x1044a95c8 0x1042cf744\n")
		b.WriteString("#\t0x1042cc4ff\ttime.Sleep+0x14f\t\t/usr/local/go/src/runtime/time.go:368\n")
		fmt.Fprintf(&b, "#\t0x1044a95c7\tmain.(*pipeline).worker+0xb7\t/home/dev/leaky/pipeline.go:%d\n\n", 100+i%50)
	}

	return b.Bytes()
}

func BenchmarkParseFull(b *testing.B) {
	for _, n := range sizes {
		data := fullDump(n)

		b.Run(fmt.Sprintf("%dk", n/1000), func(b *testing.B) {
			b.SetBytes(int64(len(data)))

			for b.Loop() {
				if _, err := ParseFull(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkParseStacks(b *testing.B) {
	for _, n := range sizes {
		data := stacksDump(n)

		b.Run(fmt.Sprintf("%dk", n/1000), func(b *testing.B) {
			b.SetBytes(int64(len(data)))

			for b.Loop() {
				if _, err := ParseStacks(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
