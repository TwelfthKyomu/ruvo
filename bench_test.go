package ruvo

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/TwelfthKyomu/ruvo/internal/collect"
	"github.com/TwelfthKyomu/ruvo/internal/dump"
)

var benchSizes = []int{1_000, 10_000, 100_000}

func park(n int) func() {
	ch := make(chan struct{})

	var wg sync.WaitGroup

	for range n {
		wg.Add(1)

		go func() {
			wg.Done()
			<-ch
		}()
	}

	wg.Wait()

	return func() { close(ch) }
}

func BenchmarkSample(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("debug1/%dk", n/1000), func(b *testing.B) {
			release := park(n)
			defer release()

			c := newCollector(collect.Local())
			defer c.close()

			ctx := context.Background()
			b.ReportAllocs()

			for b.Loop() {
				c.sample(ctx, false)
			}

			b.ReportMetric(float64(n), "goroutines")
		})
	}

	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("debug2/%dk", n/1000), func(b *testing.B) {
			release := park(n)
			defer release()

			src := collect.Local()
			c := newCollector(src)
			defer c.close()

			ctx := context.Background()
			b.ReportAllocs()

			var walk time.Duration

			for b.Loop() {
				start := time.Now()

				raw, err := src.Goroutines(ctx, 2)
				if err != nil {
					b.Fatal(err)
				}

				walk += time.Since(start)

				gs, err := dump.ParseFull(bytes.NewReader(raw))
				if err != nil {
					b.Fatal(err)
				}

				c.mu.Lock()
				c.applyFullLocked(c.now(), fullDump{gs: gs}, false)
				c.mu.Unlock()
			}

			b.ReportMetric(float64(walk.Microseconds())/1000/float64(b.N), "walk-ms/op")
			b.ReportMetric(float64(n), "goroutines")
		})
	}
}
