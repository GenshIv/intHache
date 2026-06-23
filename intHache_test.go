package intHache

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// BenchmarkMode2_Standalone measures pure, single-call stateless hash speed (Mode 2)
func BenchmarkMode2_Standalone(b *testing.B) {
	input := "standalone_single_call_payload_verification"
	b.SetBytes(int64(len(input)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Pure stack-allocated execution, no infrastructure overhead
		_ = SumString(input)
	}
}

// BenchmarkMode1_Pipeline100M measures the multi-threaded streaming mode performance (Mode 1)
func BenchmarkMode1_Pipeline100M(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.StopTimer()

		// Initialize isolated Mode 1 infrastructure
		pipeline := NewPipeline(150_000)
		numWorkers := runtime.NumCPU()
		totalItems := 100_000_000
		itemsPerWorker := totalItems / numWorkers

		var wg sync.WaitGroup
		var duplicates int64

		b.StartTimer()

		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				bufPtr := pipeline.RentBuffer()
				buf := *bufPtr

				startIdx := workerID * itemsPerWorker
				endIdx := startIdx + itemsPerWorker

				for i := startIdx; i < endIdx; i++ {
					buf = buf[:0]
					buf = append(buf, []byte("key_")...)
					// Using simple placeholder logic for quick bytes fill
					buf = append(buf, byte(i), byte(i>>8), byte(i>>16))

					hash := Sum(buf)

					if !pipeline.CheckAndInsert(hash) {
						atomic.AddInt64(&duplicates, 1)
					}
				}

				*bufPtr = buf
				pipeline.ReturnBuffer(bufPtr)
			}(w)
		}

		wg.Wait()
		b.StopTimer()

		// Clean up memory explicitly right after execution
		pipeline.Clear()
	}
}
