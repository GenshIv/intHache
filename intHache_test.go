package intHache

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// ============================================================================
// UNIT TESTS
// ============================================================================

func TestSum(t *testing.T) {
	// Test with a known value
	hash := Sum([]byte("test"))
	if hash == 0 {
		t.Errorf("Sum(\"test\") returned 0, which is unlikely for a good hash function")
	}

	// Test empty slice
	hash = Sum([]byte{})
	if hash != 0 {
		t.Errorf("Sum of empty slice should be 0, but got %d", hash)
	}

	// Test that different inputs produce different hashes
	hash1 := Sum([]byte("input1"))
	hash2 := Sum([]byte("input2"))
	if hash1 == hash2 {
		t.Errorf("Sum of different inputs should be different")
	}
}

func TestSumString(t *testing.T) {
	// Test with a known value
	hash := SumString("test")
	if hash == 0 {
		t.Errorf("SumString(\"test\") returned 0, which is unlikely for a good hash function")
	}

	// Test empty string
	hash = SumString("")
	if hash != 0 {
		t.Errorf("SumString of empty string should be 0, but got %d", hash)
	}

	// Test that different inputs produce different hashes
	hash1 := SumString("input1")
	hash2 := SumString("input2")
	if hash1 == hash2 {
		t.Errorf("SumString of different inputs should be different")
	}
}

// TestPipeline_CheckAndInsert tests basic insertion and duplication logic.
func TestPipeline_CheckAndInsert(t *testing.T) {
	pipeline := NewPipeline(100)
	defer pipeline.Clear()

	hash1 := SumString("key1")

	// 1. Test insertion of a new hash
	if !pipeline.CheckAndInsert(hash1) {
		t.Errorf("CheckAndInsert should return true for a new hash")
	}

	// 2. Test insertion of a duplicate hash in the 'current' map
	if pipeline.CheckAndInsert(hash1) {
		t.Errorf("CheckAndInsert should return false for a duplicate hash in the 'current' map")
	}
}

// TestPipeline_Rotation forces a shard rotation and verifies the sliding window behavior.
func TestPipeline_Rotation(t *testing.T) {
	const maxShardSize = 50
	pipeline := NewPipeline(maxShardSize)
	defer pipeline.Clear()

	const targetShard uint64 = 0
	var hashesForTargetShard []int64

	// 1. Generate enough hashes to guarantee they all fall into the same target shard
	//    and exceed its max size.
	i := 0
	for len(hashesForTargetShard) <= maxShardSize {
		// Use a simple varying input to generate different hashes
		input := fmt.Sprintf("key_for_rotation_%d", i)
		hash := SumString(input)

		// Check if the generated hash belongs to our target shard
		if (uint64(hash) & ShardMask) == targetShard {
			hashesForTargetShard = append(hashesForTargetShard, hash)
		}
		i++
		// Safety break to prevent infinite loop in case of a bad hash function
		if i > 100000 {
			t.Fatal("Could not generate enough hashes for the target shard. The hash function may not have good distribution.")
			return
		}
	}

	// 2. Insert the first hash that will eventually be moved to the 'previous' map
	firstHash := hashesForTargetShard[0]
	if !pipeline.CheckAndInsert(firstHash) {
		t.Fatalf("Initial insertion of the first hash failed")
	}

	// 3. Fill the target shard until it is full.
	for _, h := range hashesForTargetShard[1:maxShardSize] {
		pipeline.CheckAndInsert(h)
	}

	// At this point, the shard is full but not yet rotated.
	if pipeline.GetRotationCount() != 0 {
		t.Errorf("Rotation should not have occurred yet, but rotation count is %d", pipeline.GetRotationCount())
	}

	// 4. Insert one more hash into the same shard to trigger the rotation.
	lastHash := hashesForTargetShard[maxShardSize]
	pipeline.CheckAndInsert(lastHash)

	// 5. Verify that a rotation has occurred.
	if pipeline.GetRotationCount() != 1 {
		t.Errorf("A rotation should have occurred, but rotation count is %d", pipeline.GetRotationCount())
	}

	// 6. Verify that the very first hash is now considered a duplicate because it's in the 'previous' map.
	if pipeline.CheckAndInsert(firstHash) {
		t.Errorf("CheckAndInsert should return false for a hash that is now in the 'previous' map")
	}

	// 7. Verify that the most recently added hash is in the 'current' map and is detected as a duplicate.
	if pipeline.CheckAndInsert(lastHash) {
		t.Errorf("CheckAndInsert should return false for a hash that is in the new 'current' map")
	}
}

// ============================================================================
// BENCHMARKS
// ============================================================================

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
