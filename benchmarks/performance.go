package benchmarks

import (
	"testing"
)

func BenchmarkWrites(b *testing.B) {
	for i := 0; i < b.N; i++ {
		// Simulate a write operation
	}
}
