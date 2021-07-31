package statistics

import (
	"fmt"
	"math"
	"sync"
	"time"
)

type DurationSet struct {
	count              uint64
	variance           uint64
	mean               uint64
	square_mean        uint64
	standard_deviation uint64
	mutex              sync.Mutex
}

func (duration_set *DurationSet) Add(t time.Duration) {
	var t_value uint64

	duration_set.mutex.Lock()
	defer duration_set.mutex.Unlock()

	t_value = uint64(t.Nanoseconds())

	duration_set.mean = ((duration_set.mean * duration_set.count) + t_value) / (duration_set.count + 1)

	duration_set.square_mean = ((duration_set.square_mean * duration_set.count) + (t_value * t_value)) / (duration_set.count + 1)

	duration_set.variance = duration_set.square_mean - (duration_set.mean * duration_set.mean)
	duration_set.standard_deviation = uint64(math.Sqrt(float64(duration_set.variance)))

	duration_set.count++
}

func (duration_set *DurationSet) Mean() time.Duration {
	var result time.Duration
	duration_set.mutex.Lock()
	result = time.Duration(duration_set.mean)
	duration_set.mutex.Unlock()
	return result
}

func (duration_set *DurationSet) Count() uint64 {
	var result uint64
	duration_set.mutex.Lock()
	result = duration_set.count
	duration_set.mutex.Unlock()
	return result
}

func (duration_set *DurationSet) Stddev() time.Duration {
	var result time.Duration
	duration_set.mutex.Lock()
	result = time.Duration(duration_set.standard_deviation)
	duration_set.mutex.Unlock()
	return result
}

func (duration_set *DurationSet) Reset() {
	duration_set.mutex.Lock()
	defer duration_set.mutex.Unlock()
	duration_set.count = 0
	duration_set.mean = 0
	duration_set.square_mean = 0
	duration_set.variance = 0
	duration_set.standard_deviation = 0
}

func (duration_set *DurationSet) String() string {
	duration_set.mutex.Lock()
	defer duration_set.mutex.Unlock()
	mean := time.Duration(duration_set.mean)
	standard_deviation := time.Duration(duration_set.standard_deviation)
	return fmt.Sprintf("Total: %d, Mean: %v, Standard Deviation: %v\n", duration_set.count, mean, standard_deviation)
}
