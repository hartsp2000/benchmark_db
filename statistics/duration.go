package statistics

import (
	"fmt"
	"sync"
	"time"
)

type DurationDistribution struct {
	minimum   int64
	maximum   int64
	increment int64

	population       []uint64
	relative_percent []float64
	absolute_percent []float64

	total_count     uint64
	total_in_count  uint64
	total_out_count uint64

	mutex sync.Mutex

	placeholder int64
}

func NewDistribution(maximum_range time.Duration, placeholder int) *DurationDistribution {

	var tmp DurationDistribution = DurationDistribution{}

	tmp.total_in_count = 0
	tmp.total_out_count = 0

	tmp.population = make([]uint64, placeholder)
	tmp.relative_percent = make([]float64, placeholder+1)
	tmp.absolute_percent = make([]float64, placeholder+1)

	tmp.minimum = 0
	tmp.placeholder = int64(placeholder)
	tmp.maximum = maximum_range.Nanoseconds()
	tmp.increment = (tmp.maximum - tmp.minimum) / int64(placeholder)

	tmp.population = make([]uint64, tmp.placeholder)

	tmp.relative_percent = make([]float64, tmp.placeholder+1)
	tmp.absolute_percent = make([]float64, tmp.placeholder+1)

	for j := 0; j < int(tmp.placeholder+1); j++ {
		if j < int(tmp.placeholder) { // only NB_SLOT_NBS cells for population!
			tmp.population[j] = 0
		}
		tmp.relative_percent[j] = 0.0
		tmp.absolute_percent[j] = 0.0
	}

	return &tmp
}

func (duration_distribution *DurationDistribution) Add(t time.Duration) {
	var t_value int64

	duration_distribution.mutex.Lock()
	defer duration_distribution.mutex.Unlock()

	duration_distribution.total_count++

	t_value = t.Nanoseconds()

	if t_value >= duration_distribution.maximum {
		duration_distribution.total_out_count++ // It is an outlier!

		for j := 0; j < int(duration_distribution.placeholder); j++ {
			duration_distribution.absolute_percent[j] = float64(duration_distribution.population[j]) / float64(duration_distribution.total_count)
		}
		duration_distribution.absolute_percent[duration_distribution.placeholder] = float64(duration_distribution.total_out_count) / float64(duration_distribution.total_count)
		return
	}

	var slot int

	slot = int(t_value / duration_distribution.increment)

	if slot >= int(duration_distribution.placeholder) {
		duration_distribution.total_out_count++ //

		for j := 0; j < int(duration_distribution.placeholder); j++ {
			duration_distribution.absolute_percent[j] = float64(duration_distribution.population[j]) / float64(duration_distribution.total_count)
		}
		duration_distribution.absolute_percent[duration_distribution.placeholder] = float64(duration_distribution.total_out_count) / float64(duration_distribution.total_count)
	} else {
		duration_distribution.total_in_count++ //

		duration_distribution.population[slot]++

		for j := 0; j < int(duration_distribution.placeholder); j++ {
			duration_distribution.absolute_percent[j] = float64(duration_distribution.population[j]) / float64(duration_distribution.total_in_count)
			duration_distribution.relative_percent[j] = float64(duration_distribution.population[j]) / float64(duration_distribution.total_in_count)
		}
		duration_distribution.absolute_percent[duration_distribution.placeholder] = float64(duration_distribution.total_out_count) / float64(duration_distribution.total_count)
		duration_distribution.relative_percent[duration_distribution.placeholder] = float64(duration_distribution.total_in_count) / float64(duration_distribution.total_in_count)

	}

}

func (duration_distribution *DurationDistribution) String() string {
	var result string
	var pop_per float64
	var out_per float64

	duration_distribution.mutex.Lock()
	defer duration_distribution.mutex.Unlock()

	result += fmt.Sprintf("Total Population: %d\n", duration_distribution.total_count)

	pop_per = 100.0 * float64(duration_distribution.total_in_count) / float64(duration_distribution.total_count)
	out_per = 100.0 * float64(duration_distribution.total_out_count) / float64(duration_distribution.total_count)

	result += fmt.Sprintf("Table [%v,%v]:\n", time.Duration(duration_distribution.minimum), time.Duration(duration_distribution.maximum))
	result += fmt.Sprintf("\tPopulation: %d (%.2f)\n", duration_distribution.total_in_count, pop_per)
	result += fmt.Sprintf("\tDeviants: %d (%.2f)\n", duration_distribution.total_out_count, out_per)

	pop_line := ""
	rel_line := ""
	abs_line := ""
	first := true

	for j := 0; j < int(duration_distribution.placeholder); j++ {

		if first {
			pop_line = fmt.Sprintf("\t%v", duration_distribution.population[j])
			rel_line = fmt.Sprintf("\t%.2f", 100*duration_distribution.relative_percent[j])
			abs_line = fmt.Sprintf("\t%.2f", 100*duration_distribution.absolute_percent[j])
			first = false
		} else {
			pop_line += fmt.Sprintf(",%v", duration_distribution.population[j])
			rel_line += fmt.Sprintf(",%.2f", 100*duration_distribution.relative_percent[j])
			abs_line += fmt.Sprintf(",%.2f", 100*duration_distribution.absolute_percent[j])
		}

	}

	result += pop_line + "\n"
	result += rel_line + "\n"
	result += abs_line + "\n\n"
	return result
}

func (duration_distribution *DurationDistribution) Reset() {
	duration_distribution.total_in_count = 0
	duration_distribution.total_out_count = 0
	for j := 0; j <= int(duration_distribution.placeholder); j++ {
		if j < int(duration_distribution.placeholder) {
			duration_distribution.population[j] = 0
		}
		duration_distribution.relative_percent[j] = 0.0
		duration_distribution.absolute_percent[j] = 0.0
	}
}
