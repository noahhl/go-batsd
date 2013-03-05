package shared

import (
	"math"
	"sort"
)

func Min(a []float64) float64 {
	min := a[0]
	for i := range a {
		min = math.Min(min, a[i])
	}
	return min
}

func Max(a []float64) float64 {
	max := a[0]
	for i := range a {
		max = math.Max(max, a[i])
	}
	return max
}

func Sum(a []float64) float64 {
	sum := 0.0
	for i := range a {
		sum += a[i]
	}
	return sum
}

func Mean(a []float64) float64 {
	return Sum(a) / float64(len(a))
}

func MeanSquared(a []float64) float64 {
	ms := 0.0
	mean := Mean(a)
	for i := range a {
		ms += math.Pow(a[i]-mean, 2.0)
	}
	return ms

}

func Median(a []float64) float64 {
	sort.Float64s(a)
	return a[len(a)/2]
}

func Stddev(a []float64) float64 {
	stddev := 0.0
	if len(a) > 1 {
		stddev = math.Pow(MeanSquared(a)/float64(len(a)-1), 0.5)
	}
	return stddev
}

func Percentile(a []float64, p float64) float64 {
	sort.Float64s(a)
	return a[int(float64(len(a))*p)]
}
