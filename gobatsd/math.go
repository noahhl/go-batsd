package gobatsd

import (
	"math"
)

func SortedMin(a []float64) float64 {
	return a[0]
}

func SortedMax(a []float64) float64 {
	return a[len(a)-1]
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

func SortedMedian(a []float64) float64 {
	return a[len(a)/2]
}

func Stddev(a []float64) float64 {
	stddev := 0.0
	if len(a) > 1 {
		stddev = math.Pow(MeanSquared(a)/float64(len(a)-1), 0.5)
	}
	return stddev
}

func SortedPercentile(a []float64, p float64) float64 {
	return a[int(float64(len(a))*p)]
}
