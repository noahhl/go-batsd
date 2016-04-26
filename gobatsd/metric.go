package gobatsd

type Metric interface {
	Start()
	Update(float64)
	Active() bool
}

const maxStaleTime = 1800
