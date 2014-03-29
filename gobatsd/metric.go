package gobatsd

type Metric interface {
	Start()
	Update(float64)
}
