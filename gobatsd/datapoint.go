package gobatsd

import (
	"strconv"
	"strings"
	"time"
)

type Datapoint struct {
	Timestamp time.Time
	Name      string
	Value     float64
	Datatype  string
}

type AggregateObservation struct {
	Name      string
	Content   string
	Timestamp int64
	RawName   string
}

func ParseDatapointFromString(metric string) Datapoint {
	d := Datapoint{}
	components := strings.Split(metric, ":")
	if len(components) == 2 {
		latter_components := strings.Split(components[1], "|")
		if len(latter_components) >= 2 {
			value, _ := strconv.ParseFloat(latter_components[0], 64)
			if len(latter_components) == 3 && latter_components[1] == "c" {
				sample_rate, _ := strconv.ParseFloat(strings.Replace(latter_components[2], "@", "", -1), 64)
				value = value / sample_rate
			}
			d = Datapoint{time.Now(), components[0], value, latter_components[1]}
		}
	}
	return d
}
