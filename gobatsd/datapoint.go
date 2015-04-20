package gobatsd

import (
	"strconv"
	"strings"
)

type Datapoint struct {
	Name     string
	Value    float64
	Datatype string
}

type AggregateObservation struct {
	Name          string
	Content       string
	Timestamp     int64
	RawName       string
	Path          string
	SummaryValues map[string]float64
	Interval      int64
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
			d = Datapoint{components[0], value, latter_components[1]}
		}
	}
	return d
}

func ArtisinallyMarshallDatapointJSON(values []map[string]string) []byte {
	marshalled := []byte{}
	marshalled = append(marshalled, '[')
	for i := range values {
		if i > 0 {
			marshalled = append(marshalled, ',')
		}
		//newstr := fmt.Sprintf("{\"Timestamp\": %v, \"Value\": %v}", values[i]["Timestamp"], values[i]["Value"])
		for k := range "{\"Timestamp\":\"" {
			marshalled = append(marshalled, "{\"Timestamp\":\""[k])
		}
		for k := range values[i]["Timestamp"] {
			marshalled = append(marshalled, values[i]["Timestamp"][k])
		}
		marshalled = append(marshalled, '"', ',')
		for k := range "\"Value\":\"" {
			marshalled = append(marshalled, "\"Value\":\""[k])
		}
		for k := range values[i]["Value"] {
			marshalled = append(marshalled, values[i]["Value"][k])
		}
		marshalled = append(marshalled, '"', '}')

	}
	marshalled = append(marshalled, ']')
	return marshalled
}

type AggregateObservations []AggregateObservation

func (o AggregateObservations) Len() int {
	return len(o)
}

func (o AggregateObservations) Swap(i, j int) {
	o[i], o[j] = o[j], o[i]
}

func (o AggregateObservations) Less(i, j int) bool {
	return o[i].Timestamp < o[j].Timestamp
}
