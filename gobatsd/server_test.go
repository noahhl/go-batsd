package gobatsd

import (
	"encoding/json"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestJSONEncoding(t *testing.T) {
	values := make([]map[string]string, 0)
	for i := 0; i < 100; i++ {
		values = append(values, map[string]string{"Timestamp": strconv.FormatInt(time.Now().Unix(), 10), "Value": strconv.FormatFloat(rand.Float64()*1000, 'f', 0, 64)})
	}

	packageJSON, _ := json.Marshal(values)
	artisinalJSON := ArtisinallyMarshallDatapointJSON(values)
	if string(artisinalJSON) != string(packageJSON) {
		t.Errorf("Expected artisinal JSON to match package JSON; artisinal was\n\n%v\n\npackage was \n\n%v ", string(artisinalJSON), string(packageJSON))
	}
}

func BenchmarkJSONPackageEncoding(b *testing.B) {
	values := make([]map[string]string, 0)
	for i := 0; i < 1000; i++ {
		values = append(values, map[string]string{"Timestamp": strconv.FormatInt(time.Now().Unix(), 10), "Value": strconv.FormatFloat(rand.Float64()*1000, 'f', 0, 64)})
	}
	for j := 0; j < b.N; j++ {
		json.Marshal(values)
	}
}

func BenchmarkJSONArtisinalEncoding(b *testing.B) {
	values := make([]map[string]string, 0)
	for i := 0; i < 1000; i++ {
		values = append(values, map[string]string{"Timestamp": strconv.FormatInt(time.Now().Unix(), 10), "Value": strconv.FormatFloat(rand.Float64()*1000, 'f', 0, 64)})
	}
	for j := 0; j < b.N; j++ {
		ArtisinallyMarshallDatapointJSON(values)
	}
}
