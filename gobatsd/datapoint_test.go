package gobatsd

import (
	"testing"
)

func TestDatapointParsing(t *testing.T) {
	d := ParseDatapointFromString("foo:1|c")
	if d.Datatype != "c" {
		t.Errorf("Datatype wrong. Expected %v, got %v\n", "c", d.Datatype)
	}
	if d.Value != 1.0 {
		t.Errorf("Value wrong. Expected %v, got %v\n", 1.0, d.Value)
	}

	d = ParseDatapointFromString("foo:1|c|@0.1")
	if d.Datatype != "c" {
		t.Errorf("Datatype wrong. Expected %v, got %v\n", "c", d.Datatype)
	}
	if d.Value != 10 {
		t.Errorf("Value wrong. Expected %v, got %v\n", 10, d.Value)
	}

	d = ParseDatapointFromString("foo:1.7|g")
	if d.Datatype != "g" {
		t.Errorf("Datatype wrong. Expected %v, got %v\n", "g", d.Datatype)
	}
	if d.Value != 1.7 {
		t.Errorf("Value wrong. Expected %v, got %v\n", 1.7, d.Value)
	}

	d = ParseDatapointFromString("foo:30303|ms")
	if d.Datatype != "ms" {
		t.Errorf("Datatype wrong. Expected %v, got %v\n", "ms", d.Datatype)
	}
	if d.Value != 30303.0 {
		t.Errorf("Value wrong. Expected %v, got %v\n", 30303.0, d.Value)
	}

}

func BenchmarkDatapointParsing(b *testing.B) {
	for j := 0; j < b.N; j++ {
		ParseDatapointFromString("foo.bar.baz.a.long.random.metric:3920230|ms")
	}
}

func BenchmarkDatapointParsingWithSampleRate(b *testing.B) {
	for j := 0; j < b.N; j++ {
		ParseDatapointFromString("foo.bar.baz.a.long.random.metric:0.394|c|@0.1")
	}
}
