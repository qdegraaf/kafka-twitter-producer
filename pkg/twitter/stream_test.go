package twitter

import (
	"reflect"
	"testing"
)

func TestRulesGetValues(t *testing.T) {
	type tests struct {
		input Rules
		want  []string
	}

	tcs := []tests{
		{input: Rules{{Tag: "tag", Value: "value"}, {Tag: "tag2", Value: ""}}, want: []string{"value", ""}},
		{input: Rules{{Tag: "tag", Value: "value"}}, want: []string{"value"}},
		{input: Rules{}, want: nil},
	}

	for _, tc := range tcs {
		got := tc.input.GetValues()
		if !reflect.DeepEqual(tc.want, got) {
			t.Fatalf("expected: %v, got: %v", tc.want, got)
		}
	}
}

func TestRulesGetTags(t *testing.T) {
	type tests struct {
		input Rules
		want  []string
	}

	tcs := []tests{
		{input: Rules{{Tag: "tag", Value: "value"}, {Tag: "", Value: ""}}, want: []string{"tag", ""}},
		{input: Rules{{Tag: "tag", Value: "value"}}, want: []string{"tag"}},
		{input: Rules{}, want: nil},
	}

	for _, tc := range tcs {
		got := tc.input.GetTags()
		if !reflect.DeepEqual(tc.want, got) {
			t.Fatalf("expected: %v, got: %v", tc.want, got)
		}
	}

}

func TestGenerateAddRulesBody(t *testing.T) {
	type tests struct {
		input Rules
		want  string
	}

	tcs := []tests{
		{input: Rules{{Tag: "tag1", Value: "value1"}, {Tag: "tag2", Value: "val2"}}, want: "{\"add\": [{\"value\": \"value1\", \"tag\": \"tag1\"},{\"value\": \"val2\", \"tag\": \"tag2\"}]}"},
		{input: Rules{{Tag: "tag1", Value: "value1"}}, want: "{\"add\": [{\"value\": \"value1\", \"tag\": \"tag1\"}]}"},
		{input: Rules{}, want: "{\"add\": []}"},
	}

	for _, tc := range tcs {
		got := generateAddRulesBody(tc.input)
		if !reflect.DeepEqual(tc.want, got) {
			t.Fatalf("expected: %v, got: %v", tc.want, got)
		}
	}
}
