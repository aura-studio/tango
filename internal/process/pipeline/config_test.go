package pipeline

import (
	"testing"
	"time"
)

// TestConfigApplyDefaults_MaxStuckDuration pins the three-way semantics of the
// backpressure budget: unset takes the default, an explicit value survives, and a
// negative value (the "retry forever" escape hatch) is NOT clamped to the default.
func TestConfigApplyDefaults_MaxStuckDuration(t *testing.T) {
	cases := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"unset takes the default", 0, defaultMaxStuckDuration},
		{"explicit value survives", 45 * time.Second, 45 * time.Second},
		{"negative means unbounded", -1, -1},
		{"negative duration survives", -5 * time.Minute, -5 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Config{MaxStuckDuration: tc.in}
			c.ApplyDefaults()
			if c.MaxStuckDuration != tc.want {
				t.Errorf("MaxStuckDuration = %v, want %v", c.MaxStuckDuration, tc.want)
			}
		})
	}
}

// TestConfigRegisterDefaults_MaxStuckDuration guards the env-binding placeholder:
// without a registered key, TANGO_PROCESS_PIPELINE_MAXSTUCKDURATION is invisible
// to viper and the knob silently cannot be set from the environment.
func TestConfigRegisterDefaults_MaxStuckDuration(t *testing.T) {
	seen := map[string]any{}
	(&Config{}).RegisterDefaults(func(key string, value any) { seen[key] = value }, "process.pipeline")

	v, ok := seen["process.pipeline.maxStuckDuration"]
	if !ok {
		t.Fatalf("maxStuckDuration was not registered; registered keys: %v", seen)
	}
	if v != "0s" {
		t.Errorf("registered placeholder = %v, want \"0s\" (a duration-shaped zero)", v)
	}
}
