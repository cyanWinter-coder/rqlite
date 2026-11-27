package main

import "testing"

func Test_ForgeWriteGateDefaults(t *testing.T) {
	_, cfg, err := Forge([]string{"/data"})
	if err != nil {
		t.Fatalf("failed to parse flags: %s", err.Error())
	}
	if cfg.WriteGateCapacity != 1024 {
		t.Fatalf("default write-gate capacity: exp 1024, got %d", cfg.WriteGateCapacity)
	}
	if cfg.WriteGateWaitLimit != 1024 {
		t.Fatalf("default write-gate wait limit: exp 1024, got %d", cfg.WriteGateWaitLimit)
	}
	if cfg.WriteGateFairShare != 0 {
		t.Fatalf("default write-gate fair share: exp 0, got %d", cfg.WriteGateFairShare)
	}
	if cfg.WriteGateWaitTimeout.String() != "30s" {
		t.Fatalf("default write-gate wait timeout: exp 30s, got %s", cfg.WriteGateWaitTimeout)
	}
}

func Test_ForgeWriteGateOverrides(t *testing.T) {
	_, cfg, err := Forge([]string{
		"-write-gate-capacity", "64",
		"-write-gate-wait-limit", "128",
		"-write-gate-fair-share", "8",
		"-write-gate-wait-timeout", "5s",
		"/data",
	})
	if err != nil {
		t.Fatalf("failed to parse flags: %s", err.Error())
	}
	if cfg.WriteGateCapacity != 64 {
		t.Fatalf("write-gate capacity: exp 64, got %d", cfg.WriteGateCapacity)
	}
	if cfg.WriteGateWaitLimit != 128 {
		t.Fatalf("write-gate wait limit: exp 128, got %d", cfg.WriteGateWaitLimit)
	}
	if cfg.WriteGateFairShare != 8 {
		t.Fatalf("write-gate fair share: exp 8, got %d", cfg.WriteGateFairShare)
	}
	if cfg.WriteGateWaitTimeout.String() != "5s" {
		t.Fatalf("write-gate wait timeout: exp 5s, got %s", cfg.WriteGateWaitTimeout)
	}
}

func Test_ForgeWriteGateDisabled(t *testing.T) {
	_, cfg, err := Forge([]string{"-write-gate-capacity", "0", "/data"})
	if err != nil {
		t.Fatalf("failed to parse flags: %s", err.Error())
	}
	if cfg.WriteGateCapacity != 0 {
		t.Fatalf("write-gate capacity: exp 0 (disabled), got %d", cfg.WriteGateCapacity)
	}
}
