package spec

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestFanoutActionsFollowModelOrder(t *testing.T) {
	want := []Action{
		ActionStart,
		ActionPause,
		ActionResume,
		ActionStop,
		ActionFail,
		ActionReadBatch,
		ActionDeliver,
		ActionAckDest,
		ActionPersistCheckpoint,
		ActionCheckpointFail,
		ActionAckSource,
		ActionCrash,
		ActionRestart,
		ActionIdle,
	}
	if !reflect.DeepEqual(FanoutActions, want) {
		t.Fatalf("FanoutActions = %v, want model order %v", FanoutActions, want)
	}
}

func TestFanoutCoverageManifestMatchesRuntimeManifest(t *testing.T) {
	fileManifest, err := LoadManifest(filepath.Join("..", "..", "specs", DefaultManifestFiles[SpecCDCFlowFanout]))
	if err != nil {
		t.Fatalf("LoadManifest(): %v", err)
	}
	runtimeManifest, ok := ManifestForSpec(SpecCDCFlowFanout)
	if !ok {
		t.Fatal("fanout runtime manifest missing")
	}
	if !reflect.DeepEqual(fileManifest.Actions, runtimeManifest.Actions) {
		t.Fatalf("coverage actions = %v, runtime actions = %v", fileManifest.Actions, runtimeManifest.Actions)
	}
	if !reflect.DeepEqual(fileManifest.Invariants, runtimeManifest.Invariants) {
		t.Fatalf("coverage invariants = %v, runtime invariants = %v", fileManifest.Invariants, runtimeManifest.Invariants)
	}
}
