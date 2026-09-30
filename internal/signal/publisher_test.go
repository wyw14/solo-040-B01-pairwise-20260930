package signal

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

type failingStore struct {
	journal.Store
	fail bool
}

func (s *failingStore) Append(ctx context.Context, event model.Event) (model.Event, error) {
	if s.fail {
		return model.Event{}, context.DeadlineExceeded
	}
	return s.Store.Append(ctx, event)
}

func TestCompensateRollsBackOnAuditFailureAndIsRetryable(t *testing.T) {
	base := journal.NewMemoryStore()
	failing := &failingStore{Store: base, fail: false}
	recorder := journal.NewRecorder(failing, time.Now)
	signals := NewStore()
	publisher := NewPublisher(signals, recorder, time.Now)

	cycleID := uuid.New()
	vesselA := uuid.New()
	vesselB := uuid.New()

	// Two vessels currently show proceed in the field.
	signals.Set(vesselA, cycleID, 1, model.SignalProceed, time.Now())
	signals.Set(vesselB, cycleID, 1, model.SignalProceed, time.Now())
	publication := Publication{
		CycleID:    cycleID,
		Generation: 1,
		Published:  []uuid.UUID{vesselA, vesselB},
		Previous:   map[uuid.UUID]model.SignalState{},
	}

	failing.fail = true
	if err := publisher.Compensate(context.Background(), "chamber-1", publication); err == nil {
		t.Fatal("expected compensation error when audit journal fails")
	}

	// Atomicity: despite a partial rollback, field state must be unchanged.
	for _, id := range []uuid.UUID{vesselA, vesselB} {
		state, ok := signals.Get(id)
		if !ok || state.Aspect != model.SignalProceed {
			t.Fatalf("vessel %v signal lost after failed compensation: %+v ok=%v", id, state, ok)
		}
	}

	// Same call retried once the journal recovers must fully compensate.
	failing.fail = false
	if err := publisher.Compensate(context.Background(), "chamber-1", publication); err != nil {
		t.Fatalf("retry compensation failed: %v", err)
	}
	if _, ok := signals.Get(vesselA); ok {
		t.Fatal("vessel A signal should be removed after successful compensation")
	}
	if _, ok := signals.Get(vesselB); ok {
		t.Fatal("vessel B signal should be removed after successful compensation")
	}
}

func TestCompensateRestoresPreviousAspects(t *testing.T) {
	base := journal.NewMemoryStore()
	recorder := journal.NewRecorder(base, time.Now)
	signals := NewStore()
	publisher := NewPublisher(signals, recorder, time.Now)

	cycleID := uuid.New()
	oldCycle := uuid.New()
	vessel := uuid.New()
	previous := model.SignalState{VesselID: vessel, CycleID: oldCycle, Generation: 4, Aspect: model.SignalHold, UpdatedAt: time.Now()}
	signals.Set(vessel, oldCycle, 4, model.SignalHold, time.Now())

	publication := Publication{
		CycleID:    cycleID,
		Generation: 5,
		Published:  []uuid.UUID{vessel},
		Previous:   map[uuid.UUID]model.SignalState{vessel: previous},
	}
	signals.Set(vessel, cycleID, 5, model.SignalProceed, time.Now())

	if err := publisher.Compensate(context.Background(), "chamber-1", publication); err != nil {
		t.Fatalf("compensate: %v", err)
	}
	state, ok := signals.Get(vessel)
	if !ok || state.Aspect != model.SignalHold || state.Generation != 4 {
		t.Fatalf("expected previous hold/gen4 restored, got %+v ok=%v", state, ok)
	}
}
