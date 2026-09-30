package schedule

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

type flakyStore struct {
	journal.Store
	fail bool
}

func (s *flakyStore) Append(ctx context.Context, event model.Event) (model.Event, error) {
	if s.fail {
		return model.Event{}, context.DeadlineExceeded
	}
	return s.Store.Append(ctx, event)
}

type fixture struct {
	planner   *Planner
	publisher *signal.Publisher
	signals   *signal.Store
	queue     *admission.Queue
	store     *flakyStore
}

func newFixture() *fixture {
	now := func() time.Time { return time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC) }
	memory := journal.NewMemoryStore()
	store := &flakyStore{Store: memory}
	recorder := journal.NewRecorder(store, now)
	queue := admission.NewQueue()
	gates := gate.NewState("chamber-1")
	signals := signal.NewStore()
	controller := admission.NewController(queue, gates, signals, now)
	admissionService := admission.NewService(controller, recorder, now)
	planner := NewPlanner(admissionService, recorder, now)
	publisher := signal.NewPublisher(signals, recorder, now)
	return &fixture{planner: planner, publisher: publisher, signals: signals, queue: queue, store: store}
}

func (f *fixture) createPlan(t *testing.T, chamber string, vesselName string) model.ConvoyPlan {
	t.Helper()
	vessel, err := model.NewVessel(vesselName, "CS-"+vesselName, 20)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := f.planner.Create(context.Background(), chamber, model.DirectionUpstream, []model.Vessel{vessel})
	if err != nil {
		t.Fatalf("create plan %s: %v", vesselName, err)
	}
	return plan
}

func TestCancelRemovesVesselAndUnblocksNextPlan(t *testing.T) {
	f := newFixture()
	planA := f.createPlan(t, "chamber-1", "A")
	planB := f.createPlan(t, "chamber-1", "B")

	result, err := f.planner.Cancel(context.Background(), planA.ID, "operator cancellation", f.publisher, signal.Publication{Previous: map[uuid.UUID]model.SignalState{}})
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if !result.Plan.Cancelled || result.RemovedQueue != 1 {
		t.Fatalf("unexpected cancellation result: %+v", result)
	}

	// The old vessel must no longer occupy the chamber waiting queue.
	entries := f.queue.List("chamber-1")
	if len(entries) != 1 || entries[0].PlanID != planB.ID {
		t.Fatalf("expected only plan B vessel in queue, got %+v", entries)
	}
	head, ok := f.queue.Peek("chamber-1")
	if !ok || head.PlanID != planB.ID {
		t.Fatalf("plan B blocked by cancelled plan: head=%+v ok=%v", head, ok)
	}

	// Active plan list agrees with queue state.
	active := f.planner.Active("chamber-1")
	if len(active) != 1 || active[0].ID != planB.ID {
		t.Fatalf("expected only plan B active, got %+v", active)
	}

	// A cancelled plan can never be admitted: publish is rejected.
	svc := NewService(f.planner, f.publisher)
	if _, err := svc.Publish(context.Background(), planA.ID, model.TransitCycle{ChamberID: "chamber-1", PlanID: planA.ID, Generation: 1}); err == nil {
		t.Fatal("expected publish on cancelled plan to be rejected")
	}

	// Re-cancelling is an idempotent no-op and leaves the queue untouched.
	again, err := f.planner.Cancel(context.Background(), planA.ID, "again", f.publisher, signal.Publication{Previous: map[uuid.UUID]model.SignalState{}})
	if err != nil || again.RemovedQueue != 0 {
		t.Fatalf("second cancel should be no-op, got result=%+v err=%v", again, err)
	}
	if entries := f.queue.List("chamber-1"); len(entries) != 1 || entries[0].PlanID != planB.ID {
		t.Fatalf("queue changed after second cancel: %+v", entries)
	}
}

func TestCancelCompensationFailureKeepsOriginalStateAndRetries(t *testing.T) {
	f := newFixture()
	plan := f.createPlan(t, "chamber-1", "A")
	vessel := plan.Vessels[0]

	// Field currently shows proceed for the vessel, captured in the publication.
	cycleID := uuid.New()
	f.signals.Set(vessel.ID, cycleID, 1, model.SignalProceed, time.Now())
	publication := signal.Publication{
		CycleID: cycleID, Generation: 1,
		Published: []uuid.UUID{vessel.ID}, Previous: map[uuid.UUID]model.SignalState{},
	}

	// Journal failure makes signal compensation fail atomically.
	f.store.fail = true
	if _, err := f.planner.Cancel(context.Background(), plan.ID, "operator cancellation", f.publisher, publication); err == nil {
		t.Fatal("expected cancel error when compensation fails")
	}

	// Everything stays in its pre-cancel state.
	if head, ok := f.queue.Peek("chamber-1"); !ok || head.PlanID != plan.ID {
		t.Fatalf("queue changed after failed cancel: head=%+v ok=%v", head, ok)
	}
	if active := f.planner.Active("chamber-1"); len(active) != 1 || active[0].ID != plan.ID {
		t.Fatalf("plan marked cancelled after failed compensation: %+v", active)
	}
	if state, ok := f.signals.Get(vessel.ID); !ok || state.Aspect != model.SignalProceed {
		t.Fatalf("signal corrupted after failed compensation: %+v ok=%v", state, ok)
	}

	// Journal recovers: the identical cancellation now succeeds.
	f.store.fail = false
	result, err := f.planner.Cancel(context.Background(), plan.ID, "operator cancellation", f.publisher, publication)
	if err != nil {
		t.Fatalf("retry cancel failed: %v", err)
	}
	if !result.Plan.Cancelled {
		t.Fatal("plan should be cancelled after retry")
	}
	if _, ok := f.queue.Peek("chamber-1"); ok {
		t.Fatal("vessel still queued after successful cancel")
	}
	if _, ok := f.signals.Get(vessel.ID); ok {
		t.Fatal("proceed signal still present after successful compensation")
	}
}

func TestConcurrentCancelRunsOnce(t *testing.T) {
	f := newFixture()
	planA := f.createPlan(t, "chamber-1", "A")
	planB := f.createPlan(t, "chamber-1", "B")
	empty := signal.Publication{Previous: map[uuid.UUID]model.SignalState{}}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.planner.Cancel(context.Background(), planA.ID, "operator cancellation", f.publisher, empty)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent cancel failed: %v", err)
		}
	}

	entries := f.queue.List("chamber-1")
	if len(entries) != 1 || entries[0].PlanID != planB.ID {
		t.Fatalf("expected exactly plan B in queue after concurrent cancels, got %+v", entries)
	}
}
