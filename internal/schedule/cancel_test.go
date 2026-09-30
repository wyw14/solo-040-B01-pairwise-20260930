package schedule_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/schedule"
	"github.com/wyw14/cry-96/internal/signal"
)

const chamberID = "chamber-1"

// failingStore fails journal appends while armed, simulating compensation
// errors that must leave plan and queue state untouched.
type failingStore struct {
	journal.Store
	fail bool
}

func (s *failingStore) Append(ctx context.Context, event model.Event) (model.Event, error) {
	if s.fail {
		return model.Event{}, errors.New("journal append failed")
	}
	return s.Store.Append(ctx, event)
}

type fixture struct {
	planner   *schedule.Planner
	publisher *signal.Publisher
	admission *admission.Service
	queue     *admission.Queue
	signals   *signal.Store
	store     *failingStore
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	now := func() time.Time { return time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC) }
	store := &failingStore{Store: journal.NewMemoryStore()}
	recorder := journal.NewRecorder(store, now)
	queue := admission.NewQueue()
	gates := gate.NewState(chamberID)
	signalStore := signal.NewStore()
	controller := admission.NewController(queue, gates, signalStore, now)
	admissionService := admission.NewService(controller, recorder, now)
	return &fixture{
		planner:   schedule.NewPlanner(admissionService, recorder, now),
		publisher: signal.NewPublisher(signalStore, recorder, now),
		admission: admissionService,
		queue:     queue,
		signals:   signalStore,
		store:     store,
	}
}

func newVessels(t *testing.T, names ...string) []model.Vessel {
	t.Helper()
	result := make([]model.Vessel, 0, len(names))
	for _, name := range names {
		vessel, err := model.NewVessel(name, "call-"+name, 42)
		if err != nil {
			t.Fatalf("build vessel %s: %v", name, err)
		}
		result = append(result, vessel)
	}
	return result
}

// publication mirrors how the cancel handler derives the compensatable record
// from the currently published field signals.
func publication(store *signal.Store, cycle model.TransitCycle, plan model.ConvoyPlan) signal.Publication {
	result := signal.Publication{
		CycleID: cycle.ID, Generation: cycle.Generation,
		Previous: make(map[uuid.UUID]model.SignalState),
	}
	for _, vessel := range plan.Vessels {
		if _, ok := store.Get(vessel.ID); ok {
			result.Published = append(result.Published, vessel.ID)
		}
	}
	return result
}

func newCycle(t *testing.T, plan model.ConvoyPlan, generation uint64) model.TransitCycle {
	t.Helper()
	cycle, err := model.NewTransitCycle(plan.ChamberID, plan.ID, plan.Direction, generation, time.Now().UTC())
	if err != nil {
		t.Fatalf("build transit cycle: %v", err)
	}
	return cycle
}

// A cancelled convoy must leave neither queue entries nor field signals
// behind, so the next plan owns the queue head and can be admitted.
func TestCancelRemovesVesselsFromQueue(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	planA, err := f.planner.Create(ctx, chamberID, model.DirectionUpstream, newVessels(t, "alpha", "bravo"))
	if err != nil {
		t.Fatalf("create plan A: %v", err)
	}
	cycleA := newCycle(t, planA, 1)
	if _, err := f.publisher.Publish(ctx, cycleA, planA.Vessels, model.SignalProceed); err != nil {
		t.Fatalf("publish signals: %v", err)
	}

	cancellation, err := f.planner.Cancel(ctx, planA.ID, "operator cancellation", f.publisher, publication(f.signals, cycleA, planA))
	if err != nil {
		t.Fatalf("cancel plan A: %v", err)
	}
	if !cancellation.Plan.Cancelled {
		t.Fatal("cancelled plan is not marked cancelled")
	}
	if cancellation.RemovedQueue != 2 || cancellation.Compensated != 2 {
		t.Fatalf("unexpected cancellation counts: removed=%d compensated=%d", cancellation.RemovedQueue, cancellation.Compensated)
	}
	if entries := f.queue.List(chamberID); len(entries) != 0 {
		t.Fatalf("cancelled convoy still occupies the queue: %+v", entries)
	}
	for _, vessel := range planA.Vessels {
		if _, ok := f.signals.Get(vessel.ID); ok {
			t.Fatalf("cancelled vessel %s still has a field signal", vessel.Name)
		}
	}
	if active := f.planner.Active(chamberID); len(active) != 0 {
		t.Fatalf("cancelled plan still listed as active: %+v", active)
	}

	// A follow-up plan in the same chamber must own the queue head and be admitted.
	planB, err := f.planner.Create(ctx, chamberID, model.DirectionUpstream, newVessels(t, "charlie"))
	if err != nil {
		t.Fatalf("create plan B: %v", err)
	}
	head, ok := f.queue.Peek(chamberID)
	if !ok || head.PlanID != planB.ID {
		t.Fatalf("new plan blocked by stale queue head: %+v", head)
	}
	decision, err := f.admission.Admit(ctx, newCycle(t, planB, 2))
	if err != nil || !decision.Allowed {
		t.Fatalf("new plan cannot enter after cancellation: %+v, %v", decision, err)
	}
	if decision.VesselID != planB.Vessels[0].ID {
		t.Fatalf("admitted vessel is not the new plan's head: %s", decision.VesselID)
	}
}

// When signal compensation fails, the queue and the plan must stay exactly as
// they were so the operator can retry the cancellation.
func TestCancelCompensationFailurePreservesState(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	planA, err := f.planner.Create(ctx, chamberID, model.DirectionUpstream, newVessels(t, "alpha", "bravo"))
	if err != nil {
		t.Fatalf("create plan A: %v", err)
	}
	cycleA := newCycle(t, planA, 1)
	if _, err := f.publisher.Publish(ctx, cycleA, planA.Vessels, model.SignalProceed); err != nil {
		t.Fatalf("publish signals: %v", err)
	}
	if !f.queue.MarkReleased(chamberID, planA.Vessels[0].ID) {
		t.Fatal("failed to mark head vessel released")
	}
	before := f.queue.List(chamberID)

	f.store.fail = true
	_, err = f.planner.Cancel(ctx, planA.ID, "operator cancellation", f.publisher, publication(f.signals, cycleA, planA))
	if err == nil || !strings.Contains(err.Error(), "signal compensation failed") {
		t.Fatalf("expected compensation failure, got %v", err)
	}
	if after := f.queue.List(chamberID); !reflect.DeepEqual(after, before) {
		t.Fatalf("queue changed despite failed compensation:\nbefore: %+v\nafter:  %+v", before, after)
	}
	plan, ok := f.planner.Get(planA.ID)
	if !ok || plan.Cancelled {
		t.Fatal("plan marked cancelled despite failed compensation")
	}
	if active := f.planner.Active(chamberID); len(active) != 1 || active[0].ID != planA.ID {
		t.Fatalf("plan lost from active list after failed cancel: %+v", active)
	}

	f.store.fail = false
	cancellation, err := f.planner.Cancel(ctx, planA.ID, "operator cancellation", f.publisher, publication(f.signals, cycleA, planA))
	if err != nil {
		t.Fatalf("retried cancel: %v", err)
	}
	if !cancellation.Plan.Cancelled {
		t.Fatal("retried cancel did not mark the plan cancelled")
	}
	if entries := f.queue.List(chamberID); len(entries) != 0 {
		t.Fatalf("queue still occupied after retried cancel: %+v", entries)
	}
}
