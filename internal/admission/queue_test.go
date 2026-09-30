package admission

import (
	"testing"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

func mustVessel(t *testing.T, name string) model.Vessel {
	t.Helper()
	vessel, err := model.NewVessel(name, "CS-"+name, 10)
	if err != nil {
		t.Fatalf("vessel %s: %v", name, err)
	}
	return vessel
}

func planIDs() (uuid.UUID, uuid.UUID) { return uuid.New(), uuid.New() }

func TestPeekSkipsReleasedVessels(t *testing.T) {
	queue := NewQueue()
	planA, planB := planIDs()
	a, b, c := mustVessel(t, "A"), mustVessel(t, "B"), mustVessel(t, "C")

	if err := queue.Add("chamber-1", planA, []model.Vessel{a}); err != nil {
		t.Fatal(err)
	}
	if err := queue.Add("chamber-1", planB, []model.Vessel{b, c}); err != nil {
		t.Fatal(err)
	}

	head, ok := queue.Peek("chamber-1")
	if !ok || head.Vessel.ID != a.ID {
		t.Fatalf("expected A at head, got %+v ok=%v", head, ok)
	}

	// A has physically entered; it must stop blocking the waiting queue.
	if !queue.MarkReleased("chamber-1", a.ID) {
		t.Fatal("MarkReleased should find A")
	}
	head, ok = queue.Peek("chamber-1")
	if !ok || head.PlanID != planB || head.Vessel.ID != b.ID {
		t.Fatalf("expected B at head after A released, got plan=%v vessel=%v ok=%v", head.PlanID, head.Vessel.ID, ok)
	}
}

func TestRemoveAndRestorePlanPreservesFIFO(t *testing.T) {
	queue := NewQueue()
	planA, planB := planIDs()
	a0, a1 := mustVessel(t, "A0"), mustVessel(t, "A1")
	b0, b1 := mustVessel(t, "B0"), mustVessel(t, "B1")

	if err := queue.Add("chamber-1", planA, []model.Vessel{a0}); err != nil {
		t.Fatal(err)
	}
	if err := queue.Add("chamber-1", planB, []model.Vessel{b0}); err != nil {
		t.Fatal(err)
	}
	if err := queue.Add("chamber-1", planA, []model.Vessel{a1}); err != nil {
		t.Fatal(err)
	}
	if err := queue.Add("chamber-1", planB, []model.Vessel{b1}); err != nil {
		t.Fatal(err)
	}

	removed := queue.RemovePlan("chamber-1", planA)
	if len(removed) != 2 {
		t.Fatalf("expected 2 removed entries, got %d", len(removed))
	}
	if ids := queue.List("chamber-1"); len(ids) != 2 || ids[0].Vessel.ID != b0.ID || ids[1].Vessel.ID != b1.ID {
		t.Fatalf("expected only B0,B1 after removal, got %+v", ids)
	}

	// A newer plan arriving while the cancellation is in flight stays at tail.
	c := mustVessel(t, "C")
	planC := uuid.New()
	if err := queue.Add("chamber-1", planC, []model.Vessel{c}); err != nil {
		t.Fatal(err)
	}

	queue.RestorePlan("chamber-1", removed)
	got := queue.List("chamber-1")
	wantIDs := []uuid.UUID{a0.ID, b0.ID, a1.ID, b1.ID, c.ID}
	if len(got) != len(wantIDs) {
		t.Fatalf("expected %d entries after restore, got %d", len(wantIDs), len(got))
	}
	for i, want := range wantIDs {
		if got[i].Vessel.ID != want {
			t.Fatalf("position %d: expected vessel %v, got %v", i, want, got[i].Vessel.ID)
		}
	}

	// Restoring again is a no-op: no duplicated entries, no FIFO change.
	queue.RestorePlan("chamber-1", removed)
	got = queue.List("chamber-1")
	if len(got) != len(wantIDs) {
		t.Fatalf("duplicate entries on repeated restore: %d", len(got))
	}
}

func TestRestorePlanAfterOtherPlanRemoved(t *testing.T) {
	queue := NewQueue()
	planA, planB, planC := uuid.New(), uuid.New(), uuid.New()
	a, b, c := mustVessel(t, "A"), mustVessel(t, "B"), mustVessel(t, "C")
	if err := queue.Add("chamber-1", planA, []model.Vessel{a}); err != nil {
		t.Fatal(err)
	}
	if err := queue.Add("chamber-1", planB, []model.Vessel{b}); err != nil {
		t.Fatal(err)
	}
	if err := queue.Add("chamber-1", planC, []model.Vessel{c}); err != nil {
		t.Fatal(err)
	}

	removedA := queue.RemovePlan("chamber-1", planA) // queue: B, C
	queue.RemovePlan("chamber-1", planB)             // queue: C
	queue.RestorePlan("chamber-1", removedA)         // queue: A, C (no panic)

	got := queue.List("chamber-1")
	if len(got) != 2 || got[0].Vessel.ID != a.ID || got[1].Vessel.ID != c.ID {
		t.Fatalf("expected A,C after interleaved restore, got %+v", got)
	}
}

func TestRestorePlanKeepsReleasedState(t *testing.T) {
	queue := NewQueue()
	planA, planB := planIDs()
	a, b := mustVessel(t, "A"), mustVessel(t, "B")
	if err := queue.Add("chamber-1", planA, []model.Vessel{a}); err != nil {
		t.Fatal(err)
	}
	if err := queue.Add("chamber-1", planB, []model.Vessel{b}); err != nil {
		t.Fatal(err)
	}
	queue.MarkReleased("chamber-1", a.ID)

	removed := queue.RemovePlan("chamber-1", planA)
	queue.RestorePlan("chamber-1", removed)

	entries := queue.List("chamber-1")
	if len(entries) != 2 || !entries[0].Released {
		t.Fatalf("expected A restored still released, got %+v", entries)
	}
	if head, ok := queue.Peek("chamber-1"); !ok || head.Vessel.ID != b.ID {
		t.Fatalf("expected B at head, got %+v ok=%v", head, ok)
	}
}
