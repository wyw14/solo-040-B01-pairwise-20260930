package admission

import (
	"errors"
	"sort"
	"sync"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

type QueueEntry struct {
	Vessel   model.Vessel `json:"vessel"`
	PlanID   uuid.UUID    `json:"plan_id"`
	Released bool         `json:"released"`

	// position records the entry's index in the chamber queue at the moment it
	// was removed, so a rollback can restore it at its original place instead
	// of jumping the queue. It is not part of the external view.
	position int
}

// Queue preserves FIFO order while supporting convoy cancellation recovery.
type Queue struct {
	mu      sync.RWMutex
	entries map[string][]QueueEntry
}

func NewQueue() *Queue { return &Queue{entries: make(map[string][]QueueEntry)} }

func (q *Queue) Add(chamberID string, planID uuid.UUID, vessels []model.Vessel) error {
	if chamberID == "" || planID == uuid.Nil || len(vessels) == 0 {
		return errors.New("queue addition requires chamber, plan and vessels")
	}
	q.mu.Lock()
	for _, vessel := range vessels {
		q.entries[chamberID] = append(q.entries[chamberID], QueueEntry{Vessel: vessel, PlanID: planID})
	}
	q.mu.Unlock()
	return nil
}

// Peek returns the first vessel still waiting to enter. Already-released
// vessels have left the waiting queue and must never block later plans.
func (q *Queue) Peek(chamberID string) (QueueEntry, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	for _, entry := range q.entries[chamberID] {
		if !entry.Released {
			return entry, true
		}
	}
	return QueueEntry{}, false
}

func (q *Queue) MarkReleased(chamberID string, vesselID uuid.UUID) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for index := range q.entries[chamberID] {
		if q.entries[chamberID][index].Vessel.ID == vesselID {
			q.entries[chamberID][index].Released = true
			return true
		}
	}
	return false
}

// RemovePlan detaches every entry of a plan, preserving each entry's release
// state and original queue position for an exact rollback via RestorePlan.
func (q *Queue) RemovePlan(chamberID string, planID uuid.UUID) []QueueEntry {
	q.mu.Lock()
	defer q.mu.Unlock()
	entries := q.entries[chamberID]
	kept := entries[:0]
	var removed []QueueEntry
	for index, entry := range entries {
		if entry.PlanID == planID {
			entry.position = index
			removed = append(removed, entry)
		} else {
			kept = append(kept, entry)
		}
	}
	q.entries[chamberID] = append([]QueueEntry(nil), kept...)
	return removed
}

// RestorePlan reinserts entries removed by RemovePlan back at their original
// positions, preserving their release state. Entries whose vessel is already
// queued are skipped, so replaying a rollback cannot duplicate vessels or
// disturb FIFO order. Entries appended after the removal (newer plans) stay at
// the tail. It is safe even if other plans were removed in between.
func (q *Queue) RestorePlan(chamberID string, entries []QueueEntry) {
	q.mu.Lock()
	defer q.mu.Unlock()

	present := make(map[uuid.UUID]struct{}, len(q.entries[chamberID]))
	for _, entry := range q.entries[chamberID] {
		present[entry.Vessel.ID] = struct{}{}
	}

	restored := make([]QueueEntry, 0, len(entries))
	for _, entry := range entries {
		if _, ok := present[entry.Vessel.ID]; ok {
			continue
		}
		restored = append(restored, entry)
	}
	if len(restored) == 0 {
		return
	}
	sort.SliceStable(restored, func(i, j int) bool { return restored[i].position < restored[j].position })

	result := append([]QueueEntry(nil), q.entries[chamberID]...)
	for _, entry := range restored {
		// All previously restored entries sat at smaller positions, so the
		// entry's original index is its insertion index in the growing slice.
		index := entry.position
		if index < 0 {
			index = 0
		}
		if index > len(result) {
			index = len(result)
		}
		result = append(result, QueueEntry{})
		copy(result[index+1:], result[index:])
		result[index] = entry
	}
	q.entries[chamberID] = result
}

func (q *Queue) List(chamberID string) []QueueEntry {
	q.mu.RLock()
	result := append([]QueueEntry(nil), q.entries[chamberID]...)
	q.mu.RUnlock()
	return result
}
