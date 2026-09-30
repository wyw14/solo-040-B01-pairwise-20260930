package signal

import (
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// Store keeps the latest field aspect for each vessel.
type Store struct {
	mu     sync.RWMutex
	states map[uuid.UUID]model.SignalState
}

func NewStore() *Store { return &Store{states: make(map[uuid.UUID]model.SignalState)} }

func (s *Store) Set(vesselID, cycleID uuid.UUID, generation uint64, aspect model.SignalAspect, now time.Time) model.SignalState {
	state := model.SignalState{
		VesselID: vesselID, CycleID: cycleID, Generation: generation,
		Aspect: aspect, UpdatedAt: now.UTC(),
	}
	s.mu.Lock()
	s.states[vesselID] = state
	s.mu.Unlock()
	return state
}

func (s *Store) Get(vesselID uuid.UUID) (model.SignalState, bool) {
	s.mu.RLock()
	state, ok := s.states[vesselID]
	s.mu.RUnlock()
	return state, ok
}

func (s *Store) Remove(vesselID uuid.UUID) {
	s.mu.Lock()
	delete(s.states, vesselID)
	s.mu.Unlock()
}

// Snapshot returns a deep copy of every current signal state.
func (s *Store) Snapshot() map[uuid.UUID]model.SignalState {
	s.mu.RLock()
	snapshot := make(map[uuid.UUID]model.SignalState, len(s.states))
	for vesselID, state := range s.states {
		snapshot[vesselID] = state
	}
	s.mu.RUnlock()
	return snapshot
}

// Restore replaces all signal state with the given snapshot. A nil snapshot
// clears the store. Used to roll back a partially applied compensation so the
// caller can keep its prior state and retry the operation.
func (s *Store) Restore(snapshot map[uuid.UUID]model.SignalState) {
	restored := make(map[uuid.UUID]model.SignalState, len(snapshot))
	for vesselID, state := range snapshot {
		restored[vesselID] = state
	}
	s.mu.Lock()
	s.states = restored
	s.mu.Unlock()
}

func (s *Store) List() []model.SignalState {
	s.mu.RLock()
	result := make([]model.SignalState, 0, len(s.states))
	for _, state := range s.states {
		result = append(result, state)
	}
	s.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool {
		return result[i].VesselID.String() < result[j].VesselID.String()
	})
	return result
}
