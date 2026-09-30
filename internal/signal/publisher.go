package signal

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

type Publication struct {
	CycleID    uuid.UUID
	Generation uint64
	Published  []uuid.UUID
	Previous   map[uuid.UUID]model.SignalState
}

// Publisher applies a convoy signal change with a compensatable record.
type Publisher struct {
	store    *Store
	recorder *journal.Recorder
	now      func() time.Time
}

func NewPublisher(store *Store, recorder *journal.Recorder, now func() time.Time) *Publisher {
	if now == nil {
		now = time.Now
	}
	return &Publisher{store: store, recorder: recorder, now: now}
}

func (p *Publisher) Publish(ctx context.Context, cycle model.TransitCycle, vessels []model.Vessel, aspect model.SignalAspect) (Publication, error) {
	publication := Publication{
		CycleID: cycle.ID, Generation: cycle.Generation,
		Previous: make(map[uuid.UUID]model.SignalState),
	}
	for _, vessel := range vessels {
		if err := ctx.Err(); err != nil {
			p.Compensate(context.WithoutCancel(ctx), cycle.ChamberID, publication)
			return publication, err
		}
		if previous, ok := p.store.Get(vessel.ID); ok {
			publication.Previous[vessel.ID] = previous
		}
		state := p.store.Set(vessel.ID, cycle.ID, cycle.Generation, aspect, p.now())
		publication.Published = append(publication.Published, vessel.ID)
		if _, err := p.recorder.Record(ctx, cycle.ChamberID, model.EventSignalChanged, cycle.ID, cycle.Generation, state); err != nil {
			p.Compensate(context.WithoutCancel(ctx), cycle.ChamberID, publication)
			return publication, err
		}
	}
	return publication, nil
}

// Compensate reverts every published signal, in reverse order. It is atomic:
// if any rollback or its audit event cannot be recorded, the signal store is
// restored to exactly what it held before this call and the joined error is
// returned. The caller therefore keeps its prior state and may retry the same
// publication until compensation succeeds.
func (p *Publisher) Compensate(ctx context.Context, chamberID string, publication Publication) error {
	before := p.store.Snapshot()
	var combined error
	for index := len(publication.Published) - 1; index >= 0; index-- {
		vesselID := publication.Published[index]
		if previous, ok := publication.Previous[vesselID]; ok {
			p.store.Set(previous.VesselID, previous.CycleID, previous.Generation, previous.Aspect, p.now())
		} else {
			p.store.Remove(vesselID)
		}
		if _, err := p.recorder.Record(ctx, chamberID, model.EventSignalChanged, publication.CycleID, publication.Generation, map[string]any{
			"vessel_id": vesselID, "compensated": true,
		}); err != nil {
			combined = errors.Join(combined, err)
			break
		}
	}
	if combined != nil {
		p.store.Restore(before)
		return errors.Join(errors.New("signal compensation failed"), combined)
	}
	return nil
}
