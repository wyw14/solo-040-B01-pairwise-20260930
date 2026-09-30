package schedule

import (
	"context"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

type Cancellation struct {
	Plan         model.ConvoyPlan `json:"plan"`
	RemovedQueue int              `json:"removed_queue"`
	Compensated  int              `json:"compensated"`
}

// Cancel reverts a convoy plan's queue membership and field signals as one
// observable operation. It is a compensating transaction:
//
//   - The plan's vessels leave the chamber waiting queue first, so a cancelled
//     vessel can never be observed at the queue head and admitted again.
//   - Signal compensation is atomic. On failure nothing changes: the vessels
//     are restored to their original queue positions and the plan stays active,
//     leaving the caller free to retry the identical cancellation.
//   - The cancellation is only marked once the audit event is durable. If that
//     final record fails, both the plan flag and the queue are rolled back.
//
// Cancelling an already-cancelled plan is a successful no-op.
func (p *Planner) Cancel(ctx context.Context, planID uuid.UUID, note string, publisher *signal.Publisher, publication signal.Publication) (Cancellation, error) {
	// Serialize cancellations: two concurrent calls must not both observe the
	// plan as active and double-compensate its signals or requeue its vessels.
	p.cancelling.Lock()
	defer p.cancelling.Unlock()

	plan, err := p.require(planID)
	if err != nil {
		return Cancellation{}, err
	}
	if plan.Cancelled {
		return Cancellation{Plan: plan}, nil
	}

	chamberID := plan.ChamberID
	removed := p.admission.RemovePlan(chamberID, plan)

	if err := publisher.Compensate(context.WithoutCancel(ctx), chamberID, publication); err != nil {
		// Signals were not changed; restore the queue and keep the plan active
		// so the cancellation can be retried against the original state.
		p.admission.RestorePlan(chamberID, removed)
		return Cancellation{}, err
	}

	plan.Cancelled = true
	plan.CancelNote = note
	p.replace(plan)

	if _, err := p.recorder.Record(context.WithoutCancel(ctx), chamberID, model.EventPlanCancelled, plan.ID, 0, map[string]any{
		"note": note, "removed_queue": len(removed), "compensated_signals": len(publication.Published),
	}); err != nil {
		// Audit record lost: undo every observable effect and let the caller retry.
		plan.Cancelled = false
		plan.CancelNote = ""
		p.replace(plan)
		p.admission.RestorePlan(chamberID, removed)
		return Cancellation{}, err
	}

	return Cancellation{
		Plan: plan, RemovedQueue: len(removed), Compensated: len(publication.Published),
	}, nil
}
