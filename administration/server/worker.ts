import { Store } from "./store.ts";
import { Services } from "./services.ts";
import { ApiError } from "./contracts.ts";

// One worker per database. Never infer archive completion from command acceptance.
export class Worker {
  private busy = false;
  constructor(
    private readonly store: Store,
    private readonly services: Services,
  ) {}
  recover() {
    const pending = this.store.db
      .prepare("SELECT id FROM operations WHERE state='applying'")
      .all();
    for (const row of pending)
      this.store.transition(
        row.id as string,
        "requested",
        this.store.operation(row.id as string)?.result,
        "resuming_after_restart",
        undefined,
        ["applying"],
      );
  }
  async tick() {
    if (this.busy) return;
    this.busy = true;
    try {
      const row = this.store.db
        .prepare(
          "SELECT id FROM operations WHERE state='requested' ORDER BY createdAt,id LIMIT 1",
        )
        .get();
      if (!row) return;
      const op = this.store.operation(row.id as string)!;
      if (
        !this.store.transition(op.id, "applying", op.result, null, undefined, [
          "requested",
        ])
      )
        return;
      try {
        const result = await this.services.apply(op.command, op.id, (value) => {
          this.store.transition(op.id, "applying", value, null, undefined, [
            "applying",
          ]);
        });
        this.store.transition(op.id, "applied", result, null, undefined, [
          "applying",
        ]);
      } catch (e) {
        this.store.transition(
          op.id,
          e instanceof ApiError && (e.status >= 500 || e.status === 422)
            ? "incomplete"
            : "failed",
          this.store.operation(op.id)?.result,
          e instanceof ApiError ? e.code : "application_failed",
          undefined,
          ["applying"],
        );
      }
    } finally {
      this.busy = false;
    }
  }
}
