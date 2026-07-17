# Shardscope planning artifacts

The docs behind Shardscope, produced by the gstack planning pipeline
(office-hours → eng-review → ceo-review → design-consultation) and checked in here
so they survive independently of any local machine.

| File | What it is |
|------|-----------|
| [`design-doc.md`](./design-doc.md) | The office-hours design doc: problem, wedge, the four-room product, and the folded-in **Engineering Review Addendum** (architecture decisions, corrections, task list) + **CEO Review Addendum** (scope expansion) + the review report. The single source of truth for what Shardscope is and why. |
| [`ceo-plan.md`](./ceo-plan.md) | The CEO/scope-expansion plan: the wedge verdict and the four accepted expansions (chaos mode, incumbent contrast, edge map, deploy-your-own) with phasing. |
| [`eng-review-test-plan.md`](./eng-review-test-plan.md) | The eng-review test plan: affected routes, interactions, edge cases, and the critical paths (incl. the injected-loss-goes-red guard). |
| [`mockup-topology-hero.html`](./mockup-topology-hero.html) | The static design mockup of the Topology + Reshard hero screen (the visual target the live SPA in `../public/` realizes). Open in a browser; it's animated. |

The design system itself is [`../DESIGN.md`](../DESIGN.md).
