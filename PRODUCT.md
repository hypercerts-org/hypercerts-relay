# Product
<!-- impeccable:product-schema 1 -->

## Platform
web

## Stack
TECH-588 specifies Svelte and Tailwind. The administration application is a separate Node service in `administration/`.

## Users
Authorized Hypercerts Relay operators managing source admission, collection selection, backfill and service limits.

## Product Purpose
Show what an operator requested, what each owning service applied, what failed, and what remains incomplete. Operators make changes through a durable management contract instead of database edits.

## Capabilities and Constraints
AT Protocol OAuth with explicit administrator authorization. The control plane does not own raw sockets, archive selection or backfill mechanics. Production deployment is outside TECH-588. Unknown historical PDS attribution must remain unknown.

## Accessibility & Inclusion
Keyboard-operable screens with accessible names, validation, errors and visible operational states; component and browser acceptance checks.

## Open Decisions
The user supplied the Hypercerts design repository and authorized its application to the administration UI. DESIGN.md records the operator-facing adaptation.
