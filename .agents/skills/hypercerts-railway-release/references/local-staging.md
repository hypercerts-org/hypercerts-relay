# Local staging deployment

"Local deployment to staging" means uploading or selecting a candidate from an
operator workstation for the existing **staging** Railway environment. It is an
external mutation, not a local Docker test. Require explicit authorization for
the exact Railway project, staging environment, and services before running it.

Prefer the normal candidate pipeline. A local staging action is for an approved
recovery or controlled operator test and must use the same immutable release
manifest (`relay`, `jetstream`, and `administration` digest references) as CI.
Never deploy the working tree to production and never use `latest`, branch tags,
or a locally rebuilt image as a substitute for a staged candidate.

Before acting, inspect the selected project/environment/service context and the
current deployment IDs. Confirm the operation targets staging, uses separate
staging volumes and variables, and cannot modify production state. Deploy each
service through the private infrastructure workflow or its reviewed Railway IaC;
do not add `.railway` files to this public checkout.

Afterward, inspect each submitted deployment by ID until it is `SUCCESS`, then
run the applicable staging checks. If any deployment is failed, crashed, waiting
for approval, or otherwise nonterminal, report that exact state and do not claim
the local deployment succeeded.
