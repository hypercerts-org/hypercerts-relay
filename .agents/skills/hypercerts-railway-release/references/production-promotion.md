# Production promotion

Only a merged pull request from `dev` into `production` can trigger the public
promotion workflow. It first proves that the merge tree is byte-for-byte the
staged candidate tree. If the merge resolution changes the tree, stop: build and
validate that exact result as a new staging candidate instead of rebuilding during
production deployment.

The workflow dispatches `hypercerts-promote-production` with the candidate and
promotion revisions. The private infrastructure receiver must:

1. find the recorded successful staging receipt for the candidate revision;
2. reject the event if any component digest, staging deployment, or required
   validation is missing;
3. apply those exact `image@sha256:...` references to production; and
4. wait for each submitted Railway deployment to reach `SUCCESS`.

Production uses its own variables, PostgreSQL/volumes, domains, and approved
credentials. The image is the same as staging, but state is never copied or
shared. Treat migrations as a separately reviewed compatibility concern.

Production's GitHub Environment supplies its own `ADMIN_PUBLIC_ORIGIN` and
optional `RELAY_PUBLIC_ORIGIN`, `RAINBOW_PUBLIC_ORIGIN`, and
`JETSTREAM_PUBLIC_ORIGIN`. Pass them to the private receiver with the promotion
event, and set them only on the Administration service. The optional values are
overview links, not private control endpoints or evidence that Rainbow is running.

The production GitHub Environment must restrict deployments to the `production`
branch and require the intended reviewers before its `INFRA_DISPATCH_TOKEN` is
released. Do not enable a second Railway GitHub auto-deploy path for these
services, and do not promote mutable image tags.
