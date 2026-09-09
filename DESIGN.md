# Administration visual system

The administration UI follows the Hypercerts brand from
`hypercerts-org/hypercerts-design`, pinned in
`administration/src/brand/README.md`. This replaces the provisional green
operations interface. The product remains an operator tool: preserve routes,
forms, status meanings, accessible names and durable command behavior.

- Instrument Serif for page and section headings, with a roman/italic turn.
  Switzer for structure, navigation, forms, identity and data.
- Cream main surface; white sidebar and detail panels; grey table headings
  and selected navigation. Black controls and headings, grey body text,
  rust accent for heading emphasis, focus and failure states. Sage is the
  single optional status tint. States always retain explicit text labels.
- Use the upstream theme's type, color and radius tokens. Page titles use
  display-3 (48px, line height 1.1); panel headings use heading-4 (24px,
  line height 1.17). Compact operator text uses body-sm (14px); descriptions
  use body-lg (18px). Both heading levels use weight 400 and -0.02em tracking;
  body copy uses line height 1.5 and a maximum measure of 72ch. Keep tables
  horizontally scrollable.
- Page introductions carry a meaningful tracked eyebrow. Preserve concise
  task names and data labels; do not introduce marketing copy into controls.
  The website's generous section spacing is adapted to dense operator tasks.
- Use the supplied horizontal Hypercerts logo unchanged, at least 24px high,
  with clear space. No illustration is needed in the operational chrome.
- Desktop: sticky sidebar, independently scrolling navigation above, signed-in
  account fixed within the sidebar at the bottom left. Show the stored display
  name in bold black (18px, weight 700) and handle below in grey (14px).
  If the name is absent use the handle; if both are absent use the DID. When
  only the name is available, show the DID below it. At widths up to 640px,
  navigation scrolls horizontally and account information stays visible in
  the normal page flow.
- Keyboard focus, error recovery, loading/disabled states, reduced motion,
  desktop/mobile accessibility and overflow checks remain part of delivery.

Names and handles are presentation metadata, refreshed by OAuth sign-in.
Authorization and administrator changes always use the authenticated DID.

Brand assets and their pinned source revision are documented in
`administration/src/brand/README.md`. Instrument Serif ships with its SIL Open
Font License. Switzer is downloaded from Fontshare and checksum-verified during
development/build preparation, then served locally; its license does not permit
including the font in this public source repository. Keep that distinction when
updating assets and confirm all three font faces load after building.
