# Hypercerts administration brand

Theme, Instrument Serif fonts, Hypercerts lockup and favicon copied from
`hypercerts-org/hypercerts-design` at
`f274ac22adf930402aebbf4c9804caa4f86f4a92`.
`theme/hypercerts.css` is the upstream Tailwind v4 theme; the app's shared
stylesheet uses its tokens. Keep `theme/` and `assets/` as siblings.
Instrument Serif is distributed under the SIL Open Font License in
`assets/fonts/OFL-InstrumentSerif.txt`.

The body font is Hanken Grotesk: the unmodified
`fonts/webfonts/HankenGrotesk[wght].woff2` from
[`marcologous/hanken-grotesk`](https://github.com/marcologous/hanken-grotesk)
at `eff37d18946b018ad239cf5fd3992db5d19b82a0`, committed as
`assets/fonts/HankenGrotesk-Variable.woff2`. It is distributed under the SIL
Open Font License in `assets/fonts/OFL-HankenGrotesk.txt`. `theme/hypercerts.css`
differs from upstream only in naming Hanken Grotesk as the body face; re-pin it
to the design repository once hypercerts-org/hypercerts-design#19, which makes
the same change there, is merged. The app serves both fonts locally, without
browser requests to a font CDN.

To update, read the design repository's brand rules and font licenses, copy
only the applicable distributable assets, and record its revision here.
Confirm that roman, italic and variable fonts load in the browser after
building.
