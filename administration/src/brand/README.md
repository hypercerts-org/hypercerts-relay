# Hypercerts administration brand

Theme, Instrument Serif fonts, Hypercerts lockup and favicon copied from
`hypercerts-org/hypercerts-design` at
`f274ac22adf930402aebbf4c9804caa4f86f4a92`.
`theme/hypercerts.css` is the unchanged upstream Tailwind v4 theme; the app's
shared stylesheet uses its tokens. Keep `theme/` and `assets/` as siblings.
Instrument Serif is distributed under the included SIL Open Font License.

Switzer is **not included in this public repository**. `npm run build` and
`npm run dev` fetch its unmodified variable WOFF2 directly from Fontshare,
verify SHA-256 and cache it in the ignored `assets/fonts/` path. The app serves
it locally after building, without browser requests to a font CDN or changes
to the Content Security Policy. A first build needs access to
`cdn.fontshare.com`; unavailable or changed font downloads fail the build.
A valid cached font permits subsequent offline builds.

The [ITF Free Font License, version 2.0](https://www.fontshare.com/licenses/itf-ffl)
permits self-hosting for the licensee's own application (sections 01–02), but
restricts redistributing the font through public repositories. Each builder
obtains its copy from Fontshare and must follow that license. Do not include
Switzer in public source archives or publish it as a standalone asset package.

To update, read the design repository's brand rules and font licenses, copy
only the applicable distributable assets, and record its revision here.
For Switzer, obtain the variable normal-face URL from
`https://api.fontshare.com/v2/css?f[]=switzer@variable&display=swap`, verify the
new font and license, then update the source URL and SHA-256 in
`scripts/prepare-fonts.mjs`. Never update the checksum simply to bypass a
failed verification. Confirm that roman, italic and variable fonts load in
the browser after building.
