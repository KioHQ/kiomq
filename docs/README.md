# KioMQ documentation site

The source for the KioMQ website, built with [Hugo](https://gohugo.io) (extended). No theme, no
submodules, no Node toolchain — everything lives in this directory.

## Local development

```bash
cd docs
hugo server
```

The site is configured for the root of `kiomq.dev`; locally, the dev server is at
**<http://localhost:1313/>** (Hugo prints the exact URL). Live reload picks up content, layout,
CSS, and JS changes.

Requires Hugo **extended**, v0.146 or newer (the layout structure uses `layouts/_partials`,
`layouts/_markup`, `layouts/_shortcodes`). Install with:

```bash
brew install hugo            # macOS
scoop install hugo-extended  # Windows
snap install hugo            # Linux
```

## Building

```bash
hugo --gc --minify
```

Output lands in `docs/public/` (git-ignored). CI does this on every push to `main` that touches
`docs/**` and deploys to GitHub Pages — see
[`.github/workflows/docs.yml`](../.github/workflows/docs.yml). The workflow passes the Pages base URL
on the command line, so deployment automatically follows the configured Pages domain.

> **First-time setup:** in the repository settings, set **Pages → Build and deployment → Source** to
> **GitHub Actions**.

## Layout

```
docs/
├── hugo.toml              site config, nav menu, brand params
├── assets/
│   ├── css/main.css       design tokens + all component styles
│   ├── css/chroma.css     syntax highlighting theme
│   └── js/site.js         theme toggle, copy buttons, tabs, TOC spy, search
├── content/
│   ├── _index.md          home page metadata (layout is layouts/home.html)
│   └── docs/*.md          one file per documentation page
├── layouts/
│   ├── baseof.html        page shell
│   ├── home.html          landing page
│   ├── page.html          documentation page
│   ├── section.html       /docs/ index
│   ├── 404.html
│   ├── home.json.json     client-side search index (/index.json)
│   ├── _partials/         head, header, footer, sidebar, toc, diagram, icons,
│   │                      github-stars (build-time star count)
│   ├── _markup/           render hooks: headings, links, tables, alerts
│   └── _shortcodes/       callout, changelog
└── static/
    ├── favicon.svg
    └── img/               logo variants + Open Graph image
```

## The changelog page

`content/docs/changelog.md` is front matter plus a single `{{</* changelog */>}}` shortcode. The
shortcode renders the repository's own [`CHANGELOG.md`](../CHANGELOG.md), which `hugo.toml` mounts
into `assets/`:

```toml
[[module.mounts]]
  source = "../CHANGELOG.md"
  target = "assets/CHANGELOG.md"
```

Each `## vX.Y.Z (date)` heading becomes a release block with its own anchor, a formatted date, a
"Latest" badge on the newest entry, and commit hashes styled as chips. Nothing needs updating here
when you cut a release — regenerate `CHANGELOG.md` as usual and rebuild.

> **Note:** declaring any mount for a component replaces that component's default mount, which is why
> `assets → assets` is re-declared alongside it.

## Header star count

The header shows a GitHub star pill. The count is fetched from the GitHub API at build time by
`layouts/_partials/github-stars.html` and cached for 24 hours (`[caches.getresource]` in
`hugo.toml`). If the request fails the pill renders without a number and the build logs a warning —
it never fails the build.

## Adding a page

Create a Markdown file under `content/docs/` with front matter:

```yaml
---
title: "Page title"
linkTitle: "Sidebar label" # optional, defaults to title
group: "Guides"            # sidebar group: Start here | Guides | Project
weight: 55                 # ordering within the sidebar, low to high
lead: "One-sentence summary rendered under the page heading."
---
```

Sidebar groups appear in the order their first page appears by weight. Prev/next links, the
"On this page" table of contents, and the search index are all generated automatically.

## Writing conventions

Callouts use GitHub alert syntax:

```markdown
> [!NOTE]
> Renders as a note callout.

> [!TIP]
> Renders as a tip callout.

> [!WARNING]
> Renders as a warning callout.

> [!CAUTION]
> Renders as a danger callout.
```

There is also a shortcode, if you need a custom title:

```markdown
{{</* callout type="warning" title="Read this first" */>}}
Body text, **Markdown** allowed.
{{</* /callout */>}}
```

Wide tables scroll horizontally rather than crushing a column, so long API tables are fine.
Code blocks get a copy button automatically — just use fenced blocks with a language tag.

## Brand assets

`static/img/` holds variants derived from [`assets/logo-dark.png`](../assets/logo-dark.png) in the
repository root:

| File | Use |
|---|---|
| `kiomq-logo.png` | Full lockup, cropped, on its original navy |
| `kiomq-logo-alpha.png` | Same lockup with the navy removed — used in the hero |
| `kiomq-mark.png` | Roadrunner only, on navy — used as the header/footer badge |
| `kiomq-mark-alpha.png` | Roadrunner only, transparent |
| `og.png` | 1200×630 social preview |

The palette in `assets/css/main.css` is sampled from the logo: navy `#1E2431`, cream `#E8ECEF`,
amber `#F5B25C`. The hero stays dark in both light and dark themes so the artwork always sits on its
own navy.
