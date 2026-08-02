---
title: "Contributing"
linkTitle: "Contributing"
group: "Project"
weight: 130
lead: "Set up the repo hooks, know what CI will check, and where to ask questions."
---

KioMQ is MIT licensed and developed in the open. Issues, discussion, and pull requests are all
welcome.

## Getting set up

```bash
git clone https://github.com/KioHQ/kiomq
cd kiomq
git config core.hooksPath .githooks   # enable the repo's git hooks
cargo install cargo-nextest           # the test runner CI uses
```

The `pre-push` hook runs the same checks as CI, so failures surface before you open a pull request:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets --features tracing,redis-store -- -D warnings
cargo nextest run --locked --features tracing,redis-store
cargo test --locked --doc --features tracing,redis-store   # nextest skips doctests
cargo doc --locked --workspace --no-deps --features tracing,redis-store
```

Note that clippy runs with `-D warnings` — a lint is a build failure. Run it before you push and save
yourself a round trip.

## House style

- `rustfmt` with the repo's [`rustfmt.toml`](https://github.com/KioHQ/kiomq/blob/main/rustfmt.toml) —
  no hand formatting.
- Public items carry doc comments, with `# Errors` sections on anything fallible. `cargo doc` runs in
  CI, so broken intra-doc links fail the build.
- New public API needs a doctest. They are the examples users read first, and CI compiles them.
- Tests that touch process-global state (metrics) must tolerate running in their own process — see
  [Testing](../testing/).

## Documentation

This site lives in [`docs/`](https://github.com/KioHQ/kiomq/tree/main/docs) and is built with
[Hugo](https://gohugo.io) (extended). To work on it:

```bash
cd docs
hugo server        # http://localhost:1313
```

Pages are Markdown under `docs/content/docs/`. Front matter controls placement:

```yaml
---
title: "Page title"
linkTitle: "Sidebar label"   # optional, defaults to title
group: "Guides"              # sidebar grouping
weight: 55                   # ordering, low to high
lead: "One-sentence summary under the page heading."
---
```

`group` values appear in the sidebar in the order their first page appears by weight — currently
**Start here**, **Guides**, **Project**. Every page also gets an "Edit this page on GitHub" link, so
small fixes need no local setup at all.

GitHub-style alerts render as callouts:

```markdown
> [!NOTE]
> Rendered as a note callout.

> [!WARNING]
> Rendered as a warning callout.
```

## Pull requests

- Branch from `main`, keep the change focused, and describe the *why* in the description.
- Reference the issue it closes, if there is one.
- Public API changes should update the docs in the same pull request — both the rustdoc and, where
  relevant, the pages on this site.
- CI runs on `pull_request`; a green run plus a review is the merge bar.

## Where to ask

- **[GitHub issues](https://github.com/KioHQ/kiomq/issues)** — bugs and feature requests.
- **[Discord](https://discord.gg/Y6Vy2k9Rf)** — questions, design discussion, and everything in
  between.

## Licence

MIT — see [LICENSE](https://github.com/KioHQ/kiomq/blob/main/LICENSE). Contributions are accepted
under the same terms; there is no CLA.
