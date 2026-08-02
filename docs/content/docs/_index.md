---
title: "Documentation"
linkTitle: "Docs"
lead: "Everything you need to run background work inside a Tokio service — from a ten-line quick start to distributed Redis deployments."
---

KioMQ gives you three things: a **queue** to enqueue jobs, one or more **workers** to process
them concurrently, and a pluggable **store** that owns the state. Everything else — scheduling,
retries, events, metrics — is built on top of those primitives.

If you are new here, read [Getting started](getting-started/) and then
[Quick start](quick-start/). If you already have a queue running, jump straight to
[Configuration](configuration/) or the [API reference](https://docs.rs/kiomq) on docs.rs.
