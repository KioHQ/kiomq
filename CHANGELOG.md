# Changelog

All notable changes to this project will be documented in this file.
## Unreleased



### CI

- use --offline instead of --locked for tests in ci by @spencerjibz ([54247c5](https://github.com/KioHQ/kiomq/commit/54247c52cb049efdac8d15d4441f27b4ad85241b))


### Chores

- revert usage of --offline flag in ci by @spencerjibz ([120baba](https://github.com/KioHQ/kiomq/commit/120babaf345fbdd75161647e716b895fd9b67153))


### Release

- 0.1.1 by @spencerjibz ([#63](https://github.com/KioHQ/kiomq/pull/63)) _(release)_



### Contributors

- @spencerjibz
## v0.1.1 (2026-03-07)



### Bug Fixes

- lint warning about links by @spencerjibz ([916016b](https://github.com/KioHQ/kiomq/commit/916016b10afe2adf41196afdc0f461b6ea1f75fd)) _(rustdoc)_


### CI

- exclude unnecessary folders and files from cargo publish by @spencerjibz ([9c88c32](https://github.com/KioHQ/kiomq/commit/9c88c323dfb6e561ae8c744b21736a9163bd1fe9)) _(Crates.io)_

- workflow should references to the old library version in the readme by @spencerjibz ([#62](https://github.com/KioHQ/kiomq/pull/62)) _(cd-workflow)_


### Chores

- clippy fix (unused) by @spencerjibz ([68f67e5](https://github.com/KioHQ/kiomq/commit/68f67e5ca787b7f52dcd34b7e1f578ebee72fdc1))


### Documentation

- add ci-build, crates.io, docs.rs & licence badges  to readme by @spencerjibz ([55a4caa](https://github.com/KioHQ/kiomq/commit/55a4caaf0b0ec4ac349977c2e2e93738bba3aa57)) _(readme)_

- update code snippets to include waiting for job completion by @spencerjibz ([770f85c](https://github.com/KioHQ/kiomq/commit/770f85cf3c53eb8ed13c29d03b516fa7c48e6bb4)) _(readme)_

- add example code-snippet of redis and rocksdb usage by @spencerjibz ([0d45ff9](https://github.com/KioHQ/kiomq/commit/0d45ff900e7e62fdd711b2082085fc5a709bb50e)) _(readme)_

- use readme in lib.rs, correct links in docs by @spencerjibz ([25ba74b](https://github.com/KioHQ/kiomq/commit/25ba74bc5dc698915aa1accde790ffba6fa0dd70))

- minor tweak in rocksdb usage example by @spencerjibz ([3ce01a7](https://github.com/KioHQ/kiomq/commit/3ce01a799ef0f6abdb23340f80e6cba4cde4bcbc)) _(readme)_


### Release

- 0.1.0 by @spencerjibz ([#61](https://github.com/KioHQ/kiomq/pull/61)) _(release)_



### Contributors

- @spencerjibz
## v0.1.0 (2026-03-07)



### Bug Fixes

- fix support for job prioritization by @spencerjibz ([8e514e2](https://github.com/KioHQ/kiomq/commit/8e514e24087b9d59c4ea82dfdd64ea36d65b26a1))

- fix job retrial after failure by @spencerjibz ([#11](https://github.com/KioHQ/kiomq/pull/11))

- fix retrials & remove crossbeam-skiplist by @spencerjibz ([800fb39](https://github.com/KioHQ/kiomq/commit/800fb39b61efb50a8308e23cad3667cd1d5aefdd))

- failing missed deadline jobs by @spencerjibz ([8595655](https://github.com/KioHQ/kiomq/commit/8595655da331a303dc78eeec0833752ed44690ad))

- fix serializing redis-stream-event by @spencerjibz ([71c0ccd](https://github.com/KioHQ/kiomq/commit/71c0ccd4e0e119675428c90d48b370607980a5a1))

- skipmap.len is just an len appromixation, use atomic counter for this instead by @spencerjibz ([#14](https://github.com/KioHQ/kiomq/pull/14))

- fix flaky test by @spencerjibz ([#15](https://github.com/KioHQ/kiomq/pull/15))

- correctly serialize queue-events by @spencerjibz ([f09e065](https://github.com/KioHQ/kiomq/commit/f09e065af8c5e7386936e7a34e4aa75cfae7fef4))

- promoting delayed jobs by @spencerjibz ([0d36fa8](https://github.com/KioHQ/kiomq/commit/0d36fa86b51d225e2acc07c56575de17df332659))

- fix complete rocksdb-store::add_bulk methods by @spencerjibz ([01b2f3d](https://github.com/KioHQ/kiomq/commit/01b2f3db0f956339e74eca2c4c19899e3c800d67))

- counter bugs in in-memory and rocksdb store by @spencerjibz ([85c20ed](https://github.com/KioHQ/kiomq/commit/85c20ed886e48b65b21bf91f3c5ea4dce574b666))

- fix overflow and simple clean up by @spencerjibz ([234f7f5](https://github.com/KioHQ/kiomq/commit/234f7f50287c4f0da6fd067e47e40ec8059e363e))

- fix out of range errors by @spencerjibz ([#17](https://github.com/KioHQ/kiomq/pull/17))

- fix typo by @spencerjibz ([b0852f4](https://github.com/KioHQ/kiomq/commit/b0852f43f7e17fb3112feb8dc13d894e2baf21f0))

- bug fix by @spencerjibz ([5f08980](https://github.com/KioHQ/kiomq/commit/5f089803a42d91f7d7d7f1a4386ba7875cc4c89b))

- correct naming of metrics by @spencerjibz ([9b1e9c7](https://github.com/KioHQ/kiomq/commit/9b1e9c7615474ead4680bb2258b4313a8900f1c3))

- bug fixes by @spencerjibz ([2fd910f](https://github.com/KioHQ/kiomq/commit/2fd910f68e82f504d8f6cf81fb4d56d7bf9c5f9b))

- set return value & add timeout to listening to events by @spencerjibz ([e71a2d3](https://github.com/KioHQ/kiomq/commit/e71a2d30fcddb1f5cdd55766c9054c4c4f56152e))

- timedmap tests by @spencerjibz ([152e2f5](https://github.com/KioHQ/kiomq/commit/152e2f52606fceb6027648737a69faeafa1f94da))

- bug fix: locks.len_exnpire is async by @spencerjibz ([c71823e](https://github.com/KioHQ/kiomq/commit/c71823e317cf72e068704a107a74a8d79a9eb824))

- inmemory-store, only update stored-metrics from publish_event by @spencerjibz ([6ff80a4](https://github.com/KioHQ/kiomq/commit/6ff80a40600674171c3f27cd8d844314243047a4))

- updating prioritized and failed job counts in queue-metrics by @spencerjibz ([cdd2726](https://github.com/KioHQ/kiomq/commit/cdd272658707421298f6b14dcb06cc7752ffcc43))

- pass the right attempt for failed jobs by @spencerjibz ([e672423](https://github.com/KioHQ/kiomq/commit/e6724232f4fe76fbb0ad4b6dc133dca8338c5e46))

- redis-store returns pause_count instead of failed_job cout by @spencerjibz ([0f027d5](https://github.com/KioHQ/kiomq/commit/0f027d5c10d17e5516b25277f54933fc00ee3e54))

- typo in fetch_worker_metrics in redis-store by @spencerjibz ([9d17207](https://github.com/KioHQ/kiomq/commit/9d17207c23648028cde030f86c10bdd273029fd6))

- cargo fmt by @spencerjibz ([3ab26ed](https://github.com/KioHQ/kiomq/commit/3ab26ed59a9d952aa8fb0aa44b7df35853f062dc))

- fetching_metrics from redis-store by @spencerjibz ([0173d57](https://github.com/KioHQ/kiomq/commit/0173d57dc9f6c7957d1d46f1ea7c20f65b87b427))

- fix flawky tests by @spencerjibz ([9e84dee](https://github.com/KioHQ/kiomq/commit/9e84dee51732ecf47db1942dbe424ac60ae15daf))

- fix tests by @spencerjibz ([#49](https://github.com/KioHQ/kiomq/pull/49))

- tracing::Instrument::instrument is ambigious to TaskMonitor.instrument by @spencerjibz ([6e6f342](https://github.com/KioHQ/kiomq/commit/6e6f3422fbf3cd8eaaf589cba06ed7e1fa1a86e6)) _(method-ambiguity)_

- stop relying on timestamps to generate release.md by @spencerjibz ([3b57340](https://github.com/KioHQ/kiomq/commit/3b573406679d7bc47746f2ac664e642638f8fed7)) _(ci)_


### CI

- add ci tests by @spencerjibz ([04c4110](https://github.com/KioHQ/kiomq/commit/04c41100e364163b23a1c1acab8d366d1c4973ec))

- Merge pull request #2 from spencerjibz/add-ci by @spencerjibz ([#2](https://github.com/KioHQ/kiomq/pull/2))

- re-export both backtrace tracing macros by @spencerjibz ([3d797ca](https://github.com/KioHQ/kiomq/commit/3d797caab91da8256332e578038f84119b42639b))

- pass down removal options from queue if not specified for a job by @spencerjibz ([be9473f](https://github.com/KioHQ/kiomq/commit/be9473face1d4f14dca07a3027885fc058ef4935))

- efficiently pausing workers with global activity count, let workers finished all their work before pausing by @spencerjibz ([#12](https://github.com/KioHQ/kiomq/pull/12))

- batch pipeline commands by @spencerjibz ([fac9673](https://github.com/KioHQ/kiomq/commit/fac9673ecfce13d063b5f8830b0f4ac1845ef1a4))

- format spacing in readme by @spencerjibz ([79cc9ac](https://github.com/KioHQ/kiomq/commit/79cc9acd7a335992f5c6a872f3c05de78b3c8143))

- fix and improve ci with caching & using nextest by @spencerjibz ([c696b5c](https://github.com/KioHQ/kiomq/commit/c696b5ce1780c70d1e0be0f72cb3765506c15b80))

- don't use release flag in ci by @spencerjibz ([740cdda](https://github.com/KioHQ/kiomq/commit/740cdda967075c814d55733ce6e8815f58d16468))

- use keydb and improved workflow by @spencerjibz ([62e06c4](https://github.com/KioHQ/kiomq/commit/62e06c4d62775a6ca74a01affdbfccad08eb3d43))

- use redis instead of keydb in ci by @spencerjibz ([4bfbd33](https://github.com/KioHQ/kiomq/commit/4bfbd339d0f3d4c5fa9453b7f6d19ecca8ff675d))

- fix ci by @spencerjibz ([d671d70](https://github.com/KioHQ/kiomq/commit/d671d70d830ca4fe1e7e0126a9431c28a275c398))

- add the tracing crate by @spencerjibz ([26b15a0](https://github.com/KioHQ/kiomq/commit/26b15a0e5886075d823cdca99a3a78021401f62c))

- add tracing and tracing-subscriber to examples by @spencerjibz ([67196ab](https://github.com/KioHQ/kiomq/commit/67196aba48084adc5d6b31e9c95d77378a1810da))

- clean and add tracing to examples by @spencerjibz ([aec1a40](https://github.com/KioHQ/kiomq/commit/aec1a40b61b531e17d7140d13c273314798a2e9b))

- gate tracing under a feature flag and make dep optional by @spencerjibz ([#34](https://github.com/KioHQ/kiomq/pull/34))

- Apply code review feedback: remove explicit type in loop, drop unused return value assignment by @Copilot ([223ecab](https://github.com/KioHQ/kiomq/commit/223ecab2257574aff45ec1ac717aa7e740966ded))

- Migrate workflows to Blacksmith by @blacksmith-sh[bot] ([18f8dc2](https://github.com/KioHQ/kiomq/commit/18f8dc2ce56fc8bf739bebb7a0add7a3918e617a))

- run store tests in concurreny in ci by @spencerjibz ([1386a80](https://github.com/KioHQ/kiomq/commit/1386a802468fa87090c9f505cc5885673414e08e))

- add a release-workflow by @spencerjibz ([aab3e02](https://github.com/KioHQ/kiomq/commit/aab3e02b40bd704c3797c380fc5620482ea28955))

- populated CD workflow file by @spencerjibz ([#51](https://github.com/KioHQ/kiomq/pull/51))

- add create generation to workflow by @spencerjibz ([7b85613](https://github.com/KioHQ/kiomq/commit/7b856136c8bcfbbc6dfa850860fd02e7d36f3310)) _(cd-workflow)_

- Fix remaining clippy errors and incorporate release-pipeline lint relaxation by @Copilot ([652c1b6](https://github.com/KioHQ/kiomq/commit/652c1b6d88e4438ccd7afb63ab637289839b0827))

- fix cd pipeline by @spencerjibz ([9da2cb8](https://github.com/KioHQ/kiomq/commit/9da2cb8ef06ab936e7458660e58a322ba4f1ad28))

- add rocks build time system dependencies for publishing job by @spencerjibz ([73c036f](https://github.com/KioHQ/kiomq/commit/73c036f0f5b1619dcb68e933154f23d5a2c2c1ca)) _(cd-pipeline)_

- chose better category slugs for publishing crate by @spencerjibz ([a7a8478](https://github.com/KioHQ/kiomq/commit/a7a847841fcb0785f6757f8b075ecb6f78c1d75d)) _(cd)_


### Chores

- cargo fmt by @spencerjibz ([a7eb821](https://github.com/KioHQ/kiomq/commit/a7eb8211fdfcb66e129dde991afe5662a33b08be))

- add changelong with git-cliff by @spencerjibz ([165824e](https://github.com/KioHQ/kiomq/commit/165824e6ef7485bbe9dde56bd2fe4185fff8141b))

- add author, description, links and licence metadata by @spencerjibz ([09dfba9](https://github.com/KioHQ/kiomq/commit/09dfba97a0cdd7f0bde2302e13e48c19b3b280bf)) _(cargo.toml)_

- add best practice library clint protections by @spencerjibz ([f28968c](https://github.com/KioHQ/kiomq/commit/f28968cfc78f1d5a8c5da4b68ff2c26e2f183511))

- unused/dead_code elimination by @spencerjibz ([dac2919](https://github.com/KioHQ/kiomq/commit/dac2919540958f7e11310bdf1f1a74a225c95995))

- add missing debug implementations by @spencerjibz ([6015372](https://github.com/KioHQ/kiomq/commit/6015372f105c8ff4b78eb5de348c2b1dccf109ae))

- only expose few methods publicity & tracing vital calls by @spencerjibz ([2b79b7c](https://github.com/KioHQ/kiomq/commit/2b79b7cb48f5dbed997da908e6d602f2ee9a9578)) _(queue)_

- add more tracing spans by @spencerjibz ([fa75a53](https://github.com/KioHQ/kiomq/commit/fa75a53365b629caf6fdca1417ce9176e45d8cbe)) _(tracing)_

- manual clippy --fix by @spencerjibz ([31456a6](https://github.com/KioHQ/kiomq/commit/31456a6d4d353d6b888f29a2f78a19a5bf46d5fe)) _(clippy)_

- more docs in worker_opts by @spencerjibz ([d561826](https://github.com/KioHQ/kiomq/commit/d5618261f095ff1c5355bc72f1cb8a03da478f42))

- relax some lint constricts by @spencerjibz ([0967db7](https://github.com/KioHQ/kiomq/commit/0967db795f046d34f70e1c349dab4286d440c389))

- fix lint warnings in tests by @spencerjibz ([24dd862](https://github.com/KioHQ/kiomq/commit/24dd862eead2ca49a8cacb7d05af6297d3ef750a))

- add clippy:nursery recommendation & fmt by @spencerjibz ([#52](https://github.com/KioHQ/kiomq/pull/52)) _(clippy)_

- use the right branch for releases by @spencerjibz ([f4f4c63](https://github.com/KioHQ/kiomq/commit/f4f4c63b509edf4238c7958e3a3d12c8dc54aad3)) _(cd)_

- use real sermver version of crates by @spencerjibz ([a6e8f58](https://github.com/KioHQ/kiomq/commit/a6e8f5847f7bb26e805a4cf812082b6438737066)) _(cargo)_

- use real sermver version of crates by @spencerjibz ([5c96437](https://github.com/KioHQ/kiomq/commit/5c96437bd46317bbb7af6a4bb608200f6bb9e539)) _(cargo)_


### Documentation

- add readme by @spencerjibz ([7458a6a](https://github.com/KioHQ/kiomq/commit/7458a6a0b784d1eab39ef0da31edba0916087380))

- update readme by @spencerjibz ([4ea9557](https://github.com/KioHQ/kiomq/commit/4ea95576f5a74b2ede636c4c25a191a0c54aa9df))

- Update README.md by @spencerjibz ([69c92b5](https://github.com/KioHQ/kiomq/commit/69c92b5aff4628353b59b8c84cd2c1933963ac3b))

- Update README.md by @spencerjibz ([2c1749a](https://github.com/KioHQ/kiomq/commit/2c1749af1ad00a02329a6e7872fbe0da6df60bc2))

- update readme to reflect current developement progress by @spencerjibz ([d19d9cc](https://github.com/KioHQ/kiomq/commit/d19d9cc15b9640b28fc3c4dbee5618aedfb81c59))

- Update README.md for clarity and formatting by @spencerjibz ([9753782](https://github.com/KioHQ/kiomq/commit/975378240136b6fdc44c774f5c4cbc5476529bb0))

- update Readme by @spencerjibz ([f583669](https://github.com/KioHQ/kiomq/commit/f5836691ad49548a13621cc6cb8e19b6faa7b507))

- add extras to readme by @spencerjibz ([#45](https://github.com/KioHQ/kiomq/pull/45))

- Add logo and project overview to README by @spencerjibz ([6e40043](https://github.com/KioHQ/kiomq/commit/6e40043bbd61bd757bd7557fed656e607660777b))

- Add comprehensive Rustdoc documentation across the kiomq codebase by @Copilot ([a62c4b7](https://github.com/KioHQ/kiomq/commit/a62c4b7e2333c291bb60ae7bd3e1a768127c0c45))

- Add Rustdoc for RedisStore struct, fields, and public methods; doc fetch_redis_pass and get_queue_metrics by @Copilot ([b9c4640](https://github.com/KioHQ/kiomq/commit/b9c4640d5baffa74b6469ed0b3924eb7094478d6))

- Fix updating_metrics bug in README examples and add panics/framed section by @Copilot ([decbf13](https://github.com/KioHQ/kiomq/commit/decbf13a49993061803aac4670eed58f748337ca))

- add doc comment and example to the framed re-export in lib.rs by @Copilot ([7c1eb18](https://github.com/KioHQ/kiomq/commit/7c1eb18f799c1ee329b18d1263222822614722b8))

- condense README to match lib.rs conciseness (44% shorter) by @Copilot ([4fbc363](https://github.com/KioHQ/kiomq/commit/4fbc36347436c0bcb581ef2284620d7d1a05d030))

- restore Progress updates section and improve Panics & errors wording by @Copilot ([71c7dc7](https://github.com/KioHQ/kiomq/commit/71c7dc7569bdbc8360f3bc461170c85cd77612bb))

- add compact table of contents to README by @Copilot ([#50](https://github.com/KioHQ/kiomq/pull/50))

- Fix clippy warnings: visibility, casts, docs, match arms, async, and more by @Copilot ([db811ff](https://github.com/KioHQ/kiomq/commit/db811ff53f93256270d894b10e4161f735120a60))

- typo by @spencerjibz ([d221802](https://github.com/KioHQ/kiomq/commit/d221802c14cb494da1afa1ab1e786129c9c59684))


### Features

- add basic required deps by @spencerjibz ([3405f0a](https://github.com/KioHQ/kiomq/commit/3405f0a26b87c07ab8e102383302496a159b8cdc))

- add simple queue impl with a job queue too by @spencerjibz ([7ee4ec8](https://github.com/KioHQ/kiomq/commit/7ee4ec852d3575f54a154db188be45c5ecdb9e8f))

- add job progress updating by @spencerjibz ([f64948e](https://github.com/KioHQ/kiomq/commit/f64948eb8aa89d6fafc9dc2077fb1e8e4d06ba75))

- add base worker by @spencerjibz ([db9b270](https://github.com/KioHQ/kiomq/commit/db9b270cf5a02d054971e03d6dfa0e0fb4516c2c))

- add timer implementation by @spencerjibz ([34188fa](https://github.com/KioHQ/kiomq/commit/34188fa9c49bd904a53f2dc0f64c32356c30ad43))

- add worker options by @spencerjibz ([7519a91](https://github.com/KioHQ/kiomq/commit/7519a91256dcc5920251d0a07d3546ea10035564))

- add backtrace_capture utils by @spencerjibz ([8626a5a](https://github.com/KioHQ/kiomq/commit/8626a5a3f7950f897f33669df514f10e33a1d915))

- add more utils to wokrer by @spencerjibz ([694ecf3](https://github.com/KioHQ/kiomq/commit/694ecf3ba8f91526d729a11568e8a8445c150d5c))

- add timer queue functions by @spencerjibz ([cbed76e](https://github.com/KioHQ/kiomq/commit/cbed76ebc1d877b7c3ebedf772f255110f560267))

- add timers to worker by @spencerjibz ([9121195](https://github.com/KioHQ/kiomq/commit/9121195f45478183d2436abf06d2ff7a0d09da46))

- add support for storing stacktraces and errors separately by @spencerjibz ([6677dd6](https://github.com/KioHQ/kiomq/commit/6677dd6821346d7fe5d0e4e706fa3cc0ec9b56e2))

- add events to queue & worker by @spencerjibz ([#4](https://github.com/KioHQ/kiomq/pull/4))

- support queue-obliteration by @spencerjibz ([c158955](https://github.com/KioHQ/kiomq/commit/c15895571f7923ed60973f1ceebb84e65289ea18))

- add backoff utils by @spencerjibz ([dabd8f1](https://github.com/KioHQ/kiomq/commit/dabd8f13ee23fe33b714f3002d8c6d2375b21c70))

- add joinError to KioError type by @spencerjibz ([e09d35b](https://github.com/KioHQ/kiomq/commit/e09d35be247edfc10b1eb7246c3c976efc945a4c))

- add working video_transcoding-example by @spencerjibz ([9dd62c4](https://github.com/KioHQ/kiomq/commit/9dd62c4075b735cd89b3b92e6660c2ccea566235))

- add job options with delay by @spencerjibz ([d90d537](https://github.com/KioHQ/kiomq/commit/d90d537178bacb90b5c34ce921a1d6c924ccd1d7))

- add priority to job_data and priority helpers by @spencerjibz ([922e358](https://github.com/KioHQ/kiomq/commit/922e358bed10e265313c66daef1b2d8fa53f7ed6))

- enable scheduling without a seperate timer by @spencerjibz ([8aa3e2c](https://github.com/KioHQ/kiomq/commit/8aa3e2cc3c02389cdce81dc60f9fa8c011c99cbf))

- add the right job status for delayed jobs by @spencerjibz ([#9](https://github.com/KioHQ/kiomq/pull/9))

- add delay field to event_stream by @spencerjibz ([b73346e](https://github.com/KioHQ/kiomq/commit/b73346ee0ce83b90dc87e2debe1df8164b884d7d))

- add and move events into their own module by @spencerjibz ([75d93f1](https://github.com/KioHQ/kiomq/commit/75d93f1e3654e5d837f54ae19baca588c21c80c0))

- add stream-listener-task by @spencerjibz ([1474263](https://github.com/KioHQ/kiomq/commit/1474263cff8a28ab3032422f058475ac4ae33187))

- add queue-stream-event by @spencerjibz ([6ab4b9a](https://github.com/KioHQ/kiomq/commit/6ab4b9a3f0d0e50d518e6b6c2b6c247ce6fffcc3))

- add job-removal options by @spencerjibz ([b2d6bb2](https://github.com/KioHQ/kiomq/commit/b2d6bb20d39b97400998f69f2a3a34cdf4ff58d8))

- add tokio-console by @spencerjibz ([65e8fb1](https://github.com/KioHQ/kiomq/commit/65e8fb1134f7d355b743b18e3e7443842b3b2009))

- add queue options to example by @spencerjibz ([839faac](https://github.com/KioHQ/kiomq/commit/839faac9a1591fe6c5ba8507ce4340b8ad834271))

- add support for bulk_add by @spencerjibz ([4840b76](https://github.com/KioHQ/kiomq/commit/4840b76b2bbb4c2e5677e7e92804c44780447f52))

- add task-notifiers & waiting-job-count to metrics by @spencerjibz ([edcf098](https://github.com/KioHQ/kiomq/commit/edcf098e5fb688f05b5476cf983252f93b0829e7))

- add methods to bulk add jobs with returning them by @spencerjibz ([a1b9d67](https://github.com/KioHQ/kiomq/commit/a1b9d6754fe442c35cf85dddf287a443d2ad2f62))

- add proesssing to JobState and clean up events by @spencerjibz ([855f64c](https://github.com/KioHQ/kiomq/commit/855f64cb6f7e8becd5c4da4bbcba0f29b9321c30))

- add repeat opts  with cron support and test them in example by @spencerjibz ([2504142](https://github.com/KioHQ/kiomq/commit/2504142d2e756b109c1fdc986c4f472c384cf05c))

- add support for cron as inital delay by @spencerjibz ([c20932c](https://github.com/KioHQ/kiomq/commit/c20932c87e9b268d16a34386fd626486b0c6e894))

- add obliterate as an event, rm prioritized collection on obliteration by @spencerjibz ([7e20e2b](https://github.com/KioHQ/kiomq/commit/7e20e2be4a880e6878650ee050abf90e95aed4db))

- add sample queue tests by @spencerjibz ([8aab52f](https://github.com/KioHQ/kiomq/commit/8aab52feff5ce833addffc550bedc1e3ecd42cbb))

- add worker state as an enum by @spencerjibz ([dc542d3](https://github.com/KioHQ/kiomq/commit/dc542d3116c461bbd2d5ee90e8d6b252210e09cb))

- add worker tests by @spencerjibz ([6ebecf2](https://github.com/KioHQ/kiomq/commit/6ebecf2030f31434442c165457ee99be413bf449))

- add client to queue, support both favours of processors (sync and async) by @spencerjibz ([9039e2b](https://github.com/KioHQ/kiomq/commit/9039e2b599f53da7d95011c8dac839a3fca48a5f))

- add nextest-config file by @spencerjibz ([28b11cc](https://github.com/KioHQ/kiomq/commit/28b11cc14179153ff3cc849c6f476148fbf9d613))

- add async-trait by @spencerjibz ([981a0ce](https://github.com/KioHQ/kiomq/commit/981a0ce40e783eb7a3856da48aeccaef3fbec6a1))

- add store to examples by @spencerjibz ([c1e6308](https://github.com/KioHQ/kiomq/commit/c1e630863443d1bad200aa66b96a9d208d155e70))

- add rocksdb as a store with a feature flag by @spencerjibz ([74f9a92](https://github.com/KioHQ/kiomq/commit/74f9a921e2347c20e58d7eb3e24c6a8ac50aa41f))

- add basic example with redis-store by @spencerjibz ([c202069](https://github.com/KioHQ/kiomq/commit/c2020691c2b06fc296117ffe7a87e69bff8f6a8d))

- add an in-memory-store and its example by @spencerjibz ([0ed568d](https://github.com/KioHQ/kiomq/commit/0ed568d855b3dcca3a7c7fae07effc60a2242c15))

- add timed-map to use in the in-memory-store by @spencerjibz ([26b8169](https://github.com/KioHQ/kiomq/commit/26b8169d9b5b95342cc0c8bbf0ed94792fa9925a))

- add timed locks to both stores by @spencerjibz ([101be3d](https://github.com/KioHQ/kiomq/commit/101be3d5c43b263185dca2a05819b91585448bb5))

- implement store.remove for in-memory-store && fix typo in JobState enum" by @spencerjibz ([1725ff9](https://github.com/KioHQ/kiomq/commit/1725ff9e3433af8ef1026ff115b0ed94af0a24b1))

- add some utility functions by @spencerjibz ([da06fb8](https://github.com/KioHQ/kiomq/commit/da06fb8e38ea8af6b12161c08fbf7d60749698bd))

- add more fields to job-metrics by @spencerjibz ([8bf8942](https://github.com/KioHQ/kiomq/commit/8bf8942b3f2a66b55ef6796605f3e6afb3809cee))

- add support for purging expired entries for store by @spencerjibz ([0f0f12b](https://github.com/KioHQ/kiomq/commit/0f0f12bd44283addfb6a741bf82e5c2ab286901f))

- add job metrics to complated event by @spencerjibz ([64b6b6d](https://github.com/KioHQ/kiomq/commit/64b6b6d6efd8d3b317988f5bfcd5d902eef297c9))

- enable disabling expirations on customTimedMap by @spencerjibz ([0a84fde](https://github.com/KioHQ/kiomq/commit/0a84fded42fc21617f54cd69cf01ba5554033787))

- add delay from options to job_metrics by @spencerjibz ([#30](https://github.com/KioHQ/kiomq/pull/30))

- add resoure span to the worker by @spencerjibz ([b152f98](https://github.com/KioHQ/kiomq/commit/b152f984b7981b2e602a0a30b73f87a47d944224))

- add logging to main_task and bug fix: stop main_loop on cancellation by @spencerjibz ([c3d8613](https://github.com/KioHQ/kiomq/commit/c3d861381e85d575a1a7b8316b6592567a5b8c4e))

- add logging to timers by @spencerjibz ([dcf5070](https://github.com/KioHQ/kiomq/commit/dcf507011c5f6b936649f6996c45befdd7d1e60c))

- add logs and remodel loop and timer to promote delayed jobs too by @spencerjibz ([d2b096f](https://github.com/KioHQ/kiomq/commit/d2b096fe7e07bcfb0904f9bebca96d8646b61521))

- add a resource-span to queue by @spencerjibz ([e9b0cae](https://github.com/KioHQ/kiomq/commit/e9b0caedc8fb94e6136cf2da33024af0cbaf9b2c))

- issue-templates by @spencerjibz ([b4c02df](https://github.com/KioHQ/kiomq/commit/b4c02df07810c216afb965e6d50bd2521fba718a))

- add criterion benchmarks using inmemory-store by @spencerjibz ([75c3427](https://github.com/KioHQ/kiomq/commit/75c342772da85081983c3624354bc9f8436e1ac4))

- add an action for crition benchmarks by @spencerjibz ([8bdaca1](https://github.com/KioHQ/kiomq/commit/8bdaca11f8f35c1006a7eac6f06a955a73489cf2))

- add missing meta-data for action by @spencerjibz ([0c911e1](https://github.com/KioHQ/kiomq/commit/0c911e173a7b0baeb9df95ffebbde7cd4edeec3b))

- add permissions for action to post results by @spencerjibz ([#39](https://github.com/KioHQ/kiomq/pull/39))

- add tests for panics in processors by @spencerjibz ([893fc33](https://github.com/KioHQ/kiomq/commit/893fc3392fbf37e6cd15515c0e4a4d234d514d78))

- add tokio-metrics as a dependency by @spencerjibz ([639e34a](https://github.com/KioHQ/kiomq/commit/639e34a52a1a650ed9bf74c2bdd0255b1a0bb362))

- add worker_metrics collection to worker and setup periodic heartbeat/collection timer by @spencerjibz ([8c760dc](https://github.com/KioHQ/kiomq/commit/8c760dce47ca28c5db7cf37a158388528aaf06dc))

- add metrics collection to in-memory-store by @spencerjibz ([a11d63c](https://github.com/KioHQ/kiomq/commit/a11d63c0470675de5f351daf1ed3a2822ccb73ab))

- add metrics collection for redis-store by @spencerjibz ([e0c6a4e](https://github.com/KioHQ/kiomq/commit/e0c6a4e4c26bd8a050401dc32dcf0e807754660c))

- add timestamps to track metrics updates by @spencerjibz ([cab75f0](https://github.com/KioHQ/kiomq/commit/cab75f0e1597f924fffb3ee17a938be46bb28046))

- add asset folder with logo by @spencerjibz ([5ec1bf9](https://github.com/KioHQ/kiomq/commit/5ec1bf93e6673a4af00eeb786abcebf70b66a50e))

- add a helper macro for worker_tests by @spencerjibz ([2a6158d](https://github.com/KioHQ/kiomq/commit/2a6158d319f4f1ad497c2d85464ba92fe2575134))

- add isolated tests for each store (inmemory or redis-store) by @spencerjibz ([2aeedc9](https://github.com/KioHQ/kiomq/commit/2aeedc956f5fd327e2d712e375762cfd40c60ead))

- add helper macros for queue-tests for diffrent stores by @spencerjibz ([e970dc6](https://github.com/KioHQ/kiomq/commit/e970dc649a9e890c89f32b452ad66fb78f97d3d9))


### Other

- Initial commit by @spencerjibz ([0da1a6c](https://github.com/KioHQ/kiomq/commit/0da1a6c4b2f8cfe3d37d282fe1a52a23920ac031))

- rust library template ([32dfa0a](https://github.com/KioHQ/kiomq/commit/32dfa0a3a97be89cd2768744693eeedc7fe66f47))

- base job impl by @spencerjibz ([c1646b9](https://github.com/KioHQ/kiomq/commit/c1646b9a8c331cc8e96b74fa9804f9d96e3add15))

- remove custom field-serializers by @spencerjibz ([aae3824](https://github.com/KioHQ/kiomq/commit/aae3824d9b660488d3e872daa8166b2e2aa18cdc))

- use dashmap and use owned values by @spencerjibz ([fad0c4d](https://github.com/KioHQ/kiomq/commit/fad0c4d28445e045d83a9826e0331bc7679fd8d2))

- use some generics for queue methods by @spencerjibz ([43a7b47](https://github.com/KioHQ/kiomq/commit/43a7b4739f293a5f8142dd84ab4138b1299bf4fc))

- wip by @spencerjibz ([82c12f1](https://github.com/KioHQ/kiomq/commit/82c12f1de50ef80a5f4d2f96c76609aecaf44f06))

- reorganise modules and supplement worker by @spencerjibz ([c414985](https://github.com/KioHQ/kiomq/commit/c414985b2af3082628572102076a4fcb7039a232))

- clippy fix by @spencerjibz ([110e3b9](https://github.com/KioHQ/kiomq/commit/110e3b92240a0e8e3df5cfaa8f105dda00e8c313))

- Merge pull request #1 from spencerjibz/base-impl by @spencerjibz ([#1](https://github.com/KioHQ/kiomq/pull/1))

- ignore panic_test in backtrace_utils by @spencerjibz ([ad436a9](https://github.com/KioHQ/kiomq/commit/ad436a99e15b21e0dfa653a1364b83d53dab5f77))

- make timer simple by @spencerjibz ([6e4830a](https://github.com/KioHQ/kiomq/commit/6e4830a04be573e71dc0a0c5a5ab2140831d35c9))

- minor fixes by @spencerjibz ([d1d36de](https://github.com/KioHQ/kiomq/commit/d1d36de0fa5f3e5b9bc0a41fdba23277f3ad9fb5))

- complete running worker with panic catches by @spencerjibz ([4c13f3f](https://github.com/KioHQ/kiomq/commit/4c13f3fe6f6191695d399daec5130dc5e8297117))

- use an serializable backtrace by @spencerjibz ([26830bf](https://github.com/KioHQ/kiomq/commit/26830bf87d8989a512cecf22f82dc8ec477fca16))

- remove unecessary deps and use std version of backtrace by @spencerjibz ([3bba177](https://github.com/KioHQ/kiomq/commit/3bba1778859baa24e24879cf0523533f55ac1be8))

- create similar stacktraces by @spencerjibz ([61f9702](https://github.com/KioHQ/kiomq/commit/61f9702b6714ea5f96843f755defce5bff265351))

- clippy fix by @spencerjibz ([19267d7](https://github.com/KioHQ/kiomq/commit/19267d77493f2ab088ecf70fb139cc8220e7b963))

- Merge pull request #3 from spencerjibz/working-runner by @spencerjibz ([#3](https://github.com/KioHQ/kiomq/pull/3))

- readme edit by @spencerjibz ([1d6a267](https://github.com/KioHQ/kiomq/commit/1d6a267359d5c74720b87ba044336ce9688f9748))

- make job_state serializable from redis by @spencerjibz ([77d94a6](https://github.com/KioHQ/kiomq/commit/77d94a640f6048feb38cd41c26c475e209b0ee27))

- remove arc-swap by @spencerjibz ([570f33e](https://github.com/KioHQ/kiomq/commit/570f33edc97a6090d2c3b0c7ef472a5057c06799))

- expose the worker's cancellation token by @spencerjibz ([6474c44](https://github.com/KioHQ/kiomq/commit/6474c44ef78e22bdf347c2e3cd417b846718f965))

- access the current job count from queue by @spencerjibz ([d95fabf](https://github.com/KioHQ/kiomq/commit/d95fabfb6212f15260367cfe381ad0cba7e8fa18))

- serialize datetimes as microsecond timestamps by @spencerjibz ([de2ccfe](https://github.com/KioHQ/kiomq/commit/de2ccfe0777579e34d29c585b7ec47a4359458b5))

- ignore video files by @spencerjibz ([d79837d](https://github.com/KioHQ/kiomq/commit/d79837d3290207ed9cef1fd493871d8cd35cde8d))

- improve backoff and add a method to fetch metrics to a job by @spencerjibz ([149862b](https://github.com/KioHQ/kiomq/commit/149862b62130454fb05d9da7dc2510b9c3358a08))

- create a sample video file if none exists by @spencerjibz ([#5](https://github.com/KioHQ/kiomq/pull/5))

- Next stage (#6) by @spencerjibz ([#6](https://github.com/KioHQ/kiomq/pull/6))

- clean up after running transcoding by @spencerjibz ([cc9af51](https://github.com/KioHQ/kiomq/commit/cc9af5145459fbffe73621fceafbed37f08522d1))

- download and install ffmpeg if its not available (#8) by @spencerjibz ([#8](https://github.com/KioHQ/kiomq/pull/8))

- simple code refactor by @spencerjibz ([5a73a8f](https://github.com/KioHQ/kiomq/commit/5a73a8ffa16f1711c793c3dfd4ab961dcedf2602))

- add support for skipping first_tick by @spencerjibz ([dfd8f29](https://github.com/KioHQ/kiomq/commit/dfd8f298078330ba2e3179e991026328a264521c))

- job-schedulering-timer template by @spencerjibz ([6379443](https://github.com/KioHQ/kiomq/commit/6379443cb45caeaf867740600d04c290199bba33))

- set concurrency to number of logical cpus by @spencerjibz ([c3601b8](https://github.com/KioHQ/kiomq/commit/c3601b89b68b5c26ad1ca6ada44804be520e4cdc))

- set number of cpus as default concurrency option by @spencerjibz ([bd26ab4](https://github.com/KioHQ/kiomq/commit/bd26ab4e17b3c2259c62242658ebc905119881fc))

- better delay support by @spencerjibz ([1fa6f20](https://github.com/KioHQ/kiomq/commit/1fa6f20a83a52ed16aa052f558a713bc02b0d65d))

- completed impl by @spencerjibz ([732da55](https://github.com/KioHQ/kiomq/commit/732da55c865f364e5b002508d1bc97984c453148))

- only remove jobs if found by @spencerjibz ([a1ef2b3](https://github.com/KioHQ/kiomq/commit/a1ef2b3baa6cff78cb996494ec94944dc84a4678))

- make move_to_active_result debug printable by @spencerjibz ([375f1e7](https://github.com/KioHQ/kiomq/commit/375f1e7ed99d4cc8a490c657742821a83b35077c))

- re-use timer for scheduling delayed jobs, remove futures_concurrency primitive for running task by @spencerjibz ([345de47](https://github.com/KioHQ/kiomq/commit/345de475f5aa28ee8902d9975cd1779dedbe5e97))

- use crossbeam queue, yield in busy loops to prevent race conditions by @spencerjibz ([c3aa115](https://github.com/KioHQ/kiomq/commit/c3aa1159ae14322c702fc4df41f66f92a0d7259e))

- emit progress to stream when it's updated by @spencerjibz ([a9c9c52](https://github.com/KioHQ/kiomq/commit/a9c9c52bed06e6fdadd22f4cb37ce53ec1c7d121))

- emit events directly from redis-streams by @spencerjibz ([#10](https://github.com/KioHQ/kiomq/pull/10))

- rebase conflict fix by @spencerjibz ([de77c24](https://github.com/KioHQ/kiomq/commit/de77c24b00795a52d1aa66961874fcea691a2665))

- add live metrics and opts to the queue by @spencerjibz ([85368ce](https://github.com/KioHQ/kiomq/commit/85368ce08e685c4b0e6c5fb91dc83469546575d7))

- remove dashmap, completely lockfree impl by @spencerjibz ([0a0159b](https://github.com/KioHQ/kiomq/commit/0a0159b039dfdf226fcae8d3a060727d9f831e57))

- update example to support all job features: delaying, prioritization by @spencerjibz ([1f2bac8](https://github.com/KioHQ/kiomq/commit/1f2bac8299eb4458b58be80ad67f8d433bcb9c94))

- clean up by @spencerjibz ([18feb2d](https://github.com/KioHQ/kiomq/commit/18feb2da06cd11d69bdcee4d1130bf4a847c408b))

- use lockfree version of typed-emitter by @spencerjibz ([2396420](https://github.com/KioHQ/kiomq/commit/23964208093b68e318a996dd670d065c1735bb1b))

- update uuid:dep by @spencerjibz ([53322e2](https://github.com/KioHQ/kiomq/commit/53322e2ed7956eca0d947c08295f47cd249eab9d))

- boxed some futures to reduce their size by @spencerjibz ([b05890e](https://github.com/KioHQ/kiomq/commit/b05890eb4d5342d6cb5817eb2a6d6e934cd09746))

- wip by @spencerjibz ([f5ba7b0](https://github.com/KioHQ/kiomq/commit/f5ba7b020fad25fc9afc864796aaae0c690c1e9d))

- only return joinhandle if timer is running by @spencerjibz ([a7254fc](https://github.com/KioHQ/kiomq/commit/a7254fc551e541b5906d54c754bb011b0d6d39d2))

- Revert "fix retrials & remove crossbeam-skiplist" by @spencerjibz ([d9a236e](https://github.com/KioHQ/kiomq/commit/d9a236eb30a25d244ee276e42ef1bb240d84f206))

- prioritize delayed jobs by @spencerjibz ([c283935](https://github.com/KioHQ/kiomq/commit/c28393597ee4bfefe9623f638e82f55c3906eccd))

- fail jobs that miss delay reschedule by @spencerjibz ([34982c9](https://github.com/KioHQ/kiomq/commit/34982c9ea884fef218913e505272dfa938d6a5a4))

- reduce cpu usage during idle times by @spencerjibz ([ba7b441](https://github.com/KioHQ/kiomq/commit/ba7b4413a759b70d82acfe7949899213a04cfd79))

- make timers pausable by @spencerjibz ([569b423](https://github.com/KioHQ/kiomq/commit/569b4233c59f943d81baea40e3705e28b6c2a7ae))

- pause both timer when queue and worker are idle by @spencerjibz ([8a6e9b9](https://github.com/KioHQ/kiomq/commit/8a6e9b950b914adfe68ad3d68c9a67fe01b4adef))

- improve examples by @spencerjibz ([7856f22](https://github.com/KioHQ/kiomq/commit/7856f22837563fcc7b3e57bb2bc5b3142fab139c))

- reduce string usage by @spencerjibz ([979c1e0](https://github.com/KioHQ/kiomq/commit/979c1e094b7662f3cefd8f7f59fd888183d08819))

- Support either pubsub or redis_stream as queue-event-stream by @spencerjibz ([68c16dc](https://github.com/KioHQ/kiomq/commit/68c16dc0ef205853adeef416d92f8beae3e1e1ae))

- wip by @spencerjibz ([ab5da6a](https://github.com/KioHQ/kiomq/commit/ab5da6ae371233f0627a6ba9143465c132460cd0))

- refactor and cleanup by @spencerjibz ([a1104ef](https://github.com/KioHQ/kiomq/commit/a1104efc7de68310656828235ac787e6d2ef9445))

- support for both modes by @spencerjibz ([3945796](https://github.com/KioHQ/kiomq/commit/39457969b938e94a5f722bbcf6f1f3781ceeb2b5))

- use simple types in job and queue by @spencerjibz ([a7733f1](https://github.com/KioHQ/kiomq/commit/a7733f13cf5154d9489fd03edf9a7c62f75aa922))

- wip by @spencerjibz ([6754e9f](https://github.com/KioHQ/kiomq/commit/6754e9f78be8adc58f0c955eaf0707e08e6f375c))

- use simd_json by @spencerjibz ([6938133](https://github.com/KioHQ/kiomq/commit/693813307f273c15a58635040e860e057d041cae))

- cheap parsing when using redis-stream-events by @spencerjibz ([4144ac2](https://github.com/KioHQ/kiomq/commit/4144ac22450dc463313c440561ceb1521abeb3b4))

- restore examples by @spencerjibz ([e4c1577](https://github.com/KioHQ/kiomq/commit/e4c15775d2bb43ad857445a1601db95be7034b74))

- make bulk_add faster by @spencerjibz ([5c8ff95](https://github.com/KioHQ/kiomq/commit/5c8ff95b105fbf72f349ae04a3f302f06e8efd5c))

- clippy fix by @spencerjibz ([06521b4](https://github.com/KioHQ/kiomq/commit/06521b4af96736d5917218c0041a26402b2b9dc4))

- repeat-opts-working progress by @spencerjibz ([38f3a4f](https://github.com/KioHQ/kiomq/commit/38f3a4f758ec769be1175f5ccbc671533d216502))

- simple fixes by @spencerjibz ([1592c5a](https://github.com/KioHQ/kiomq/commit/1592c5af5bac3b70e9d3c7d3649bdbd2aa43e4b4))

- increment ids correctly by @spencerjibz ([6d44ee7](https://github.com/KioHQ/kiomq/commit/6d44ee71747af38c5d538894a8508d9e301ac072))

- update examples by @spencerjibz ([00129bf](https://github.com/KioHQ/kiomq/commit/00129bf17d8c200bf116baee3cda618e3b76cd49))

- code refactor by @spencerjibz ([#13](https://github.com/KioHQ/kiomq/pull/13))

- update the job_id after addition by @spencerjibz ([f27f96e](https://github.com/KioHQ/kiomq/commit/f27f96e9893e1c29cb7053f7bee792c599489eaf))

- more queue tests by @spencerjibz ([339f4d8](https://github.com/KioHQ/kiomq/commit/339f4d8ab745aeb995cddb34d7f9a41f81a3c78a))

- :emitter_on is not an async function: correct this by @spencerjibz ([538a281](https://github.com/KioHQ/kiomq/commit/538a2819d5bea1d5a36ed8d13d6681d6cc31c188))

- remove tokio_shared_rt and use a multithread runtime for worker tests by @spencerjibz ([060620a](https://github.com/KioHQ/kiomq/commit/060620ac395f713024e9ae677638cba822865d30))

- fetch number of cpus by @spencerjibz ([3ed68b1](https://github.com/KioHQ/kiomq/commit/3ed68b13c2084399523b3bbfbcd7db3aa0b809f4))

- support both sync and async callbacks by @spencerjibz ([a26d72f](https://github.com/KioHQ/kiomq/commit/a26d72f581a04f4e3e8a303edb2b9f69486ea46d))

- update examples and tests by @spencerjibz ([44d9e75](https://github.com/KioHQ/kiomq/commit/44d9e7590fafc86b9d9596257f8b3b61fbaed0ed))

- reorganise code structure by @spencerjibz ([e72038a](https://github.com/KioHQ/kiomq/commit/e72038a9b8c725ac3f983e14b49765e5cb4c8012))

- make job-update progress sync by @spencerjibz ([54ab3a4](https://github.com/KioHQ/kiomq/commit/54ab3a461d3c914f29b5d790fda479c97f5f476d))

- update examples by @spencerjibz ([0efb189](https://github.com/KioHQ/kiomq/commit/0efb189c1ca560bba54cb57bded0247f1457c3e0))

- restore connection-example by @spencerjibz ([0728dcc](https://github.com/KioHQ/kiomq/commit/0728dcc65ba86f8a93e22c0421c938db7eefc7b8))

- Format README.md with proper headings and lists by @spencerjibz ([b83ec7e](https://github.com/KioHQ/kiomq/commit/b83ec7e0ba77358d9b2dc0e4faa87eaeaaff96c0))

- make all errors public and the worker'id by @spencerjibz ([8ef613f](https://github.com/KioHQ/kiomq/commit/8ef613feea67fad962486bf103fd7da0ed23705b))

- return an error if a worker is already running and a called to run is made by @spencerjibz ([23dc2cd](https://github.com/KioHQ/kiomq/commit/23dc2cdb8a69dfc871638e6a52f7f455d40054c1))

- make CollectionSuffix enum and worker_state field pub by @spencerjibz ([5c07d40](https://github.com/KioHQ/kiomq/commit/5c07d40465fb5ceef6e3ab72d3de6d0c7793959b))

- create a store trait and a redis store by @spencerjibz ([438f627](https://github.com/KioHQ/kiomq/commit/438f627c5c4ab9ad50e57c276ded250b5280ad58))

- clean up and use dynamic store logic in queue-impl by @spencerjibz ([a131a23](https://github.com/KioHQ/kiomq/commit/a131a23e0ae78bff10d888637bc4d0312d108836))

- refactor to use dynamic stores by @spencerjibz ([ddae040](https://github.com/KioHQ/kiomq/commit/ddae040fcbfeddfa7f25337507b5dfedfc8441df))

- Integrate the Store trait into queue, worker and utils function by @spencerjibz ([60ccf6e](https://github.com/KioHQ/kiomq/commit/60ccf6e8068d20552793dd0784df505c943fb37c))

- update tests with store by @spencerjibz ([8d2cd80](https://github.com/KioHQ/kiomq/commit/8d2cd80d2241b7074b1b7b6cf325194a5a4021ad))

- clippy fix by @spencerjibz ([47f6a30](https://github.com/KioHQ/kiomq/commit/47f6a30d9dddd1876e2d89101eec6b47001af465))

- make collectionSuffix serialized as an u64 tag by @spencerjibz ([86efd8b](https://github.com/KioHQ/kiomq/commit/86efd8bd6a20c298db9558a2974556c3fd5baa2c))

- move event-stream-handle creating from store implementations by @spencerjibz ([2762b50](https://github.com/KioHQ/kiomq/commit/2762b503b5ff36c7dc3de83aff6448dfb3436b0a))

- complete Store trait impl for redis-store by @spencerjibz ([eee2409](https://github.com/KioHQ/kiomq/commit/eee2409005179bf3426be9a66e206bfb19e360d5))

- dont run rocksdb-example in tests by @spencerjibz ([29ffc99](https://github.com/KioHQ/kiomq/commit/29ffc9970eaa3852707626f79261585c575656e4))

- yield in task picker by @spencerjibz ([e32a34f](https://github.com/KioHQ/kiomq/commit/e32a34fdfe10b00e038108d42786301ddf0f7aee))

- use parking_lot primitives instead by @spencerjibz ([#16](https://github.com/KioHQ/kiomq/pull/16))

- simple improvement to in-memory-store by @spencerjibz ([41b3654](https://github.com/KioHQ/kiomq/commit/41b36546cfcc357516877884df2fdc7918b0b446))

- refactor store.set_fields and move the ```move_to_state``` method out of the store by @spencerjibz ([2b1467a](https://github.com/KioHQ/kiomq/commit/2b1467a98980fa7530ba57dbdb4d7a625046ea70))

- clean and reduce store methods by @spencerjibz ([ecb50b4](https://github.com/KioHQ/kiomq/commit/ecb50b4848dca2bae982f14ad9fe7dab8f3aa9ad))

- expose important methods from store by @spencerjibz ([51f8715](https://github.com/KioHQ/kiomq/commit/51f8715cd98e7ed23da62e3a4361890ed61f83ce))

- clean up events && split functions in utils by @spencerjibz ([61aebeb](https://github.com/KioHQ/kiomq/commit/61aebeba576a4400e73c447af55448aea664e5f6))

- remove stream-handle, use emiiter directly to publish events for rocksdb and in-memory-store by @spencerjibz ([506da8b](https://github.com/KioHQ/kiomq/commit/506da8bce14dfceb7d4ebe5ab641500d20c6f9b2))

- clippy fix@ by @spencerjibz ([61d5a38](https://github.com/KioHQ/kiomq/commit/61d5a389b1452453e45dd083121523eca5451121))

- use a checked insert method by @spencerjibz ([fc80401](https://github.com/KioHQ/kiomq/commit/fc8040156e901aa0a9fad15545a118b9c1322834))

- use a delay-queue for timers instead of spawning task (previous timer) by @spencerjibz ([#18](https://github.com/KioHQ/kiomq/pull/18))

- use tasktracker, add graceful shutdown by @spencerjibz ([#19](https://github.com/KioHQ/kiomq/pull/19))

- make redis-store optional by @spencerjibz ([88c960e](https://github.com/KioHQ/kiomq/commit/88c960ee24846e58443629d87f9c1df114140f7b))

- make redis-store a default feature by @spencerjibz ([a6cfee3](https://github.com/KioHQ/kiomq/commit/a6cfee36c0fdc0b2d85f075dda9eeb40dd0ea336))

- merge similar examples into a few with feature flags by @spencerjibz ([#21](https://github.com/KioHQ/kiomq/pull/21))

- use new timedmap by @spencerjibz ([183a2f9](https://github.com/KioHQ/kiomq/commit/183a2f953aa422af6825cdc7e58505eb4cb2ee0b))

- only purge expired when ther e is some expiring soon by @spencerjibz ([9f76297](https://github.com/KioHQ/kiomq/commit/9f762972e5f9c850c6bf1dc2a65001d544b2784b))

- use atomiccell instead of arc-swap and use tokio-mutex for timed-map by @spencerjibz ([183919d](https://github.com/KioHQ/kiomq/commit/183919d4402112a384977830f9874ecab4f2080c))

- Refactor set_key to use Key directly by @spencerjibz ([f14b97c](https://github.com/KioHQ/kiomq/commit/f14b97c6a2844493123cd0cfcac686876df3bd95))

- clippy fix && reduce critical section for race condtion (when both mutexes are held by @spencerjibz ([785879b](https://github.com/KioHQ/kiomq/commit/785879bdc9a1d5c41d4bea69548a490e9fba50e7))

- purge both locks and jobs concurrenctly by @spencerjibz ([923ecd3](https://github.com/KioHQ/kiomq/commit/923ecd3fcc785ff614fd5a13f47751fc58dee6ee))

- use length in purge_expired: inMemory by @spencerjibz ([f13468e](https://github.com/KioHQ/kiomq/commit/f13468e9d30ada653544b82e2f0cdfdbf5e9e508))

- use a better and faster mutex by @spencerjibz ([#22](https://github.com/KioHQ/kiomq/pull/22))

- remove job from events, update examples and tests to reflct this by @spencerjibz ([0e09ab3](https://github.com/KioHQ/kiomq/commit/0e09ab3a0b9484d08064eebcf6a5539bab3980d5))

- minor bug fixes(panics) too by @spencerjibz ([d34e460](https://github.com/KioHQ/kiomq/commit/d34e460c4a436d2ec5299a66038f925a4e60312a))

- update tests by @spencerjibz ([#23](https://github.com/KioHQ/kiomq/pull/23))

- clippy fix in examples by @spencerjibz ([6872fb5](https://github.com/KioHQ/kiomq/commit/6872fb591198714a16078e9dfdcfe3e294416248))

- minor changes by @spencerjibz ([#24](https://github.com/KioHQ/kiomq/pull/24))

- only use async version of xutex::Mutex by @spencerjibz ([ad0c370](https://github.com/KioHQ/kiomq/commit/ad0c3706e1fb1f05314a9603fae208b718db1526))

- use semaphore to limit concurrency by @spencerjibz ([#25](https://github.com/KioHQ/kiomq/pull/25))

- update queue-metrics in rocksdb-store by @spencerjibz ([#33](https://github.com/KioHQ/kiomq/pull/33))

- clean up main loop by @spencerjibz ([7a98d6f](https://github.com/KioHQ/kiomq/commit/7a98d6f0fa30b1d9dbf1e459af218ecf65ead20f))

- wip by @spencerjibz ([6d761d9](https://github.com/KioHQ/kiomq/commit/6d761d94ed1407bd71af6d0f2ae60287d3c699f3))

- clippy fix by @spencerjibz ([2e420b5](https://github.com/KioHQ/kiomq/commit/2e420b5e12c83429b71c8a9c9e083dc18d34927f))

- reduce verbosity of logging and refactor smt by @spencerjibz ([85e4b72](https://github.com/KioHQ/kiomq/commit/85e4b7241e4353df89f5cf884c6497b857b97230))

- more logging by @spencerjibz ([aab3733](https://github.com/KioHQ/kiomq/commit/aab3733b86230db2e906aa516c7a4436d03e3ede))

- uncomment tokio-console directives by @spencerjibz ([f60217f](https://github.com/KioHQ/kiomq/commit/f60217fa80b40a866c836883d66029ad99da32b9))

- store joinhandle of running tasks instead of their ids only by @spencerjibz ([8545cd5](https://github.com/KioHQ/kiomq/commit/8545cd5c56fb7a6b46dd9d5b6e730dc232a10dca))

- better debug formating for TimerType::PromoteJob by @spencerjibz ([36f672a](https://github.com/KioHQ/kiomq/commit/36f672ae28512abfdaea8b6e788120ac80b92a06))

- PR template by @spencerjibz ([#36](https://github.com/KioHQ/kiomq/pull/36))

- main_loop fix: only proceed processing if a permit is available by @spencerjibz ([#37](https://github.com/KioHQ/kiomq/pull/37))

- make Timer public by @spencerjibz ([a08e0a9](https://github.com/KioHQ/kiomq/commit/a08e0a93c6a3971ddb18258a10640d8ac713706a))

- make a worker_opts a copy type by @spencerjibz ([98a2c35](https://github.com/KioHQ/kiomq/commit/98a2c35fa52491bfd557afcac467bedf554466ba))

- clippy fix by @spencerjibz ([d2bbefe](https://github.com/KioHQ/kiomq/commit/d2bbefef79a1f54ebac482ddf9ed74787c54f75c))

- clean & better usage of atomic ordering by @spencerjibz ([989189c](https://github.com/KioHQ/kiomq/commit/989189c7b2fc27289308ff8dd21940c2a19d3847))

- guard data in timed_maps with a parking_lot primitives by @spencerjibz ([83d0178](https://github.com/KioHQ/kiomq/commit/83d01789d9f8ad5761d7012c067b6eb95298b649))

- use parking_lot mutex instead rwlock for in-memory-store by @spencerjibz ([#38](https://github.com/KioHQ/kiomq/pull/38))

- maintain Acquire-Release ordering by @spencerjibz ([68a8518](https://github.com/KioHQ/kiomq/commit/68a851829e5c1d3a6449030276c97ab0a447937a))

- exclude library tests from benchmarks by @spencerjibz ([77b695e](https://github.com/KioHQ/kiomq/commit/77b695e7389f7a9ed248a14ba90222c0e87740a9))

- use the latest stable for benchmarks by @spencerjibz ([ebe2dbc](https://github.com/KioHQ/kiomq/commit/ebe2dbc9357a2284d3a5713717be47558509836c))

- wip by @spencerjibz ([7552a70](https://github.com/KioHQ/kiomq/commit/7552a704e69b75c1550d45d87d763beeecba24ad))

- exclude library tests in benchmarks && fix caching by @spencerjibz ([faed406](https://github.com/KioHQ/kiomq/commit/faed406d1d0b47a217421f0315b18136b1583742))

- catch panics from sync_processors by @spencerjibz ([a200802](https://github.com/KioHQ/kiomq/commit/a2008020c0312e0967b6cb84a0c8f0358f083483))

- expose worker-metrics methods from store by @spencerjibz ([31e60e1](https://github.com/KioHQ/kiomq/commit/31e60e160317d19bcc198f6d235bd6d453e783d0))

- collect metrics from timers by @spencerjibz ([4449e36](https://github.com/KioHQ/kiomq/commit/4449e3634fbbed643ab2d44e95470888dd07e0e1))

- minor metrics updates by @spencerjibz ([9c209a9](https://github.com/KioHQ/kiomq/commit/9c209a9808c5b32c489edcf19ec2354c78dff038))

- a clippy fix by @spencerjibz ([#41](https://github.com/KioHQ/kiomq/pull/41))

- make all fields in WorkerMetrics pub by @spencerjibz ([be3a0a1](https://github.com/KioHQ/kiomq/commit/be3a0a16e7e159a494e3309367f2644ccbc82fbc))

- make worker_metrics clonable and comparable by @spencerjibz ([b4653a6](https://github.com/KioHQ/kiomq/commit/b4653a67c5445ab4d5e96c03a9f06e8aa6d94598))

- export our timedmap implementation to the library users by @spencerjibz ([2d843cd](https://github.com/KioHQ/kiomq/commit/2d843cd207d9023e1093888cd238f02ede87435a))

- make the ```insert_expirable``` method sync by @spencerjibz ([b76a039](https://github.com/KioHQ/kiomq/commit/b76a039ab4d6d4623ce77c2354f6bbea76771e3b))

- don't hold on to mutex for long by @spencerjibz ([a0c8b10](https://github.com/KioHQ/kiomq/commit/a0c8b10ee2ed983ae518b0692d42cbcf096e0b2a))

- strictly use async-mutex event in sync code by @spencerjibz ([55654f6](https://github.com/KioHQ/kiomq/commit/55654f60c02db1b7f4d84ff847a529f23e0343f9))

- collective metrics for the user's processor function instead of the worker task by @spencerjibz ([5dc9271](https://github.com/KioHQ/kiomq/commit/5dc9271cf48ea1cc4437d9bb8b8133be443fd6a9))

- Fix task metrics collection: instrument spawned future, fix TTL bug, fallback task_id, add test by @Copilot ([910ca34](https://github.com/KioHQ/kiomq/commit/910ca349e8f2c9d1ff15d2731c1d7126eeb43db8))

- comments clean up by @spencerjibz ([80d6f37](https://github.com/KioHQ/kiomq/commit/80d6f37361372dc634974bbf74ba0465c17bac5e))

- clippy clean-up by @spencerjibz ([#43](https://github.com/KioHQ/kiomq/pull/43))

- library-rename by @spencerjibz ([6b573d6](https://github.com/KioHQ/kiomq/commit/6b573d67b542116cf0404596a40917ce12174c9d))

- library name correction in readme by @spencerjibz ([585e1b2](https://github.com/KioHQ/kiomq/commit/585e1b250f35fa54f65cfb0e798cc9b27430231c))

- replace crossbeam-sub-crates(queue& utils) by @spencerjibz ([48af5ca](https://github.com/KioHQ/kiomq/commit/48af5ca19d978c794a3ef94eee20675f891f23e0))

- replace usage of atomic_ref_cell by @spencerjibz ([eabf62b](https://github.com/KioHQ/kiomq/commit/eabf62b5dc0420e8999b62cb7c21c20b6028799c))

- clippy fixes by @spencerjibz ([#47](https://github.com/KioHQ/kiomq/pull/47))

- fix clippy warnings by @spencerjibz ([#46](https://github.com/KioHQ/kiomq/pull/46))

- Complete documentation coverage: all public items documented, no missing-docs gaps by @Copilot ([55274f2](https://github.com/KioHQ/kiomq/commit/55274f2743f4ede17dfab455911fcc4d14fd6f4e))

- Center lib.rs header with div align=center and downshift all heading levels by one by @Copilot ([a8731ce](https://github.com/KioHQ/kiomq/commit/a8731ce75e3150331374afc855450254224e1584))

- Add logo image to lib.rs centered header block by @Copilot ([11c134d](https://github.com/KioHQ/kiomq/commit/11c134d045aac7d0f66c93f0ab9e15ad8cd4c92d))

- Link RedisStore in top description alongside InMemoryStore by @Copilot ([bad5147](https://github.com/KioHQ/kiomq/commit/bad5147ab7c0bf2b686bb44d24470be6d9ed7c79))

- Move Tokio runtime requirements section before Installation in lib.rs docs by @Copilot ([1ec88cd](https://github.com/KioHQ/kiomq/commit/1ec88cd22df36ea89664f612bcc53205082eea6b))

- fix updating_metrics bug and add panics section to README by @Copilot ([11063e6](https://github.com/KioHQ/kiomq/commit/11063e6706457e630109856ff13b75a73522f85f))



### Contributors

- @Copilot

- @blacksmith-sh[bot]

- @spencerjibz

