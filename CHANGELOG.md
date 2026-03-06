# Changelog

All notable changes to this project will be documented in this file.
## Unreleased



### Bug Fixes

- failing missed deadline jobs

- correctly serialize queue-events

- promoting delayed jobs

- counter bugs in in-memory and rocksdb store

- set return value & add timeout to listening to events

- timedmap tests

- inmemory-store, only update stored-metrics from publish_event

- updating prioritized and failed job counts in queue-metrics

- typo in fetch_worker_metrics in redis-store

- cargo fmt

- fetching_metrics from redis-store

- tracing::Instrument::instrument is ambigious to TaskMonitor.instrument (**method-ambiguity**)

- stop relying on timestamps to generate release.md (**ci**)


### CI

- add a release-workflow

- populated CD workflow file

- add create generation to workflow (**cd-workflow**)

- fix cd pipeline


### Chores

- add changelong with git-cliff

- add author, description, links and licence metadata (**cargo.toml**)

- add best practice library clint protections

- unused/dead_code elimination

- add missing debug implementations

- only expose few methods publicity & tracing vital calls (**queue**)

- add more tracing spans (**tracing**)

- manual clippy --fix (**clippy**)

- more docs in worker_opts

- relax some lint constricts

- fix lint warnings in tests

- add clippy:nursery recommendation & fmt (**clippy**)

- use the right branch for releases (**cd**)


### Documentation

- add doc comment and example to the framed re-export in lib.rs

- condense README to match lib.rs conciseness (44% shorter)

- restore Progress updates section and improve Panics & errors wording

- add compact table of contents to README


### Other

- Initial commit

- rust library template

- base job impl

- remove custom field-serializers

- add basic required deps

- add simple queue impl with a job queue too

- add job progress updating

- use dashmap and use owned values

- add base worker

- use some generics for queue methods

- wip

- add timer implementation

- add worker options

- add backtrace_capture utils

- reorganise modules and supplement worker

- clippy fix

- Merge pull request #1 from spencerjibz/base-impl

Base impl

- add ci tests

- Merge pull request #2 from spencerjibz/add-ci

add ci tests

- ignore panic_test in backtrace_utils

- make timer simple

run the first interval tick before the callback

- add more utils to wokrer

- minor fixes

- add timer queue functions

- add timers to worker

- complete running worker with panic catches

- add support for storing stacktraces and errors separately

- use an serializable backtrace

- remove unecessary deps and use std version of backtrace

- create similar stacktraces

- clippy fix

- Merge pull request #3 from spencerjibz/working-runner

Working runner

- add readme

- readme edit

- make job_state serializable from redis

- add events to queue & worker

- update readme

- remove arc-swap

- Update README.md

- expose the worker's cancellation token

- access the current job count from queue

- serialize datetimes as microsecond timestamps

- support queue-obliteration

- add backoff utils

- re-export both backtrace tracing macros

- add joinError to KioError type

- ignore video files

- improve backoff and add a method to fetch metrics to a job

- add working video_transcoding-example

- create a sample video file if none exists

- Next stage (#6)

* update type-emitter crate

* spawn a background task when run is called

reorganise worker module too

* worker now runs the main_loop in the background

* add support for worker autorun

* add the options to force stop worker and active jobs

* add more size to transcoding example

remove batching

* format

- clean up after running transcoding

- download and install ffmpeg if its not available (#8)

- simple code refactor

- add support for skipping first_tick

- job-schedulering-timer template

- add job options with delay

- add priority to job_data and priority helpers

- set concurrency to number of logical cpus

- set number of cpus as default concurrency option

- better delay support

- enable scheduling without a seperate timer

- completed impl

- only remove jobs if found

- make move_to_active_result debug printable

- re-use timer for scheduling delayed jobs, remove futures_concurrency primitive for running task

reduce usage of locking structures (mutexes)

- use crossbeam queue, yield in busy loops to prevent race conditions

- add the right job status for delayed jobs

- Update README.md

- emit progress to stream when it's updated

- add delay field to event_stream

- add and move events into their own module

- add stream-listener-task

- add queue-stream-event

- emit events directly from redis-streams

- add job-removal options

- rebase conflict fix

- add live metrics and opts to the queue

clean up examples to reflect this too

- remove dashmap, completely lockfree impl

- fix support for job prioritization

- update example to support all job features: delaying, prioritization

- clean up

- use lockfree version of typed-emitter

- update uuid:dep

- add tokio-console

- boxed some futures to reduce their size

- pass down removal options from queue if not specified for a job

- add queue options to example

- wip

- fix job retrial after failure

- only return joinhandle if timer is running

- add support for bulk_add

- fix retrials & remove crossbeam-skiplist

- Revert "fix retrials & remove crossbeam-skiplist"

This reverts commit 3be8ef159d4b839698a2f6184a98d1db94ae554f.

- prioritize delayed jobs

- add task-notifiers & waiting-job-count to metrics

- fail jobs that miss delay reschedule

- reduce cpu usage during idle times

- add methods to bulk add jobs with returning them

- make timers pausable

- pause both timer when queue and worker are idle

- improve examples

- reduce string usage

- add proesssing to JobState and clean up events

- efficiently pausing workers with global activity count, let workers finished all their work before pausing

- Support either pubsub or redis_stream as queue-event-stream

- wip

- refactor and cleanup

- support for both modes

- use simple types in job and queue

- wip

- use simd_json

- fix serializing redis-stream-event

- cheap parsing when using redis-stream-events

- restore examples

- make bulk_add faster
&
make one only incr call

- clippy fix

- repeat-opts-working progress

- add repeat opts  with cron support and test them in example

- simple fixes

- add support for cron as inital delay

- increment ids correctly

- update examples

- code refactor

- batch pipeline commands

- update the job_id after addition

- update readme to reflect current developement progress

- add obliterate as an event, rm prioritized collection on obliteration

- add sample queue tests

- format spacing in readme

- fix and improve ci with caching & using nextest

- more queue tests

- :emitter_on is not an async function: correct this

- add worker state as an enum

- add worker tests

- don't use release flag in ci

- remove tokio_shared_rt and use a multithread runtime for worker tests

- fetch number of cpus

- use keydb and improved workflow

- support both sync and async callbacks

- add client to queue, support both favours of processors (sync and async)

- update examples and tests

- reorganise code structure

- make job-update progress sync

- update examples

- restore connection-example

- skipmap.len is just an len appromixation, use atomic counter for this instead

- Update README.md for clarity and formatting

- Format README.md with proper headings and lists

- make all errors public and the worker'id

- return an error if a worker is already running and a called to run is made

- make CollectionSuffix enum and worker_state field pub

- use redis instead of keydb in ci
try buildjet
wip: improve worker and fix them in ci

- add nextest-config file

- fix flaky test

- create a store trait and a redis store

- clean up and use dynamic store logic in queue-impl

- refactor to use dynamic stores

- add async-trait

- Integrate the Store trait into queue, worker and utils function

- add store to examples

- update tests with store

- clippy fix

- make collectionSuffix serialized as an u64 tag

- move event-stream-handle creating from store implementations

- complete Store trait impl for redis-store

- add rocksdb as a store with a feature flag

- dont run rocksdb-example in tests

- fix ci

- fix complete rocksdb-store::add_bulk methods

- add basic example with redis-store

- yield in task picker

- add an in-memory-store and its example

- add timed-map to use in the in-memory-store

- add timed locks to both stores

- use parking_lot primitives instead

- implement store.remove for in-memory-store && fix typo in JobState enum"

- simple improvement to in-memory-store

- refactor store.set_fields and move the ```move_to_state``` method out of the store

- clean and reduce store methods

- add some utility functions

- expose important methods from store

- clean up events && split functions in utils

- remove stream-handle, use emiiter directly to publish events for rocksdb and in-memory-store

- clippy fix@

- fix overflow and simple clean up

- add more fields to job-metrics

- fix out of range errors

- use a checked insert method

- use a delay-queue for timers instead of spawning task (previous timer)

- fix typo

- use tasktracker, add graceful shutdown

- add support for purging expired entries for store

- make redis-store optional

- make redis-store a default feature

- merge similar examples into a few with feature flags

- use new timedmap

- only purge expired when ther e is some expiring soon

- use atomiccell instead of arc-swap and use tokio-mutex for timed-map

- bug fix

- Refactor set_key to use Key directly

- clippy fix && reduce critical section for race condtion (when both mutexes are held

- purge both locks and jobs concurrenctly

- use length in purge_expired: inMemory

- use a better and faster mutex

- correct naming of metrics

- remove job from events, update examples and tests to reflct this

- add job metrics to complated event

- bug fixes

- cargo fmt

- minor bug fixes(panics) too

- update tests

- clippy fix in examples

- minor changes

- only use async version of xutex::Mutex

- enable disabling expirations on customTimedMap

- use semaphore to limit concurrency

- add delay from options to job_metrics

- bug fix: locks.len_exnpire is async

- update queue-metrics in rocksdb-store

- add the tracing crate

- add resoure span to the worker

- add logging to main_task and bug fix: stop main_loop on cancellation

- add tracing and tracing-subscriber to examples

- add logging to timers

- add logs and remodel loop and timer to promote delayed jobs too

- add a resource-span to queue

- clean up main loop

- wip

- clippy fix

- clean and add tracing to examples

- reduce verbosity of logging and refactor smt

- more logging

- uncomment tokio-console directives

- store joinhandle of running tasks instead of their ids only

- better debug formating for TimerType::PromoteJob

- gate tracing under a feature flag and make dep optional

- issue-templates

- PR template

- main_loop fix: only proceed processing if a permit is available

- make Timer public

- make a worker_opts a copy type

- clippy fix

- pass the right attempt for failed jobs

- clean & better usage of atomic ordering

- guard data in timed_maps with a parking_lot primitives

- use parking_lot mutex instead rwlock for in-memory-store

- maintain Acquire-Release ordering

- add criterion benchmarks using inmemory-store

- exclude library tests from benchmarks

- add an action for crition benchmarks

- add missing meta-data for action

- use the latest stable for benchmarks

- wip

- exclude library tests in benchmarks && fix caching

- add permissions for action to post results

- redis-store returns pause_count instead of failed_job cout

- catch panics from sync_processors

- add tests for panics in processors

- add tokio-metrics as a dependency

- add worker_metrics collection to worker and setup periodic heartbeat/collection timer

- add metrics collection to in-memory-store

- add metrics collection for redis-store

- expose worker-metrics methods from store

- collect metrics from timers

- add timestamps to track metrics updates

- minor metrics updates

- a clippy fix

- make all fields in WorkerMetrics pub

- make worker_metrics clonable and comparable

- export our timedmap implementation to the library users

- make the ```insert_expirable``` method sync

- don't hold on to mutex for long

- strictly use async-mutex event in sync code

- collective metrics for the user's processor function instead of the worker task

- Fix task metrics collection: instrument spawned future, fix TTL bug, fallback task_id, add test

Co-authored-by: spencerjibz <41294519+spencerjibz@users.noreply.github.com>

- Apply code review feedback: remove explicit type in loop, drop unused return value assignment

Co-authored-by: spencerjibz <41294519+spencerjibz@users.noreply.github.com>

- comments clean up

- clippy clean-up

- update Readme

- add asset folder with logo

- library-rename

- add extras to readme

- Add logo and project overview to README

Updated README to include logo and project description.

- library name correction in readme

- replace crossbeam-sub-crates(queue& utils)

with the main crossbeam crate

- replace usage of atomic_ref_cell

avoid panicking at runtime; use arcswap when needed

- Migrate workflows to Blacksmith

- clippy fixes

- fix clippy warnings

- add a helper macro for worker_tests

- add isolated tests for each store (inmemory or redis-store)

- add helper macros for queue-tests for diffrent stores

- run store tests in concurreny in ci

- fix flawky tests

- fix tests

- Add comprehensive Rustdoc documentation across the kiomq codebase

Co-authored-by: spencerjibz <41294519+spencerjibz@users.noreply.github.com>

- Complete documentation coverage: all public items documented, no missing-docs gaps

Co-authored-by: spencerjibz <41294519+spencerjibz@users.noreply.github.com>

- Add Rustdoc for RedisStore struct, fields, and public methods; doc fetch_redis_pass and get_queue_metrics

Co-authored-by: spencerjibz <41294519+spencerjibz@users.noreply.github.com>

- Center lib.rs header with div align=center and downshift all heading levels by one

Co-authored-by: spencerjibz <41294519+spencerjibz@users.noreply.github.com>

- Add logo image to lib.rs centered header block

Co-authored-by: spencerjibz <41294519+spencerjibz@users.noreply.github.com>

- Link RedisStore in top description alongside InMemoryStore

Co-authored-by: spencerjibz <41294519+spencerjibz@users.noreply.github.com>

- Move Tokio runtime requirements section before Installation in lib.rs docs

Co-authored-by: spencerjibz <41294519+spencerjibz@users.noreply.github.com>

- fix updating_metrics bug and add panics section to README

- Fix updating_metrics bug in README examples and add panics/framed section

Co-authored-by: spencerjibz <41294519+spencerjibz@users.noreply.github.com>

- Fix clippy warnings: visibility, casts, docs, match arms, async, and more

Co-authored-by: spencerjibz <41294519+spencerjibz@users.noreply.github.com>

- Fix remaining clippy errors and incorporate release-pipeline lint relaxation

Co-authored-by: spencerjibz <41294519+spencerjibz@users.noreply.github.com>

- typo


