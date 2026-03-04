## [unreleased]

### 🚀 Features

- Pass down removal options from queue if not specified for a job
- Collective metrics for the user's processor function instead of the worker task

### 🐛 Bug Fixes

- Failing missed deadline jobs
- Correctly serialize queue-events
- Promoting delayed jobs
- Counter bugs in in-memory and rocksdb store
- Set return value & add timeout to listening to events
- Timedmap tests
- Inmemory-store, only update stored-metrics from publish_event
- Updating prioritized and failed job counts in queue-metrics
- Redis-store returns pause_count instead of failed_job cout
- Typo in fetch_worker_metrics in redis-store
- Cargo fmt
- Fetching_metrics from redis-store

### 💼 Other

- Add support for skipping first_tick
- Wip
- Support either pubsub or redis_stream as queue-event-stream
- Support for both modes
- Support both sync and async callbacks
- Skipmap.len is just an len appromixation, use atomic counter for this instead
- Merge similar examples into a few with feature flags
- Use a better and faster mutex
- Clean up main loop
- Issue-templates
- Pass the right attempt for failed jobs
- Collect metrics from timers
- Make the ```insert_expirable``` method sync
- Don't hold on to mutex for long
- Fix clippy warnings
- Fix updating_metrics bug and add panics section to README

### 📚 Documentation

- Add doc comment and example to the framed re-export in lib.rs
- Condense README to match lib.rs conciseness (44% shorter)
- Restore Progress updates section and improve Panics & errors wording
- Add compact table of contents to README

### 🧪 Testing

- Return an error if a worker is already running and a called to run is made
