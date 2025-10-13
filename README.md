### kio-mq

[![CI](https://github.com/spencerjibz/kio-mq/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/spencerjibz/kio-mq/actions/workflows/ci.yml)
A simple async background task queue based on Redis (inspired by BullMQ).

### Development Status

1. Queue

- [x] Job Creation both (single and bulk)
- [x] Pausing both the queue and workers(when idle)
- [x] Delaying jobs with custom delay(even with cron)
- [x] Priority Support
- [x] Emitting Global Queue Events
- [x] Cron scheduling for both initial delays and backoff

2. Jobs

- [x] Serialisable directly from Redis
- [x] Updating Progress whilst processing the job.
- [x] Repeatable with Repeat Options
- [x] Retryable on failure with Backoff Options

3. Worker

- [x] Runing jobs to completion
- [x] Pause when queue is idle and vice versa
- [x] In-memory queue to run pick up delayed jobs
- [x] Schedule jobs for retrial (failed)
- [x] Clean up completed jobs or failed from queue_removal_options

### Testing

Run tests using cargo as follows:
`cargo test `

### License

MIT License (MIT), see [LICENSE](LICENSE)
