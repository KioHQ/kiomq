### kio-mq

[![CI](https://github.com/spencerjibz/kio-mq/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/spencerjibz/kio-mq/actions/workflows/ci.yml)

A simple async background task queue based on Redis (inspired by BullMQ).

### Development Status

- [x] Queue with job creation, waiting and pausing
- [x] Jobs (serialisable directly from redis)
- [x] Worker implementation with support for only async tasks
- [x] Queue Events(listenes on either queues or workers)

### Testing

Run tests using cargo as follows:
`cargo test`

### License

MIT License (MIT), see [LICENSE](LICENSE)

