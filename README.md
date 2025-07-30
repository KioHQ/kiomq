### Kiomq

[![CI](https://github.com/spencerjibz/kio-mq/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/spencerjibz/kio-mq/actions/workflows/ci.yml)

A simple async background queue based on redis (inspired by bullmq).

### Development Status

- [x] Queue with job creation, waiting and pausing
- [x] Jobs (serialisable straight from redis)
- [x] worker implementation with support for only async tasks

### Testing

Run tests using cargo as follows:
`cargo test`

### License

MIT License (MIT), see [LICENSE](LICENSE)
