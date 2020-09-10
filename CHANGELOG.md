# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Types of changes
* _Added_ for new features.
* _Changed_ for changes in existing functionality.
* _Deprecated_ for soon-to-be removed features.
* _Removed_ for now removed features.
* _Fixed_ for any bug fixes.
* _Security_ in case of vulnerabilities.


## [Unreleased]

## [0.5.0] - 2020-09-10

### Added
- Add the `rustls-tls` TLS backend
- Add the `Client::set_tls_domain` method

### Changed
- The `tls` feature is not on by default. There are now two potential TLS backends `native-tls` or `rustls-tls`.
- Rename `Client::set_tls_connector` to `Client::set_tls_config`

## [0.4.4] - 2020-08-14

### Added
- The `Client::request_with_timeout` method #25

## [0.4.3] - 2019-04-13

### Added
- Subject constructor #18

### Fixed
- Disconnect on TCP error #24

## [0.4.2] - 2019-12-19

## [0.4.1] - 2019-12-17

### Added

- `not_connected` method to `Error` type

### Fixed

- Wait for `OK+` when connecting in verbose mode
- Check if client is connected before erring due to exceeding max payload.

## [0.4.0] - 2019-12-16

### Added

- TLS support
- Verify a new connection with a ping-pong exchange
- Allow `Address`to be looked up through DNS

### Changed

- Address is not based on `SocketAddr`
- Update `tokio`, `futures`, and `bytes` dependencies
- Use single subject for request-reply pattern, see more details on the design [here](https://github.com/nats-io/nats.go/issues/294)

## [0.3.1] - 2019-10-07

### Changed

- Implement backpressure in subscriber
- Update dependencies
