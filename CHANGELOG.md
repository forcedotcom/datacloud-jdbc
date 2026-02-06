# Changelog

## [0.42.1](https://github.com/forcedotcom/datacloud-jdbc/compare/v0.42.0...v0.42.1) (2026-02-06)


### Bug Fixes

* Remove comments from JDBC driver version ([#153](https://github.com/forcedotcom/datacloud-jdbc/issues/153)) ([32f7216](https://github.com/forcedotcom/datacloud-jdbc/commit/32f7216bac41cdf6c2621d079f8c8fd77cd49a16))
* resolve release workflow validation error for tag expression ([#156](https://github.com/forcedotcom/datacloud-jdbc/issues/156)) ([449ce0d](https://github.com/forcedotcom/datacloud-jdbc/commit/449ce0d5ba517e42f6f49eaaaeafcb1712b286b8))

## [0.42.0](https://github.com/forcedotcom/datacloud-jdbc/compare/0.41.0...v0.42.0) (2026-02-05)


### Features

* Add SSL/TLS Support to DataCloud JDBC Driver ([#89](https://github.com/forcedotcom/datacloud-jdbc/issues/89)) ([9123c1b](https://github.com/forcedotcom/datacloud-jdbc/commit/9123c1b8981f425a9477f5a219d5472fe7f34213))
* implement automated release pipeline using Release Please ([#139](https://github.com/forcedotcom/datacloud-jdbc/issues/139)) ([268eb2e](https://github.com/forcedotcom/datacloud-jdbc/commit/268eb2eaf7979587e919a02503786ed0c06fe41b))
* Provide a low level Async Interface ([#150](https://github.com/forcedotcom/datacloud-jdbc/issues/150)) ([b1fba90](https://github.com/forcedotcom/datacloud-jdbc/commit/b1fba90fdf56db4414f1a289ddf3419ce819d6b4))
* Support PreparedStatement.getMetaData() ([#151](https://github.com/forcedotcom/datacloud-jdbc/issues/151)) ([52448d9](https://github.com/forcedotcom/datacloud-jdbc/commit/52448d9ee0c82d17c70af651eb8282658a38ead0))


### Bug Fixes

* Breaking - Remove data loss for slow readers ([#142](https://github.com/forcedotcom/datacloud-jdbc/issues/142)) ([1ff41dc](https://github.com/forcedotcom/datacloud-jdbc/commit/1ff41dc64c5619d76eeb7cb5e51a2f6d91c6f38d))
* **ci:** remove explicit SNAPSHOT version from snapshot workflow ([#141](https://github.com/forcedotcom/datacloud-jdbc/issues/141)) ([b30c3fe](https://github.com/forcedotcom/datacloud-jdbc/commit/b30c3fec1e1db2ea189e582774303ebe8bb8b5c1))
* **ci:** synchronize Release Please state to resolve empty change set error ([#143](https://github.com/forcedotcom/datacloud-jdbc/issues/143)) ([8518b23](https://github.com/forcedotcom/datacloud-jdbc/commit/8518b23a07ff27e15ea0900e330303d57705718a))
* **ci:** use simple release-type with extra-files for Gradle project ([#145](https://github.com/forcedotcom/datacloud-jdbc/issues/145)) ([5a8aac4](https://github.com/forcedotcom/datacloud-jdbc/commit/5a8aac455df8668964ce38739e1865363884777a))
* Remove comments from JDBC driver version ([#153](https://github.com/forcedotcom/datacloud-jdbc/issues/153)) ([32f7216](https://github.com/forcedotcom/datacloud-jdbc/commit/32f7216bac41cdf6c2621d079f8c8fd77cd49a16))


### Performance Improvements

* Optimize ResultSet column lookup with HashMap-based indexing ([#138](https://github.com/forcedotcom/datacloud-jdbc/issues/138)) ([b8c5eb9](https://github.com/forcedotcom/datacloud-jdbc/commit/b8c5eb96f3cf57bb809d947f5d45cd32f15bfe79))

## Changelog
