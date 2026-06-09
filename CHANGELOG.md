# Changelog

## [1.0.1](https://github.com/forcedotcom/datacloud-jdbc/compare/v1.0.0...v1.0.1) (2026-06-09)


### Bug Fixes

* **ci:** bump codecov-action v5 -&gt; v6 for keybase GPG migration ([#192](https://github.com/forcedotcom/datacloud-jdbc/issues/192)) ([3169997](https://github.com/forcedotcom/datacloud-jdbc/commit/3169997994efedeea1997a8bf79964cc40e43f90))

## [1.0.0](https://github.com/forcedotcom/datacloud-jdbc/compare/v0.42.2...v1.0.0) (2026-05-22)


### ⚠ BREAKING CHANGES

* `DataCloudResultSet` is now a class instead of an interface; `StreamingResultSet`, `DataCloudMetadataResultSet`, `SimpleResultSet`, `ColumnAccessor` are removed; metadata int-column `getDate`/`getTime`/`getTimestamp` throw `SQLException` (was `UnsupportedOperationException`); `getTypeInfo()` boolean columns are typed `BOOLEAN` instead of `VARCHAR` (`getObject` returns `Boolean`, not `String`); `getColumnTypeName` on metadata result sets returns the JDBC type name (`VARCHAR`/`SMALLINT`/`INTEGER`/`BOOLEAN`) instead of the prior Hyper-flavored labels (`TEXT`/`SHORT`/`INTEGER`/`BOOL`); `ps.setObject` with `Types.VARCHAR` rejects non-String/byte[] payloads; integer-family and DECIMAL setters reject out-of-range values instead of silently narrowing.

### Features

* add support for using CDP auth token directly ([#177](https://github.com/forcedotcom/datacloud-jdbc/issues/177)) ([ecce116](https://github.com/forcedotcom/datacloud-jdbc/commit/ecce116d053a495c3f6d8ca8bbde36c7c0d3c4d9))
* Avatical Removal Part 3/3 - Remove Avatica dependency completely ([#166](https://github.com/forcedotcom/datacloud-jdbc/issues/166)) ([0e7d912](https://github.com/forcedotcom/datacloud-jdbc/commit/0e7d912c4f9de5b16ae915e70edfb04cf5fac1ee))
* expose DataCloudStatement.getQueryStatus() with execution stats ([#178](https://github.com/forcedotcom/datacloud-jdbc/issues/178)) ([d4b1dcf](https://github.com/forcedotcom/datacloud-jdbc/commit/d4b1dcf5177ae5a13dbdfe41efe91054b623e973))
* Expose query execution statistics in QueryStatus ([#164](https://github.com/forcedotcom/datacloud-jdbc/issues/164)) ([f01c77a](https://github.com/forcedotcom/datacloud-jdbc/commit/f01c77aacec47f693095b0591ceb515192c9d302))
* implement timezone and timestamp handling with JDBC 4.2 support ([#158](https://github.com/forcedotcom/datacloud-jdbc/issues/158)) ([d0301f5](https://github.com/forcedotcom/datacloud-jdbc/commit/d0301f59f3ee8ace08fa163fa3b3f0905c4e921b))
* Improve authentication error logging and retry handling ([#140](https://github.com/forcedotcom/datacloud-jdbc/issues/140)) ([0d42ad1](https://github.com/forcedotcom/datacloud-jdbc/commit/0d42ad151ff782818a568f59a432611b8a60dc81))
* Improve virtual thread compatibility ([#165](https://github.com/forcedotcom/datacloud-jdbc/issues/165)) ([18d9f38](https://github.com/forcedotcom/datacloud-jdbc/commit/18d9f38f02f2e92d8dd9420cdd71d0f93e7a521f))


### Bug Fixes

* async interrupt race condition in SyncIteratorAdapter ([#170](https://github.com/forcedotcom/datacloud-jdbc/issues/170)) ([e81a0e6](https://github.com/forcedotcom/datacloud-jdbc/commit/e81a0e6f179e192520d612ced5143bb8eab62d2e))
* correct DatabaseMetaData.getTableTypes() to return table type names ([#162](https://github.com/forcedotcom/datacloud-jdbc/issues/162)) ([a5399bb](https://github.com/forcedotcom/datacloud-jdbc/commit/a5399bb426b57a1652d92469d70f09f3124ca046))
* don't return garbage values instead of null under arrow.enable_null_check_for_get=false ([#187](https://github.com/forcedotcom/datacloud-jdbc/issues/187)) ([a3f8712](https://github.com/forcedotcom/datacloud-jdbc/commit/a3f87122c74e3782c8e7dd5b586690298d923703))
* JDBC spec compliance and small metadata bug fixes ([#171](https://github.com/forcedotcom/datacloud-jdbc/issues/171)) ([5a78b40](https://github.com/forcedotcom/datacloud-jdbc/commit/5a78b40c11d4c9d086a9ae51a93c5814dec72958))
* propagate caller ThreadLocals to follow-up gRPC calls in async iterators ([#181](https://github.com/forcedotcom/datacloud-jdbc/issues/181)) ([7ac79f8](https://github.com/forcedotcom/datacloud-jdbc/commit/7ac79f8eff319e5a11163db5d1a92bc0491a5b00))
* support getObject(Class) with identity class type in QueryJDBCAccessor ([#186](https://github.com/forcedotcom/datacloud-jdbc/issues/186)) ([f47714f](https://github.com/forcedotcom/datacloud-jdbc/commit/f47714f56ff98ac6f6b55526a382afb716d7871a))
* Upgrade 3PP versions and fix gRPC stream leak exposed by gRPC 1.80 ([#168](https://github.com/forcedotcom/datacloud-jdbc/issues/168)) ([c0d2086](https://github.com/forcedotcom/datacloud-jdbc/commit/c0d2086db8e6dc4f548a8e7e2ae61e56bb034b65))


### Code Refactoring

* unify ResultSet implementations on Arrow-backed path ([#175](https://github.com/forcedotcom/datacloud-jdbc/issues/175)) ([9760106](https://github.com/forcedotcom/datacloud-jdbc/commit/97601061cc3dd97deb40a8c3490a38ef65d644ae))

## [0.42.2](https://github.com/forcedotcom/datacloud-jdbc/compare/v0.42.1...v0.42.2) (2026-03-05)


### Bug Fixes

* allow refresh-token auth without userName ([#157](https://github.com/forcedotcom/datacloud-jdbc/issues/157)) ([97bc515](https://github.com/forcedotcom/datacloud-jdbc/commit/97bc51504e1284d9b9a4f825d61d943ea0c6019a))
* properly handle thread interrupts during gRPC channel shutdown ([#159](https://github.com/forcedotcom/datacloud-jdbc/issues/159)) ([b0a96d1](https://github.com/forcedotcom/datacloud-jdbc/commit/b0a96d1a6e196f2badafc207ee3c03b1447d37c0))

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
