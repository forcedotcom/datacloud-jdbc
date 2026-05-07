/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */

/**
 * Canonical internal type model for the Data Cloud JDBC driver.
 *
 * <p>Every Arrow, pg_catalog, Spark, and prepared-statement conversion routes through
 * {@link com.salesforce.datacloud.jdbc.protocol.data.HyperType}. The translation to
 * {@link java.sql.JDBCType} / {@link java.sql.Types} happens only at the JDBC API boundary (in
 * {@link com.salesforce.datacloud.jdbc.core.metadata.DataCloudResultSetMetaData} via
 * {@link com.salesforce.datacloud.jdbc.core.types.HyperTypes}); to Arrow only at the Arrow
 * boundary (via {@link com.salesforce.datacloud.jdbc.protocol.data.HyperTypeToArrow} and
 * {@link com.salesforce.datacloud.jdbc.protocol.data.ArrowToHyperTypeMapper}); to Spark only at
 * the Spark boundary ({@code spark-datasource-core}).
 *
 * <p>This package consolidates the four type graphs described by the original design:
 *
 * <ul>
 *   <li>Arrow {@link org.apache.arrow.vector.types.pojo.Field} {@code ->} HyperType — see
 *       {@link com.salesforce.datacloud.jdbc.protocol.data.ArrowToHyperTypeMapper}.
 *   <li>{@code pg_catalog.format_type()} string {@code ->} HyperType — see
 *       {@link com.salesforce.datacloud.jdbc.protocol.data.PgCatalogTypeParser}.
 *   <li>HyperType {@code ->} Arrow {@link org.apache.arrow.vector.types.pojo.FieldType} — see
 *       {@link com.salesforce.datacloud.jdbc.protocol.data.HyperTypeToArrow}.
 *   <li>HyperType {@code ->} JDBC metadata (type code, type name, java class, precision, scale,
 *       display size, case-sensitivity, signedness, …) — see
 *       {@link com.salesforce.datacloud.jdbc.core.types.HyperTypes}.
 * </ul>
 *
 * <p>New places that need to reason about column types should add to
 * {@link com.salesforce.datacloud.jdbc.core.types.HyperTypes} rather than switching on
 * {@link java.sql.JDBCType} directly.
 */
package com.salesforce.datacloud.jdbc.core.types;
