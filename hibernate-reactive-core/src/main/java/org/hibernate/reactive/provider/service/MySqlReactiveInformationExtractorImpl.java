/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.tool.schema.extract.spi.ExtractionContext;

/**
 * An implementation of {@link AbstractReactiveInformationSchemaBasedExtractorImpl}
 * specifically for MySQL that obtains metadata from MySQL's non-standard
 * information_schema tables.
 *<p/>
 * MySQL's schema is actually a catalog. MySQL's implementation of
 * {@link java.sql.DatabaseMetaData} automatically changes catalog
 * arguments to refer to schema columns instead. Unfortunately,
 * MySQL's information_schema stores catalog names in
 * a schema name column. This class works around that idiosyncrasy.
 *
 * @author Gail Badner
 */
public class MySqlReactiveInformationExtractorImpl extends AbstractReactiveInformationSchemaBasedExtractorImpl {

	public MySqlReactiveInformationExtractorImpl(ExtractionContext extractionContext) {
		super( extractionContext );
	}

	@Override
	protected String getResultSetTableTypesPhysicalTableConstant() {
		return "BASE TABLE";
	}

	protected String getDatabaseCatalogColumnName(String catalogColumnName, String schemaColumnName ) {
		return schemaColumnName;
	}

	protected String getDatabaseSchemaColumnName(String catalogColumnName, String schemaColumnName ) {
		return null;
	}

	@Override
	protected <T> T processPrimaryKeysResultSet(
			String catalogFilter,
			String schemaFilter,
			Identifier tableName,
			ExtractionContext.ResultSetProcessor<T> processor) throws SQLException {
		// This functionality is not used by ORM.
		throw new UnsupportedOperationException();
	}

	@Override
	protected <T> T processCatalogsResultSet(ExtractionContext.ResultSetProcessor<T> processor) throws SQLException {
		// MySQL does not implement information_schema.information_schema_catalog_name
		// so the superclass method won't work.

		// MySQL's information_schema stores the catalog name in
		// the schema_name column. This is why the schema_name column is
		// returned with the column label, getResultSetCatalogLabel().
		return getExtractionContext().getQueryResults(
				String.format(
						"select schema_name as %s from information_schema.information_schema.schemata",
						getResultSetCatalogLabel()
				),
				null,
				processor
		);
	}

	@Override
	protected <T> T processIndexInfoResultSet(
			String catalog,
			String schema,
			String table,
			boolean unique,
			boolean approximate,
			ExtractionContext.ResultSetProcessor<T> processor) throws SQLException {

		// According to the MySQL 8.0 documentation about
		// information_schema.statistics in the "Notes" section
		// (https://dev.mysql.com/doc/mysql-infoschema-excerpt/8.0/en/information-schema-statistics-table.html),
		//
		// "There is no standard INFORMATION_SCHEMA table for indexes.
		// The MySQL column list is similar to what SQL Server 2000
		// returns for sp_statistics, except that QUALIFIER and OWNER
		// are replaced with CATALOG and SCHEMA, respectively"
		//
		// Because the information_schema.statistics table is specific to MySQL,
		// it is included in this class (instead of the superclass).

		// According to the MySQL documentation, ensuring that statistics
		// are up-to-date is outside the scope of SchemaMigrator.

		// "Columns in STATISTICS that represent table statistics hold cached
		// values. The information_schema_stats_expiry system variable defines
		// the period of time before cached table statistics expire. The default
		// is 86400 seconds (24 hours). If there are no cached statistics or
		// statistics have expired, statistics are retrieved from storage
		// engines when querying table statistics columns. To update cached
		// values at any time for a given table, use ANALYZE TABLE. To always
		// retrieve the latest statistics directly from storage engines,
		// set information_schema_stats_expiry=0. For more information, see
		// Optimizing INFORMATION_SCHEMA Queries."

		// TODO: ORM currently passes true for the "approximate" argument,
		// but that may be a bug.

		// Documentation for INDEX_TYPE column says the possible values are:
		// BTREE, FULLTEXT, HASH, RTREE. Since there is no index type equivalent to
		// DatabaseMetaData.tableIndexStatistic, the value, -1, will be returned
		// for the column with label getResultSetIndexTypeLabel().

		final StringBuilder sb = new StringBuilder()
				.append("select index_name as " ).append( getResultSetIndexNameLabel() )
				.append( ", -1 as " ).append( getResultSetIndexTypeLabel() )
				.append( ", column_name as " ).append( getResultSetColumnNameLabel() )
				.append( " from information_schema.statistics where true" );

		final List<Object> parameters = new ArrayList<>();
		// MySQL's information_schema.statistics stores the catalog name in
		// the schema_name column. This is why the table_schema column is
		// is constrained to be catalog value.
		assert schema == null || schema.isEmpty();
		appendClauseAndParameterIfNotNullOrEmpty( " and table_schema = ", catalog, sb, parameters );
		appendClauseAndParameterIfNotNullOrEmpty( " and table_name = ", table, sb, parameters );

		if ( unique ) {
			appendClauseAndParameterIfNotNullOrEmpty( " and non_unique = ", 0, sb, parameters );
		}

		sb.append( " order by index_name, seq_in_index" );

		return getExtractionContext().getQueryResults( sb.toString(), parameters.toArray(), processor );
	}

	@Override
	protected <T> T processImportedKeysResultSet(
			String catalog, String schema, String table, ExtractionContext.ResultSetProcessor<T> processor)
			throws SQLException {

		// MySQL's information_schema.key_column_usage stores the catalog name in
		// the schema_name column. This is why the referenced_table_schema column is
		// returned with the label returned by #getResultSetPrimaryKeyCatalogLabel()
		// and null is returned with the label returned by #getResultSetPrimaryKeySchemaLabel().

		final StringBuilder sb = new StringBuilder()
				.append( "select constraint_name as " ).append( getResultSetForeignKeyLabel() )
				.append( ", referenced_table_schema as " ).append( getResultSetPrimaryKeyCatalogLabel() )
				.append( ", null as " ).append( getResultSetPrimaryKeySchemaLabel() )
				.append( ", referenced_table_name as " ).append( getResultSetPrimaryKeyTableLabel() )
				.append( ", referenced_column_name as ").append( getResultSetPrimaryKeyColumnNameLabel() )
				.append( ", column_name as " ).append( getResultSetForeignKeyColumnNameLabel() )
				.append( " from information_schema.key_column_usage" )
				// Exclude primary keys, which do not have a referenced table.
				.append( " where referenced_table_name is not null" );


		// Now add constraints for the requested catalog/schema/table

		final List<Object> parameters = new ArrayList<>();
		final List<String> orderByList = new ArrayList<>();

		// MySQL's information_schema.statistics stores the catalog name in
		// the schema_name column. This is why the table_schema column
		// is constrained to be catalog value.
		if ( appendClauseAndParameterIfNotNullOrEmpty( " and table_schema = ", catalog, sb, parameters ) ) {
			orderByList.add( "table_schema" );
		}
		if ( appendClauseAndParameterIfNotNullOrEmpty( " and table_name = ", table, sb, parameters ) ) {
			orderByList.add( "table_name" );
		}
		orderByList.add( "ordinal_position" );

		sb.append( " order by " ).append( orderByList.get( 0 ) );
		for ( int i = 1 ; i < orderByList.size() ; i++ ) {
			sb.append( ", " ).append( orderByList.get( i ) );
		}

		return getExtractionContext().getQueryResults( sb.toString(), parameters.toArray(), processor );
	}
}
