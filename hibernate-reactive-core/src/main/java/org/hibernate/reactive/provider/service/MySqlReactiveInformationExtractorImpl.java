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
import org.hibernate.cfg.NotYetImplementedException;
import org.hibernate.tool.schema.extract.spi.ExtractionContext;

/**
 * An implementation of {@link AbstractReactiveInformationSchemaBasedExtractorImpl}
 * specifically for MySQL that obtains metadata from MySQL's non-standard
 * information_schema tables, when it is not available from PosgreSQL's
 * information_schema.
 *<p/>
 * MySQL's schema is actually a catalog. MySQL's implementation of
 * {@link java.sql.DatabaseMetaData} automatically changes catalog
 * arguments to refer to a schema columns instead. Unfortunately,
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
		return "null";
	}

	@Override
	protected <T> T processPrimaryKeysResultSet(
			String catalogFilter,
			String schemaFilter,
			Identifier tableName,
			ExtractionContext.ResultSetProcessor<T> processor) throws SQLException {
		// This functionality is not used by ORM.
		throw new NotYetImplementedException();
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
		// MySQL's information_schema stores the catalog name in
		// the schema_name column. This is why the schema_name column is
		// returned with the column label, getResultSetCatalogLabel().
		final String catalogColumnNameClause =
				" and " +
						getDatabaseCatalogColumnName( "table_catalog", "table_schema" ) +
						" = ";
		appendClauseAndParameterIfNotNullOrEmpty( catalogColumnNameClause, catalog, sb, parameters );
		assert schema == null || schema.isEmpty();
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

		// All necessary information can be found in information_schema.key_column_usage,
		// *except* for the catalog for the primary key that is imported.
		// The catalog for the primary key is available from information_schema.referential_constraints
		// in the unique_constraint_catalog column.

		// The extensive join with information_schema.referential_constraints is done
		// in order to return that catalog for the primary key. If we can assume that
		// the catalog for the primary key is the same as the constraint_catalog column in
		// information_schema.key_column_usage, then the join can be removed.

		final String referencedTableCatalogColumn = getDatabaseCatalogColumnName(
				"r.unique_constraint_catalog",
				"k.referenced_table_schema"
		);
		final String referencedTableSchemaColumn = getDatabaseSchemaColumnName(
				"r.unique_constraint_catalog",
				"k.referenced_table_schema"
		);

		final StringBuilder sb = new StringBuilder()
				.append( "select k.constraint_name as " ).append( getResultSetForeignKeyLabel() )
				.append( ", " ).append( referencedTableCatalogColumn ).append( " as " ).append( getResultSetPrimaryKeyCatalogLabel() )
				.append( ", " ).append( referencedTableSchemaColumn ).append( " as " ).append( getResultSetPrimaryKeySchemaLabel() )
				.append( ", k.referenced_table_name as " ).append( getResultSetPrimaryKeyTableLabel() )
				.append( ", k.referenced_column_name as ").append( getResultSetPrimaryKeyColumnNameLabel() )
				.append( ", k.column_name as " ).append( getResultSetForeignKeyColumnNameLabel() )
				.append( " from information_schema.key_column_usage k, information_schema.referential_constraints r" )
				.append( " where true" );

		// The following are join constraints that may be unnecessary
		sb.append( " and k.constraint_catalog = r.constraint_catalog" )
				.append( " and k.constraint_schema = r.constraint_schema" )
				.append( " and k.constraint_name = r.constraint_name" )
				.append( " and k.table_name = r.table_name" )
				.append( " and k.referenced_table_name = r.referenced_table_name" );

		// Now add constraints for the requested catalog/schema/table

		final List<Object> parameters = new ArrayList<>();
		final List<String> orderByList = new ArrayList<>();

		final String catalogColumn = getDatabaseCatalogColumnName(
				"k.table_catalog",
				"k.table_schema"
		);
		final String catalogColumnClause = " and " + catalogColumn + " = ";
		if ( appendClauseAndParameterIfNotNullOrEmpty( catalogColumnClause, catalog, sb, parameters ) ) {
			orderByList.add( catalogColumn );
		}
		final String schemaColumn = getDatabaseSchemaColumnName(
				"k.table_catalog",
				"k.table_schema"
		);
		final String schemaColumnClause = " and " + schemaColumn + " = ";
		if ( appendClauseAndParameterIfNotNullOrEmpty( schemaColumnClause, schema, sb, parameters ) ) {
			orderByList.add( schemaColumn );
		}
		if ( appendClauseAndParameterIfNotNullOrEmpty( " and k.table_name = ", table, sb, parameters ) ) {
			orderByList.add( "k.table_name" );
		}
		orderByList.add( "k.ordinal_position" );

		if ( orderByList.size() > 0 ) {
			sb.append( " order by " ).append( orderByList.get( 0 ) );
			for ( int i = 1 ; i < orderByList.size() ; i++ ) {
				sb.append( ", " ).append( orderByList.get( i ) );
			}
		}

		return getExtractionContext().getQueryResults( sb.toString(), parameters.toArray(), processor );
	}
}
