/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.cfg.NotYetImplementedException;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.tool.schema.extract.spi.ExtractionContext;

public class OracleSqlReactiveInformationExtractorImpl extends AbstractReactiveInformationSchemaBasedExtractorImpl {

	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( PostgreSqlReactiveInformationExtractorImpl.class );

	public OracleSqlReactiveInformationExtractorImpl(ExtractionContext extractionContext) {
		super( extractionContext );
	}

	@Override
	protected String getResultSetTableTypesPhysicalTableConstant() {
		return "BASE TABLE";
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
	protected <T> T processIndexInfoResultSet(
			String catalog,
			String schema,
			String table,
			boolean unique,
			boolean approximate,
			ExtractionContext.ResultSetProcessor<T> processor) throws SQLException {
		// This functionality is not used by ORM.
		throw new NotYetImplementedException();
//		// This implementation is based on org.postgresql.jdbc.PgDatabaseMetaData#getIndexInfo.
//		// It excludes columns that are specified by DatabaseMetaData#getIndexInfo, but
//		// not specified by AbstractInformationExtractorImpl#processIndexInfoResultSet.
//
//		// TODO: How should the "approximate" parameter be used?
//		// org.postgresql.jdbc.PgDatabaseMetaData#getIndexInfo
//		// does not use that argument. It is currently ignored here as well.
//
//		// Generate the inner query first.
//		final StringBuilder innerQuery = new StringBuilder()
//				.append( "select ci.relname as index_name" )
//				.append( " , case i.indisclustered when true then " ).append( DatabaseMetaData.tableIndexClustered )
//				.append( " else case am.amname when 'hash' then " ).append( DatabaseMetaData.tableIndexHashed )
//				.append( " else " ).append( DatabaseMetaData.tableIndexOther ).append( " end" )
//				.append( " end as index_type" )
//				.append( " , (information_schema._pg_expandarray(i.indkey)).n as position" )
//				.append( " , ci.oid as ci_iod" )
//				.append( " from pg_catalog.pg_class ct" )
//				.append( " join pg_catalog.pg_namespace n on (ct.relnamespace = n.oid)" )
//				.append( " join pg_catalog.pg_index i on (ct.oid = i.indrelid)" )
//				.append( " join pg_catalog.pg_class ci on (ci.oid = i.indexrelid)" )
//				.append( " join pg_catalog.pg_am am on (ci.relam = am.oid)" )
//				.append( " where true" );
//
//		final List<Object> parameterValues = new ArrayList<>();
//
//		appendClauseAndParameterIfNotNullOrEmpty( " and n.nspname = ", schema, innerQuery, parameterValues );
//		appendClauseAndParameterIfNotNullOrEmpty( " and ct.relname = ", table, innerQuery, parameterValues );
//
//		if ( unique ) {
//			innerQuery.append( " AND i.indisunique = true" );
//		}
//
//		return getExtractionContext().getQueryResults(
//				"select tmp.index_name as " + getResultSetIndexNameLabel() +
//						", tmp.index_type as " + getResultSetIndexTypeLabel() +
//						", trim(both '\"' from pg_catalog.pg_get_indexdef(tmp.ci_iod, tmp.position, false)) as " + getResultSetColumnNameLabel() +
//						" from ( " + innerQuery + " ) tmp" +
//						" order by " + getResultSetIndexNameLabel() + ", tmp.position",
//				parameterValues.toArray(),
//				processor
//		);
	}

	@Override
	protected <T> T processImportedKeysResultSet(
			String catalog,
			String schema,
			String table,
			ExtractionContext.ResultSetProcessor<T> processor
	) throws SQLException {
		// This functionality is not used by ORM.
		throw new NotYetImplementedException();
		// This implementation is based on org.postgresql.jdbc.PgDatabaseMetaData#getImportedExportedKeys.
		// It excludes columns that are specified by DatabaseMetaData#getImportedKeys, but
		// not specified by AbstractInformationExtractorImpl#processImportedKeysResultSet.

//		final StringBuilder sb = new StringBuilder()
//				.append( "select null as " ).append( getResultSetPrimaryKeyCatalogLabel() )
//				.append( ", pkn.nspname as " ).append( getResultSetPrimaryKeySchemaLabel() )
//				.append( ", pkc.relname as " ).append( getResultSetPrimaryKeyTableLabel() )
//				.append( ", pka.attname as " ).append( getResultSetPrimaryKeyColumnNameLabel() )
//				.append( ", fka.attname as " ).append( getResultSetForeignKeyColumnNameLabel() )
//				.append( ", pos.n as " ).append( getResultSetColumnPositionColumn() )
//				.append( ", con.conname as " ).append( getResultSetForeignKeyLabel() )
//				.append( " from pg_catalog.pg_namespace pkn, pg_catalog.pg_class pkc, pg_catalog.pg_attribute pka" )
//				.append( ",  pg_catalog.pg_namespace fkn, pg_catalog.pg_class fkc, pg_catalog.pg_attribute fka" )
//				.append( ", pg_catalog.pg_constraint con" )
//				.append( ", pg_catalog.generate_series(1, cast( (select setting from pg_catalog.pg_settings where name='max_index_keys') as integer ) ) pos(n)" )
//				.append( " where pkn.oid = pkc.relnamespace and pkc.oid = pka.attrelid and pka.attnum = con.confkey[pos.n] and con.confrelid = pkc.oid" )
//				.append( " and fkn.oid = fkc.relnamespace and fkc.oid = fka.attrelid and fka.attnum = con.conkey[pos.n] and con.conrelid = fkc.oid" )
//				.append( " and con.contype = 'f' " );
//
//		final List<Object> parameterValues = new ArrayList<>();
//
//		appendClauseAndParameterIfNotNullOrEmpty( " and fkn.nspname = ", schema, sb, parameterValues );
//		appendClauseAndParameterIfNotNullOrEmpty( " and fkc.relname = ", table, sb, parameterValues );
//
//		// No need to order by catalog since it is always null.
//		sb.append( " order by pkn.nspname, pkc.relname, con.conname, pos.n" );
//
//		return getExtractionContext().getQueryResults( sb.toString(), parameterValues.toArray(), processor );
	}

	@Override
	protected <T> T processTableResultSet(
			String catalog,
			String schemaPattern,
			String tableNamePattern,
			String[] types,
			ExtractionContext.ResultSetProcessor<T> processor
	) throws SQLException {
		final String catalogColumn = getDatabaseCatalogColumnName(
				"table_catalog",
				"table_schema"
		);
		final String schemaColumn = getDatabaseSchemaColumnName(
				"table_catalog",
				"table_schema"
		);
		final StringBuilder sb = new StringBuilder()
				.append( "select null").append( " as " ).append( getResultSetCatalogLabel() )
				.append( " , " ).append( schemaColumn ).append( " as " ).append( getResultSetSchemaLabel() )
				.append( " , sat.table_name as " ).append( getResultSetTableNameLabel() )
				.append( " , 'TABLE' as " ).append( getResultSetTableTypeLabel() )
				.append( " , satc.comments as " ).append( getResultSetRemarksLabel() )
				// Hibernate ORM does not currently do anything with remarks,
				// so just return null for now.
				.append( " from sys.all_tables sat, sys.all_tab_comments satc where 1 = 1" );
		List<Object> parameterValues = new ArrayList<>();
		appendClauseAndParameterIfNotNullOrEmpty( " and " + schemaColumn + " like ", schemaPattern, sb, parameterValues );
		appendClauseAndParameterIfNotNullOrEmpty( " and sat.table_name like ", tableNamePattern, sb, parameterValues );
		appendClauseAndParameterIfNotNullOrEmpty( " and satc.table_name like ", tableNamePattern, sb, parameterValues );
		return getExtractionContext().getQueryResults( sb.toString(), parameterValues.toArray(), processor );
	}

	@Override
	protected <T> T processColumnsResultSet(
			String catalog,
			String schemaPattern,
			String tableNamePattern,
			String columnNamePattern,
			ExtractionContext.ResultSetProcessor<T> processor) throws SQLException {
		final StringBuilder sb = new StringBuilder()
				.append( "select c.table_name as " ).append( getResultSetTableNameLabel() )
				.append( ", c.column_name as " ).append( getResultSetColumnNameLabel() )
				.append( ", c." ).append( getInformationSchemaColumnsDataTypeColumn() )
				.append( " as " ).append( getResultSetTypeNameLabel() )
				.append( ", null as " ).append( getResultSetColumnSizeLabel() )
				// Column size is fairly complicated to get out of information_schema
				// and likely to be DB-dependent. Currently, Hibernate ORM does not use
				// column size for anything, so for now, just return null.
				.append( ", null as " ) .append( getResultSetDecimalDigitsLabel() )
				// Decimal digits is fairly complicated to get out of information_schema
				// and likely to be DB-dependent. Currently, Hibernate ORM does not use
				// decimal digits for anything, so for now, just return null.
				.append( ", c.nullable as " ).append( getResultSetIsNullableLabel() )
				.append( ", null as " ).append( getResultSetSqlTypeCodeLabel() )
				// Currently, Hibernate ORM only uses
				// the SQL type code for SchemaMigrator to check if a column
				// type in the DB is consistent with what is computed in
				// Hibernate's metadata for the column. ORM also considers
				// the same column type name as a match, so the SQL code is
				// optional. For now, just return null for the SQL type code.
				.append( " from sys.user_tab_columns c inner join sys.user_objects o" )
				.append( " on c.table_name = o.object_name and o.object_type in ( 'TABLE', 'VIEW' ) " )
				// o.object_type = 'TABLE' is for a user-defined table
				// o.object_type = 'VIEW' is for a view
				.append( " where 1 = 1" );

		final List<Object> parameterValues = new ArrayList<>();
		final String catalogColumn = getDatabaseCatalogColumnName(
				"table_catalog",
				"table_schema"
		);
		final String schemaColumn = getDatabaseSchemaColumnName(
				"table_catalog",
				"table_schema"
		);
		appendClauseAndParameterIfNotNullOrEmpty( " and " + catalogColumn + " = " , catalog, sb, parameterValues );
		appendClauseAndParameterIfNotNullOrEmpty( " and table_name like " , tableNamePattern, sb, parameterValues );

		sb.append(  " order by table_name, column_name, column_id" );

		return getExtractionContext().getQueryResults( sb.toString(), parameterValues.toArray(), processor );
	}

	protected String getDatabaseSchemaColumnName(String catalogColumnName, String schemaColumnName ) {
		return "sat.owner";
	}

	protected String getResultSetIsNullableLabel() {
		return "nullable";
	}

	@Override
	protected int dataTypeCode(String typeName) {
		// ORACLE only supports "float" sql type for double precision
		// so return code for double for both double and float column types
		if ( typeName.equalsIgnoreCase( "float" ) ||
				typeName.toLowerCase().startsWith( "double" ) ) {
			return Types.DOUBLE;
		}
		return super.dataTypeCode( typeName );
	}
}
