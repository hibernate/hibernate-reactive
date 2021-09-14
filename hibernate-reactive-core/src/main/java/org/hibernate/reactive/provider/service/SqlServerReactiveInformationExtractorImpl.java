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
 * specifically for SQL Server that obtains metadata from SQL Server's system
 * tables, when it is not available from SQL Server's information_schema.
 *
 * @author Gail Badner
 */
public class SqlServerReactiveInformationExtractorImpl extends AbstractReactiveInformationSchemaBasedExtractorImpl  {

	public SqlServerReactiveInformationExtractorImpl(ExtractionContext extractionContext) {
		super( extractionContext );
	}

	// TODO: All queries on an information_schema view that return a schema name
	// or constrain on a schema name for a database object should need be rewritten to join with
	// sys.objects.
	//
	// For example, documentation for information_schema.tables says the
	// following for the table_schema:
	// "** Important ** only reliable way to find the schema of an object
	// is to query the sys.objects catalog view. INFORMATION_SCHEMA views
	// could be incomplete since they are not updated for all new features."
	// (See https://docs.microsoft.com/en-us/sql/relational-databases/system-information-schema-views/tables-transact-sql?view=sql-server-ver15&viewFallbackFrom=sql-server-ver19)

	// #processSchemaResultSet in the superclass is probably OK as is
	// because it uses information_schema.schemata and it is the
	// schema itself that is the data object. Documentation says
	// the schema owner may incomplete, but the schema owner is not
	// needed.
	// (See https://docs.microsoft.com/en-us/sql/relational-databases/system-information-schema-views/schemata-transact-sql?view=sql-server-ver15&viewFallbackFrom=sql-server-ver19)

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
				.append( "select ").append( catalogColumn ).append( " as " ).append( getResultSetCatalogLabel() )
				.append( " , " ).append( schemaColumn ).append( " as " ).append( getResultSetSchemaLabel() )
				.append( " , table_name as " ).append( getResultSetTableNameLabel() )
				.append( " , table_type as " ).append( getResultSetTableTypeLabel() )
				.append( " , null as " ).append( getResultSetRemarksLabel() )
				// Remarks are not available from information_schema.
				// Hibernate ORM does not currently do anything with remarks,
				// so just return null for now.
				.append( " from information_schema.tables where 1 = 1" );
		List<Object> parameterValues = new ArrayList<>();
		appendClauseAndParameterIfNotNullOrEmpty( " and " + catalogColumn + " = ", catalog, sb, parameterValues );
		appendClauseAndParameterIfNotNullOrEmpty( " and " + schemaColumn + " like ", schemaPattern, sb, parameterValues );
		appendClauseAndParameterIfNotNullOrEmpty( " and table_name like ", tableNamePattern, sb, parameterValues );

		if ( types != null && types.length > 0 ) {
			appendClauseAndParameterIfNotNullOrEmpty(
					" and table_type in ( ",
					types[0].equals( "TABLE" ) ? getResultSetTableTypesPhysicalTableConstant() : types[0],
					sb,
					parameterValues
			);
			for ( int i = 1 ; i < types.length ; i++ ) {
				appendClauseAndParameterIfNotNullOrEmpty(
						", ",
						types[i].equals( "TABLE" ) ? getResultSetTableTypesPhysicalTableConstant() : types[i],
						sb,
						parameterValues
				);
			}
			sb.append( " ) " );
		}
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
				.append( "select table_name as " ).append( getResultSetTableNameLabel() )
				.append( ", column_name as " ).append( getResultSetColumnNameLabel() )
				// SQL Server supports a SSVARIANT datatype which needs to get mapped to VARCHAR
				.append( ", " ).append( " case when " )
				.append( getInformationSchemaColumnsDataTypeColumn() )
				.append( " = 'SSVARIANT' then 'VARCHAR' else " )
				.append( getInformationSchemaColumnsDataTypeColumn() )
				.append( " end as ")
				.append( getResultSetTypeNameLabel() )
				.append( ", null as " ).append( getResultSetColumnSizeLabel() )
				// Column size is fairly complicated to get out of information_schema
				// and likely to be DB-dependent. Currently, Hibernate ORM does not use
				// column size for anything, so for now, just return null.
				.append( ", null as " ) .append( getResultSetDecimalDigitsLabel() )
				// Decimal digits is fairly complicated to get out of information_schema
				// and likely to be DB-dependent. Currently, Hibernate ORM does not use
				// decimal digits for anything, so for now, just return null.
				.append( ", is_nullable as " ).append( getResultSetIsNullableLabel() )
				.append( ", null as " ).append( getResultSetSqlTypeCodeLabel() )
				// There is a SQL type code available from sys.types in SQL Server,
				// Currently, Hibernate ORM only uses
				// the SQL type code for SchemaMigrator to check if a column
				// type in the DB is consistent with what is computed in
				// Hibernate's metadata for the column. ORM also considers
				// the same column type name as a match, so the SQL code is
				// optional. For now, just return null for the SQL type code.
				.append( " from information_schema.columns where 1 = 1" );

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
		appendClauseAndParameterIfNotNullOrEmpty( " and " + schemaColumn + " like " , schemaPattern, sb, parameterValues );
		appendClauseAndParameterIfNotNullOrEmpty( " and table_name like " , tableNamePattern, sb, parameterValues );

		sb.append(  " order by table_catalog, table_schema, table_name, column_name, ordinal_position" );

		return getExtractionContext().getQueryResults( sb.toString(), parameterValues.toArray(), processor );
	}

	@Override
	protected <T> T processPrimaryKeysResultSet(
			String catalogFilter,
			String schemaFilter,
			Identifier tableName,
			ExtractionContext.ResultSetProcessor<T> processor) throws SQLException {
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
		// Generate the inner query first.

		StringBuilder innerQuery = new StringBuilder()
				.append("select s.name as schema_name, t.name as table_name, i.name as index_name, i.type as index_type, c.name as column_name from sys.tables t\n" +
						"        inner join sys.schemas s on t.schema_id = s.schema_id\n" +
						"        inner join sys.indexes i on i.object_id = t.object_id\n" +
						"        inner join sys.index_columns ic on ic.object_id = t.object_id \n" +
						"                                          AND i.index_id = ic.index_id\n" +
						"        inner join sys.columns c on c.object_id = t.object_id \n" +
						"                                          and  ic.column_id = c.column_id \n" +
						"        where i.index_id > 0    \n" +
						"            and i.type in (1, 2) -- clustered & nonclustered only\n" +
						"            and i.is_primary_key = 0 -- do not include PK indexes\n" +
						"            and i.is_unique_constraint = 0 -- do not include UQ\n");

		final List<Object> parameterValues = new ArrayList<>();

		appendClauseAndParameterIfNotNullOrEmpty( " and t.name = ", table, innerQuery, parameterValues );
		appendClauseAndParameterIfNotNullOrEmpty( " and s.name = ", schema, innerQuery, parameterValues );

		if ( unique ) {
			innerQuery.append( " AND i.is_unique_constraint = true" );
		}

		T result = getExtractionContext().getQueryResults(
				"select tmp.index_name as " + getResultSetIndexNameLabel() +
						", tmp.index_type as " + getResultSetIndexTypeLabel() +
						", column_name as " + getResultSetColumnNameLabel() +
						" from ( " + innerQuery + " ) tmp",
				parameterValues.toArray(),
				processor
		);
		return result;
	}

	@Override
	protected <T> T processImportedKeysResultSet(
			String catalog,
			String schema,
			String table,
			ExtractionContext.ResultSetProcessor<T> processor) throws SQLException {

		// The ResultSet must be ordered by the primary key catalog/schema/table and column position within the key.
		final StringBuilder sb = new StringBuilder()
				.append( "select constraint_name as " ).append( getResultSetForeignKeyLabel() )
				.append( ", constraint_catalog as " ).append( getResultSetPrimaryKeyCatalogLabel() )
				.append( ", constraint_schema as " ).append( getResultSetPrimaryKeySchemaLabel() )
				.append( ", table_name as " ).append( getResultSetPrimaryKeyTableLabel() )
				.append( ", column_name as ").append( getResultSetForeignKeyColumnNameLabel() )
				.append( ", column_name as ").append( getResultSetPrimaryKeyColumnNameLabel() )
				.append( " from information_schema.key_column_usage" )
				.append( " where table_name is not null" );

		// Now add constraints for the requested catalog/schema/table

		final List<Object> parameters = new ArrayList<>();
		final List<String> orderByList = new ArrayList<>();

		if ( appendClauseAndParameterIfNotNullOrEmpty( " and table_schema = ", schema, sb, parameters ) ) {
			orderByList.add( "table_schema" );
		}
		if ( appendClauseAndParameterIfNotNullOrEmpty( " and table_name = ", table, sb, parameters ) ) {
			orderByList.add( "table_name" );
		}
		orderByList.add( "ordinal_position" );

		if ( orderByList.size() > 0 ) {
			sb.append( " order by " ).append( orderByList.get( 0 ) );
			for ( int i = 1 ; i < orderByList.size() ; i++ ) {
				sb.append( ", " ).append( orderByList.get( i ) );
			}
		}

		return getExtractionContext().getQueryResults( sb.toString(), parameters.toArray(), processor );
	}


}
