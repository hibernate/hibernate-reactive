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

	// #processSchemaResultSet in the superclass is OK as is
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

		// Documentation for information_schema.tables says the following for
		// table_schema:
		// "** Important ** only reliable way to find the schema of an object
		// is to query the sys.objects catalog view. INFORMATION_SCHEMA views
		// could be incomplete since they are not updated for all new features."
		// (See https://docs.microsoft.com/en-us/sql/relational-databases/system-information-schema-views/tables-transact-sql?view=sql-server-ver15&viewFallbackFrom=sql-server-ver19)

		// This is the reason for joining information_schema.tables with sys.objects.

		final StringBuilder sb = new StringBuilder()
				.append( "select t.table_catalog as " ).append( getResultSetCatalogLabel() )
				.append( " , OBJECT_SCHEMA_NAME( o.object_id ) as " ).append( getResultSetSchemaLabel() )
				.append( " , t.table_name as " ).append( getResultSetTableNameLabel() )
				.append( " , t.table_type as " ).append( getResultSetTableTypeLabel() )
				.append( " , null as " ).append( getResultSetRemarksLabel() )
				// Remarks are not available from information_schema.
				// Hibernate ORM does not currently do anything with remarks,
				// so just return null for now.
				.append( " from information_schema.tables t inner join sys.objects o" )
				.append( " on t.table_name = o.name " )
				.append( " and ( ( t.table_type = 'BASE TABLE' and o.type = 'U' ) or ( t.table_type = 'VIEW' and o.type = 'V' ) )" )
				// o.type = 'U' is for a user-defined table
				// o.type = 'V' is for a view
				.append( " where 1 = 1" );

		List<Object> parameterValues = new ArrayList<>();

		appendClauseAndParameterIfNotNullOrEmpty( " and t.table_catalog = ", catalog, sb, parameterValues );
		appendClauseAndParameterIfNotNullOrEmpty( " and OBJECT_SCHEMA_NAME( o.object_id ) like ", schemaPattern, sb, parameterValues );
		appendClauseAndParameterIfNotNullOrEmpty( " and t.table_name like ", tableNamePattern, sb, parameterValues );

		if ( types != null && types.length > 0 ) {
			appendClauseAndParameterIfNotNullOrEmpty(
					" and t.table_type in ( ",
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

		// Documentation for information_schema.columns says the following for
		// table_schema:
		// ** Important ** Do not use INFORMATION_SCHEMA views to determine the
		// schema of an object. INFORMATION_SCHEMA views only represent a subset
		// of the metadata of an object. The only reliable way to find the schema
		// of a object is to query the sys.objects catalog view.
		// (See https://docs.microsoft.com/en-us/sql/relational-databases/system-information-schema-views/columns-transact-sql?view=sql-server-ver15&viewFallbackFrom=sql-server-ver19)

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
				.append( ", c.is_nullable as " ).append( getResultSetIsNullableLabel() )
				.append( ", null as " ).append( getResultSetSqlTypeCodeLabel() )
				// There is a SQL type code available from sys.types in SQL Server,
				// Currently, Hibernate ORM only uses
				// the SQL type code for SchemaMigrator to check if a column
				// type in the DB is consistent with what is computed in
				// Hibernate's metadata for the column. ORM also considers
				// the same column type name as a match, so the SQL code is
				// optional. For now, just return null for the SQL type code.
				.append( " from information_schema.columns c inner join sys.objects o" )
				.append( " on c.table_name = o.name and o.type in ( 'U', 'V' ) " )
				// o.type = 'U' is for a user-defined table
				// o.type = 'V' is for a view
				.append( " where 1 = 1" );

		final List<Object> parameterValues = new ArrayList<>();
		appendClauseAndParameterIfNotNullOrEmpty( " and c.table_catalog = " , catalog, sb, parameterValues );
		appendClauseAndParameterIfNotNullOrEmpty( " and OBJECT_SCHEMA_NAME( o.object_id ) like " , schemaPattern, sb, parameterValues );
		appendClauseAndParameterIfNotNullOrEmpty( " and c.table_name like " , tableNamePattern, sb, parameterValues );

		sb.append(  " order by c.table_catalog, OBJECT_SCHEMA_NAME( o.object_id ), c.table_name, c.column_name, c.ordinal_position" );

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

		StringBuilder sb = new StringBuilder()
				.append( "select i.name as " ).append( getResultSetIndexNameLabel() )
				.append( " , i.type as " ).append( getResultSetIndexTypeLabel() )
				.append( " , COL_NAME(ic.object_id, ic.column_id) as " ).append( getResultSetColumnNameLabel() )
				.append( " from sys.indexes i inner join sys.index_columns ic" )
				.append( " on ic.object_id = i.object_id and ic.index_id = i.index_id" )
				.append( " where i.index_id > 0" )
				.append( " and i.type in (1, 2)" )
				.append( " and i.is_primary_key = 0" );
				// do not include PK indexes

		final List<Object> parameterValues = new ArrayList<>();

		appendClauseAndParameterIfNotNullOrEmpty( " and DB_NAME() = ", catalog, sb, parameterValues );
		appendClauseAndParameterIfNotNullOrEmpty( " and OBJECT_NAME( i.object_id ) = ", table, sb, parameterValues );
		appendClauseAndParameterIfNotNullOrEmpty( " and OBJECT_SCHEMA_NAME( i.object_id) = ", schema, sb, parameterValues );

		if ( unique ) {
			sb.append( " and i.is_unique_constraint = true" );
		}

		sb.append( " order by OBJECT_SCHEMA_NAME( i.object_id), OBJECT_NAME( i.object_id ), ic.key_ordinal" );

		return getExtractionContext()
				.getQueryResults( sb.toString(), parameterValues.toArray(), processor );
	}

	@Override
	protected <T> T processImportedKeysResultSet(
			String catalog,
			String schema,
			String table,
			ExtractionContext.ResultSetProcessor<T> processor) throws SQLException {

		// Documentation for information_schema.key_column_usage says the following for
		// table_schema and constraint_schema:

		// ** Important ** Do not use INFORMATION_SCHEMA views to determine the schema
		// of an object. INFORMATION_SCHEMA views only represent a subset of the
		// metadata of an object. The only reliable way to find the schema of a object
		// is to query the sys.objects catalog view.
		// (See https://docs.microsoft.com/en-us/sql/relational-databases/system-information-schema-views/key-column-usage-transact-sql?view=sql-server-ver15&viewFallbackFrom=sql-server-ver19)

		// The ResultSet must be ordered by the primary key catalog/schema/table and column position within the key.

		final StringBuilder sb = new StringBuilder()
				.append( "select OBJECT_NAME( constraint_object_id ) as " ).append( getResultSetForeignKeyLabel() )
				.append( ", DB_NAME() as " ).append( getResultSetPrimaryKeyCatalogLabel() )
				.append( ", OBJECT_SCHEMA_NAME( referenced_object_id ) as " ).append( getResultSetPrimaryKeySchemaLabel() )
				.append( ", OBJECT_NAME( referenced_object_id ) as " ).append( getResultSetPrimaryKeyTableLabel() )
				.append( ", COL_NAME( parent_object_id, parent_column_id ) as ").append( getResultSetForeignKeyColumnNameLabel() )
				.append( ", COL_NAME( referenced_object_id, referenced_column_id) as ").append( getResultSetPrimaryKeyColumnNameLabel() )
				.append( " from sys.foreign_key_columns" )
				.append( " where 1 = 1" );

		// Now add constraints for the requested catalog/schema/table

		final List<Object> parameters = new ArrayList<>();
		final List<String> orderByList = new ArrayList<>();

		if ( appendClauseAndParameterIfNotNullOrEmpty( " and DB_NAME() = ", catalog, sb, parameters ) ) {
			orderByList.add( "DB_NAME()" );
		}
		if ( appendClauseAndParameterIfNotNullOrEmpty( " and OBJECT_SCHEMA_NAME( parent_object_id ) = ", schema, sb, parameters ) ) {
			orderByList.add( "OBJECT_SCHEMA_NAME( parent_object_id )" );
		}
		if ( appendClauseAndParameterIfNotNullOrEmpty( " and OBJECT_NAME( parent_object_id ) = ", table, sb, parameters ) ) {
			orderByList.add( "OBJECT_NAME( parent_object_id )" );
		}
		orderByList.add( "constraint_column_id" );

		sb.append( " order by " ).append( orderByList.get( 0 ) );
		for ( int i = 1 ; i < orderByList.size() ; i++ ) {
			sb.append( ", " ).append( orderByList.get( i ) );
		}

		return getExtractionContext().getQueryResults( sb.toString(), parameters.toArray(), processor );
	}

	@Override
	protected int dataTypeCode(String typeName) {
		// SQL Server only supports "float" sql type for double precision
		// so return code for double for both double and float column types
		if ( typeName.equalsIgnoreCase( "float" ) ||
				typeName.toLowerCase().startsWith( "double" ) ) {
			return Types.DOUBLE;
		}
		return super.dataTypeCode( typeName );
	}

}
