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
		return "TABLE";
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

		final StringBuilder sb = new StringBuilder()
				.append("select uic.index_name as " ).append( getResultSetIndexNameLabel() )
				.append( ", -1 as " ).append( getResultSetIndexTypeLabel() )
				.append( ", uic.column_name as " ).append( getResultSetColumnNameLabel() )
				.append( " from user_ind_columns uic join user_indexes ui on ui.table_name = uic.table_name" )
				.append( " where 1 = 1");

		final List<Object> parameters = new ArrayList<>();
		// Oracle's index TABLE_OWNER (schema) name and TABLE_NAME in USER_INDEXES
		// column names for an index are in USE_IND_COLUMNS
		assert catalog == null || catalog.isEmpty();
		appendClauseAndParameterIfNotNullOrEmpty( " and ui.table_owner = ", schema, sb, parameters );
		appendClauseAndParameterIfNotNullOrEmpty( " and ui.table_name = ", table, sb, parameters );


		return getExtractionContext().getQueryResults( sb.toString(), parameters.toArray(), processor );
	}

	@Override
	protected <T> T processImportedKeysResultSet(
			String catalog,
			String schema,
			String table,
			ExtractionContext.ResultSetProcessor<T> processor
	) throws SQLException {

		String fkNameSubquery = "(select constraint_name from user_constraints where r_owner = '" + schema + "' and table_name = '" + table + "')";
		String constraintSubquery =
				"(SELECT R_CONSTRAINT_NAME FROM USER_CONSTRAINTS WHERE OWNER = '" + schema + "' and table_name = '" + table + "' and CONSTRAINT_TYPE = 'R')";

		final StringBuilder sb = new StringBuilder()
				.append( "select " + fkNameSubquery + " as " ).append( getResultSetForeignKeyLabel() )
				.append( ", null as " ).append( getResultSetPrimaryKeyCatalogLabel() )
				.append( ", uc.owner as " ).append( getResultSetPrimaryKeySchemaLabel() )
				.append( ", ucc.table_name as " ).append( getResultSetPrimaryKeyTableLabel() )
				.append( ", ucc.column_name as ").append( getResultSetPrimaryKeyColumnNameLabel() )
				.append( ", ucc.column_name as " ).append( getResultSetForeignKeyColumnNameLabel() )
				.append( " from user_constraints uc join user_cons_columns ucc on uc.constraint_name = ucc.constraint_name ")
				// Exclude primary keys, which do not have a referenced table.
				.append( " where uc.constraint_name = " + constraintSubquery );


		// Now add constraints for the requested catalog/schema/table

		final List<Object> parameters = new ArrayList<>();

		final List<String> orderByList = new ArrayList<>();
		orderByList.add( "uc.owner" );
		orderByList.add( "ucc.table_name" );
		orderByList.add( "ucc.position" );

		if ( orderByList.size() > 0 ) {
			sb.append( " order by " ).append( orderByList.get( 0 ) );
			for ( int i = 1 ; i < orderByList.size() ; i++ ) {
				sb.append( ", " ).append( orderByList.get( i ) );
			}
		}

		T result =  getExtractionContext().getQueryResults( sb.toString(), parameters.toArray(), processor );

		return result;
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
				.append( " from sys.all_tables sat join sys.all_tab_comments satc on sat.TABLE_NAME = satc.TABLE_NAME where 1 = 1" );
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
