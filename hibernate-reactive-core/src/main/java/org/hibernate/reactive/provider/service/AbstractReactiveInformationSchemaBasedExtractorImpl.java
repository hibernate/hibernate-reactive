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
import org.hibernate.tool.schema.extract.internal.AbstractInformationExtractorImpl;
import org.hibernate.tool.schema.extract.spi.ExtractionContext;

/**
 * @Author Gail Badner
 */
public abstract class AbstractReactiveInformationSchemaBasedExtractorImpl extends AbstractInformationExtractorImpl  {

	public AbstractReactiveInformationSchemaBasedExtractorImpl(ExtractionContext extractionContext) {
		super( extractionContext );
	}

	@Override
	protected String getResultSetCatalogLabel() {
		return normalizeLabel( super.getResultSetCatalogLabel() );
	}
	@Override
	protected String getResultSetSchemaLabel() {
		return normalizeLabel( super.getResultSetSchemaLabel() );
	}
	@Override
	protected String getResultSetTableNameLabel() {
		return normalizeLabel( super.getResultSetTableNameLabel() );
	}
	@Override
	protected String getResultSetTableTypeLabel() {
		return normalizeLabel( super.getResultSetTableTypeLabel() );
	}
	@Override
	protected String getResultSetRemarksLabel() {
		return normalizeLabel( super.getResultSetRemarksLabel() );
	}
	@Override
	protected String getResultSetColumnNameLabel() {
		return normalizeLabel( super.getResultSetColumnNameLabel() );
	}

	@Override
	protected String getResultSetSqlTypeCodeLabel() {
		return normalizeLabel( super.getResultSetSqlTypeCodeLabel() );
	}

	@Override
	protected String getResultSetTypeNameLabel() {
		return normalizeLabel( super.getResultSetTypeNameLabel() );
	}

	@Override
	protected String getResultSetColumnSizeLabel() {
		return normalizeLabel( super.getResultSetColumnSizeLabel() );
	}

	@Override
	protected String getResultSetDecimalDigitsLabel() {
		return normalizeLabel( super.getResultSetDecimalDigitsLabel() );
	}

	@Override
	protected String getResultSetIsNullableLabel() {
		return normalizeLabel( super.getResultSetIsNullableLabel() );
	}

	@Override
	protected String getResultSetIndexTypeLabel() {
		return normalizeLabel( super.getResultSetIndexTypeLabel() );
	}

	@Override
	protected String getResultSetIndexNameLabel() {
		return normalizeLabel( super.getResultSetIndexNameLabel() );
	}

	@Override
	protected String getResultSetForeignKeyLabel() {
		return normalizeLabel( super.getResultSetForeignKeyLabel() );
	}

	@Override
	protected String getResultSetPrimaryKeyNameLabel() {
		return normalizeLabel( super.getResultSetPrimaryKeyNameLabel() );
	}

	@Override
	protected String getResultSetColumnPositionColumn() {
		return normalizeLabel( super.getResultSetColumnPositionColumn() );
	}

	@Override
	protected String getResultSetPrimaryKeyColumnNameLabel() {
		return normalizeLabel( super.getResultSetPrimaryKeyColumnNameLabel() );
	}

	@Override
	protected String getResultSetForeignKeyColumnNameLabel() {
		return normalizeLabel( super.getResultSetForeignKeyColumnNameLabel() );
	}

	@Override
	protected String getResultSetPrimaryKeyCatalogLabel() {
		return normalizeLabel( super.getResultSetPrimaryKeyCatalogLabel() );
	}

	@Override
	protected String getResultSetPrimaryKeySchemaLabel() {
		return normalizeLabel( super.getResultSetPrimaryKeySchemaLabel() );
	}

	@Override
	protected String getResultSetPrimaryKeyTableLabel() {
		return normalizeLabel( super.getResultSetPrimaryKeyTableLabel() );
	}

	private String normalizeLabel(String columnLabel) {
		return toMetaDataObjectName( Identifier.toIdentifier( columnLabel ) );
	}

	@Override
	protected <T> T processCatalogsResultSet(ExtractionContext.ResultSetProcessor<T> processor) throws SQLException {
		return getExtractionContext().getQueryResults(
				String.format(
						"SELECT catalog_name AS %s FROM information_schema.information_schema_catalog_name",
						getResultSetCatalogLabel()
				),
				null,
				processor
		);
	}

	@Override
	protected <T> T processSchemaResultSet(
			String catalogFilter,
			String schemaFilter,
			ExtractionContext.ResultSetProcessor<T> processor) throws SQLException {
		final StringBuilder sb = new StringBuilder()
				.append( "SELECT catalog_name AS " ).append( getResultSetCatalogLabel() )
				.append( " , schema_name AS "  ).append( getResultSetSchemaLabel() )
				.append( " FROM information_schema.schemata WHERE true" );
		final List<Object> parameters = new ArrayList<>();
		appendClauseAndParameterIfNotNull( " AND catalog_name = ", catalogFilter, sb, parameters );
		appendClauseAndParameterIfNotNull( " AND schema_name = ", schemaFilter, sb, parameters );
		return getExtractionContext().getQueryResults( sb.toString(), parameters.toArray(), processor );
	}

	protected void appendClauseAndParameterIfNotNull(
			String clause,
			Object parameter,
			StringBuilder sb,
			List<Object> parameters) {

		if ( parameter != null ) {
			parameters.add( parameter );
			sb.append( clause );
			// TODO: PostgreSQL does not seem to accept "?" parameter placeholders.
			//       It does accept $<position>. Will this work for all dialects?
			sb.append( "$").append( parameters.size() );
		}
	}

	@Override
	protected <T> T processTableResultSet(
			String catalogFilter,
			String schemaFilter,
			String tableNameFilter,
			String[] tableTypes,
			ExtractionContext.ResultSetProcessor<T> processor
	) throws SQLException {

		final StringBuilder sb = new StringBuilder()
				.append( "SELECT table_catalog AS " ).append( getResultSetCatalogLabel() )
				.append( " , table_schema AS "  ).append( getResultSetSchemaLabel() )
				.append( " , table_name AS " ).append( getResultSetTableNameLabel() )
				.append( " , table_type AS " ).append( getResultSetTableTypeLabel() )
				.append( " , null AS " ).append( getResultSetRemarksLabel() )
				// Remarks are not available from information_schema.
				// Hibernate ORM does not currently do anything with remarks,
				// so just return null for now.
				.append( " FROM information_schema.tables WHERE true" );
		List<Object> parameterValues = new ArrayList<>();
		appendClauseAndParameterIfNotNull( " AND table_catalog = ", catalogFilter, sb, parameterValues );
		appendClauseAndParameterIfNotNull( " AND table_schema = ", schemaFilter, sb, parameterValues );
		appendClauseAndParameterIfNotNull( " AND table_name = ", tableNameFilter, sb, parameterValues );

		if ( tableTypes != null && tableTypes.length > 0 ) {
			appendClauseAndParameterIfNotNull(
					" AND table_type IN ( ",
					tableTypes[0].equals( "TABLE" ) ? getResultSetTableTypesPhysicalTableConstant() : tableTypes[0],
					sb,
					parameterValues
			);
			for ( int i = 1 ; i < tableTypes.length ; i++ ) {
				appendClauseAndParameterIfNotNull(
						", ",
						tableTypes[i].equals( "TABLE" ) ? getResultSetTableTypesPhysicalTableConstant() : tableTypes[i],
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
			String catalogFilter,
			String schemaFilter,
			String tableName,
			ExtractionContext.ResultSetProcessor<T> processor) throws SQLException {

		final StringBuilder sb = new StringBuilder()
				.append( "SELECT table_name AS " ).append( getResultSetTableNameLabel() )
				.append( ", column_name AS " ).append( getResultSetColumnNameLabel() )
				.append( ", udt_name AS " ).append( getResultSetTypeNameLabel() )
				.append( ", null AS " ).append( getResultSetColumnSizeLabel() )
				// Column size is fairly complicated to get out of information_schema
				// and likely to be DB-dependent. Currently, Hibernate ORM does not use
				// column size for anything, so for now, just return null.
				.append( ", null AS " ) .append( getResultSetDecimalDigitsLabel() )
				// Decimal digits is fairly complicated to get out of information_schema
				// and likely to be DB-dependent. Currently, Hibernate ORM does not use
				// decimal digits for anything, so for now, just return null.
				.append( ", is_nullable AS " ).append( getResultSetIsNullableLabel() )
				.append( ", null AS " ).append( getResultSetSqlTypeCodeLabel() )
				// SQL type code is not available from information_schema,
				// and, for PostgreSQL at least, it appears to be hard-coded
				// into the JDBC driver. Currently, Hibernate ORM only uses
				// the SQL type code for SchemaMigrator to check if a column
				// type in the DB is consistent with what is computed in
				// Hibernate's metadata for the column. ORM also considers
				// the same column type name as a match, so the SQL code is
				// optional. For now, just return null for the SQL type code.
				.append( " FROM information_schema.columns WHERE true" );

		final List<Object> parameterValues = new ArrayList<>();
		appendClauseAndParameterIfNotNull( " AND table_catalog = " , catalogFilter, sb, parameterValues );
		appendClauseAndParameterIfNotNull( " AND table_schema = " , schemaFilter, sb, parameterValues );
		appendClauseAndParameterIfNotNull( " AND table_name = " , tableName, sb, parameterValues );

		sb.append(  "ORDER BY table_name, column_name, ordinal_position" );

		return getExtractionContext().getQueryResults( sb.toString(), parameterValues.toArray(), processor );
	}
}
