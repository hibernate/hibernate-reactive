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
import org.hibernate.tool.schema.extract.spi.InformationExtractor;

/**
 * An implementation of {@link InformationExtractor} that obtains metadata
 * information from a database's information_schema.
 *
 * @author Gail Badner
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
						"select catalog_name as %s from information_schema.information_schema_catalog_name",
						getResultSetCatalogLabel()
				),
				null,
				processor
		);
	}

	@Override
	protected <T> T processSchemaResultSet(
			String catalog,
			String schemaPattern,
			ExtractionContext.ResultSetProcessor<T> processor) throws SQLException {
		final StringBuilder sb = new StringBuilder()
				.append( "select catalog_name as " ).append( getResultSetCatalogLabel() )
				.append( " , schema_name as "  ).append( getResultSetSchemaLabel() )
				.append( " from information_schema.schemata where true" );
		final List<Object> parameters = new ArrayList<>();
		appendClauseAndParameterIfNotNull( " and catalog_name = ", catalog, sb, parameters );
		appendClauseAndParameterIfNotNull( " and schema_name like ", schemaPattern, sb, parameters );
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
			String catalog,
			String schemaPattern,
			String tableNamePattern,
			String[] types,
			ExtractionContext.ResultSetProcessor<T> processor
	) throws SQLException {

		final StringBuilder sb = new StringBuilder()
				.append( "select table_catalog as " ).append( getResultSetCatalogLabel() )
				.append( " , table_schema as "  ).append( getResultSetSchemaLabel() )
				.append( " , table_name as " ).append( getResultSetTableNameLabel() )
				.append( " , table_type as " ).append( getResultSetTableTypeLabel() )
				.append( " , null as " ).append( getResultSetRemarksLabel() )
				// Remarks are not available from information_schema.
				// Hibernate ORM does not currently do anything with remarks,
				// so just return null for now.
				.append( " from information_schema.tables where true" );
		List<Object> parameterValues = new ArrayList<>();
		appendClauseAndParameterIfNotNull( " and table_catalog = ", catalog, sb, parameterValues );
		appendClauseAndParameterIfNotNull( " and table_schema like ", schemaPattern, sb, parameterValues );
		appendClauseAndParameterIfNotNull( " and table_name like ", tableNamePattern, sb, parameterValues );

		if ( types != null && types.length > 0 ) {
			appendClauseAndParameterIfNotNull(
					" and table_type in ( ",
					types[0].equals( "TABLE" ) ? getResultSetTableTypesPhysicalTableConstant() : types[0],
					sb,
					parameterValues
			);
			for ( int i = 1 ; i < types.length ; i++ ) {
				appendClauseAndParameterIfNotNull(
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
				.append( ", udt_name as " ).append( getResultSetTypeNameLabel() )
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
				// SQL type code is not available from information_schema,
				// and, for PostgreSQL at least, it appears to be hard-coded
				// into the JDBC driver. Currently, Hibernate ORM only uses
				// the SQL type code for SchemaMigrator to check if a column
				// type in the DB is consistent with what is computed in
				// Hibernate's metadata for the column. ORM also considers
				// the same column type name as a match, so the SQL code is
				// optional. For now, just return null for the SQL type code.
				.append( " from information_schema.columns where true" );

		final List<Object> parameterValues = new ArrayList<>();
		appendClauseAndParameterIfNotNull( " and table_catalog = " , catalog, sb, parameterValues );
		appendClauseAndParameterIfNotNull( " and table_schema like " , schemaPattern, sb, parameterValues );
		appendClauseAndParameterIfNotNull( " and table_name like " , tableNamePattern, sb, parameterValues );

		sb.append(  " order by table_catalog, table_schema, table_name, column_name, ordinal_position" );

		return getExtractionContext().getQueryResults( sb.toString(), parameterValues.toArray(), processor );
	}
}
