/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.hibernate.boot.model.TruthValue;
import org.hibernate.boot.model.naming.DatabaseIdentifier;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.tool.schema.extract.internal.AbstractInformationExtractorImpl;
import org.hibernate.tool.schema.extract.internal.ColumnInformationImpl;
import org.hibernate.tool.schema.extract.spi.ColumnInformation;
import org.hibernate.tool.schema.extract.spi.ExtractionContext;
import org.hibernate.tool.schema.extract.spi.InformationExtractor;
import org.hibernate.tool.schema.extract.spi.TableInformation;

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
	protected String getResultSetTableTypesPhysicalTableConstant() {
		return "BASE TABLE";
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
				.append( " from information_schema.schemata where 1 = 1" );
		final List<Object> parameters = new ArrayList<>();
		appendClauseAndParameterIfNotNullOrEmpty( " and catalog_name = ", catalog, sb, parameters );
		appendClauseAndParameterIfNotNullOrEmpty( " and schema_name like ", schemaPattern, sb, parameters );
		return getExtractionContext().getQueryResults( sb.toString(), parameters.toArray(), processor );
	}

	protected boolean appendClauseAndParameterIfNotNullOrEmpty(
			String clause,
			Object parameter,
			StringBuilder sb,
			List<Object> parameters) {

		if ( parameter != null && ( ! String.class.isInstance( parameter ) || ! ( (String) parameter ).isEmpty() ) ) {
			parameters.add( parameter );
			sb.append( clause );
			sb.append( "?");
			return true;
		}
		return false;
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
				.append( ", " ).append( " case when " )
				.append( getInformationSchemaColumnsDataTypeColumn() )
				.append( " = 'bpchar' then 'CHAR' else " )
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
				// SQL type code is not available from information_schema,
				// and, for PostgreSQL at least, it appears to be hard-coded
				// into the JDBC driver. Currently, Hibernate ORM only uses
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

	/**
	 * Gets the name of column in information_schema.columns for the
	 * database-specific column type.
	 *
	 * @return the name of column in information_schema.columns for the
	 * database-specific column type
	 */
	protected String getInformationSchemaColumnsDataTypeColumn() {
		// The SQL-92 standard says the column name is DATA_TYPE.
		return "data_type";
	}

	/**
	 * Given a catalog column name and a schema column name in an
	 * information_schema table/view, this method returns the column
	 * used for storing the catalog name, or <code null/>, if there
	 * is no valid column containing the catalog name.
	 * <p/>
	 * MySQL, for example, uses the schema name column
	 * in the information_schema to store the catalog name.
	 * (@see MySqlReactiveInformationExtractorImpl)
	 *
	 * @param catalogColumnName - the catalog column name
	 * @param schemaColumnName  - the schema column name
	 * @return the column used for storing the catalog name,
	 * or <code null/> if there is no valid column containing
	 * the catalog name.
	 */
	protected String getDatabaseCatalogColumnName(String catalogColumnName, String schemaColumnName ) {
		return catalogColumnName;

	}

	/**
	 * Given a catalog column name and a schema column name in an
	 * information_schema table/view, this method returns the column
	 * used for storing the schema name, or <code null/>, if there
	 * is no valid column containing the schema name.
	 * <p/>
	 * MySQL, for example, does not have a valid column in
	 * in the information_schema to store the schema name.
	 * (@see MySqlReactiveInformationExtractorImpl)
	 *
	 * @param catalogColumnName - the catalog column name
	 * @param schemaColumnName  - the schema column name
	 * @return the column used for storing the schema name,
	 * or <code null/> if there is no valid column containing
	 * the schema name.
	 */
	protected String getDatabaseSchemaColumnName(String catalogColumnName, String schemaColumnName ) {
		return schemaColumnName;
	}

	@Override
	protected void addExtractedColumnInformation(
			TableInformation tableInformation, ResultSet resultSet) throws SQLException {
		final String typeName = new StringTokenizer( resultSet.getString( getResultSetTypeNameLabel() ), "() " ).nextToken();
		final ColumnInformation columnInformation = new ColumnInformationImpl(
				tableInformation,
				DatabaseIdentifier.toIdentifier( resultSet.getString( getResultSetColumnNameLabel() ) ),
				dataTypeCode( typeName ),
				typeName,
				resultSet.getInt( getResultSetColumnSizeLabel() ),
				resultSet.getInt( getResultSetDecimalDigitsLabel() ),
				interpretTruthValue( resultSet.getString( getResultSetIsNullableLabel() ) )
		);
		tableInformation.addColumn( columnInformation );
	}

	/**
	 * Return a JDBC Type code for the given type name
	 */
	protected int dataTypeCode(String typeName) {
		return 0;
	}

	private TruthValue interpretTruthValue(String nullable) {
		if ( "yes".equalsIgnoreCase( nullable ) ) {
			return TruthValue.TRUE;
		}
		else if ( "no".equalsIgnoreCase( nullable ) ) {
			return TruthValue.FALSE;
		}
		return TruthValue.UNKNOWN;
	}
}
