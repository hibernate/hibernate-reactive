/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.cfg.NotYetImplementedException;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.tool.schema.extract.spi.ExtractionContext;

/**
 * @Author Gail Badner
 */
public class PostgreSqlReactiveInformationExtractorImpl extends AbstractReactiveInformationSchemaBasedExtractorImpl {

	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( PostgreSqlReactiveInformationExtractorImpl.class );

	public PostgreSqlReactiveInformationExtractorImpl(ExtractionContext extractionContext) {
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
			String catalogFilter,
			String schemaFilter,
			Identifier tableName,
			boolean approximate,
			ExtractionContext.ResultSetProcessor<T> processor) throws SQLException {

		// Generate the inner query first.
		final StringBuilder innerQuery = new StringBuilder()
				.append( "SELECT ci.relname AS index_name" )
				.append( " , CASE i.indisclustered WHEN true THEN " ).append( DatabaseMetaData.tableIndexClustered )
				.append( " ELSE CASE am.amname WHEN 'hash' THEN " ).append( DatabaseMetaData.tableIndexHashed )
				.append( " ELSE " ).append( DatabaseMetaData.tableIndexOther ).append( " END" )
				.append( " END AS index_type" )
				.append( " , (information_schema._pg_expandarray(i.indkey)).n AS position" )
				.append( " , ci.oid AS ci_iod" )
				.append( " FROM pg_catalog.pg_class ct" )
				.append( " JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)" )
 				.append( " JOIN pg_catalog.pg_index i ON (ct.oid = i.indrelid)" )
				.append( " JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)" )
				.append( " JOIN pg_catalog.pg_am am ON (ci.relam = am.oid)" )
				.append( " WHERE true" );

		final List<Object> parameterValues = new ArrayList<>();

		appendClauseAndParameterIfNotNull( " AND n.nspname = ", schemaFilter, innerQuery, parameterValues );
		appendClauseAndParameterIfNotNull( " AND ct.relname = ", tableName.getText(), innerQuery, parameterValues );

		return getExtractionContext().getQueryResults(
				"SELECT tmp.index_name AS " + getResultSetIndexNameLabel() +
						", tmp.index_type AS " + getResultSetIndexTypeLabel() +
						", trim(both '\"' from pg_catalog.pg_get_indexdef(tmp.ci_iod, tmp.position, false)) AS " + getResultSetColumnNameLabel() +
						" FROM ( " + innerQuery + " ) tmp" +
						" ORDER BY " + getResultSetIndexNameLabel() + ", tmp.position",
				parameterValues.toArray(),
				processor
		);
	}

	@Override
	protected <T> T processImportedKeysResultSet(
			String catalogFilter,
			String schemaFilter,
			String tableName,
			ExtractionContext.ResultSetProcessor<T> processor
	) throws SQLException {
		final StringBuilder sb = new StringBuilder()
				.append( "SELECT NULL AS " ).append( getResultSetPrimaryKeyCatalogLabel() )
				.append( ", pkn.nspname AS " ).append( getResultSetPrimaryKeySchemaLabel() )
				.append( ", pkc.relname AS " ).append( getResultSetPrimaryKeyTableLabel() )
				.append( ", pka.attname AS " ).append( getResultSetPrimaryKeyColumnNameLabel() )
				.append( ", fka.attname AS " ).append( getResultSetForeignKeyColumnNameLabel() )
				.append( ", pos.n AS " ).append( getResultSetColumnPositionColumn() )
				.append( ", con.conname AS " ).append( getResultSetForeignKeyLabel() )
				.append( " FROM pg_catalog.pg_namespace pkn, pg_catalog.pg_class pkc, pg_catalog.pg_attribute pka" )
				.append( ",  pg_catalog.pg_namespace fkn, pg_catalog.pg_class fkc, pg_catalog.pg_attribute fka" )
				.append( ", pg_catalog.pg_constraint con" )
				.append( ", pg_catalog.generate_series(1, CAST( (SELECT setting FROM pg_catalog.pg_settings WHERE name='max_index_keys') AS INTEGER ) ) pos(n)" )
				.append( " WHERE pkn.oid = pkc.relnamespace AND pkc.oid = pka.attrelid AND pka.attnum = con.confkey[pos.n] AND con.confrelid = pkc.oid" )
				.append( " AND fkn.oid = fkc.relnamespace AND fkc.oid = fka.attrelid AND fka.attnum = con.conkey[pos.n] AND con.conrelid = fkc.oid" )
				.append( " AND con.contype = 'f' " );

		final List<Object> parameterValues = new ArrayList<>();

		appendClauseAndParameterIfNotNull( " AND fkn.nspname = ", schemaFilter, sb, parameterValues );
		appendClauseAndParameterIfNotNull( " AND fkc.relname = ", tableName, sb, parameterValues );

		sb.append( " ORDER BY pkn.nspname,pkc.relname, con.conname, pos.n");

		return getExtractionContext().getQueryResults( sb.toString(), parameterValues.toArray(), processor );
	}
}
