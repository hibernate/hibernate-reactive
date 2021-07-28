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
				.append( "select ci.relname as index_name" )
				.append( " , case i.indisclustered when true then " ).append( DatabaseMetaData.tableIndexClustered )
				.append( " else case am.amname when 'hash' then " ).append( DatabaseMetaData.tableIndexHashed )
				.append( " else " ).append( DatabaseMetaData.tableIndexOther ).append( " end" )
				.append( " end as index_type" )
				.append( " , (information_schema._pg_expandarray(i.indkey)).n as position" )
				.append( " , ci.oid as ci_iod" )
				.append( " from pg_catalog.pg_class ct" )
				.append( " join pg_catalog.pg_namespace n on (ct.relnamespace = n.oid)" )
 				.append( " join pg_catalog.pg_index i on (ct.oid = i.indrelid)" )
				.append( " join pg_catalog.pg_class ci on (ci.oid = i.indexrelid)" )
				.append( " join pg_catalog.pg_am am on (ci.relam = am.oid)" )
				.append( " where true" );

		final List<Object> parameterValues = new ArrayList<>();

		appendClauseAndParameterIfNotNull( " and n.nspname = ", schemaFilter, innerQuery, parameterValues );
		appendClauseAndParameterIfNotNull( " and ct.relname = ", tableName.getText(), innerQuery, parameterValues );

		return getExtractionContext().getQueryResults(
				"select tmp.index_name as " + getResultSetIndexNameLabel() +
						", tmp.index_type as " + getResultSetIndexTypeLabel() +
						", trim(both '\"' from pg_catalog.pg_get_indexdef(tmp.ci_iod, tmp.position, false)) as " + getResultSetColumnNameLabel() +
						" from ( " + innerQuery + " ) tmp" +
						" order by " + getResultSetIndexNameLabel() + ", tmp.position",
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
				.append( "select null as " ).append( getResultSetPrimaryKeyCatalogLabel() )
				.append( ", pkn.nspname as " ).append( getResultSetPrimaryKeySchemaLabel() )
				.append( ", pkc.relname as " ).append( getResultSetPrimaryKeyTableLabel() )
				.append( ", pka.attname as " ).append( getResultSetPrimaryKeyColumnNameLabel() )
				.append( ", fka.attname as " ).append( getResultSetForeignKeyColumnNameLabel() )
				.append( ", pos.n as " ).append( getResultSetColumnPositionColumn() )
				.append( ", con.conname as " ).append( getResultSetForeignKeyLabel() )
				.append( " from pg_catalog.pg_namespace pkn, pg_catalog.pg_class pkc, pg_catalog.pg_attribute pka" )
				.append( ",  pg_catalog.pg_namespace fkn, pg_catalog.pg_class fkc, pg_catalog.pg_attribute fka" )
				.append( ", pg_catalog.pg_constraint con" )
				.append( ", pg_catalog.generate_series(1, cast( (select setting from pg_catalog.pg_settings where name='max_index_keys') as integer ) ) pos(n)" )
				.append( " where pkn.oid = pkc.relnamespace and pkc.oid = pka.attrelid and pka.attnum = con.confkey[pos.n] and con.confrelid = pkc.oid" )
				.append( " and fkn.oid = fkc.relnamespace and fkc.oid = fka.attrelid and fka.attnum = con.conkey[pos.n] and con.conrelid = fkc.oid" )
				.append( " and con.contype = 'f' " );

		final List<Object> parameterValues = new ArrayList<>();

		appendClauseAndParameterIfNotNull( " and fkn.nspname = ", schemaFilter, sb, parameterValues );
		appendClauseAndParameterIfNotNull( " and fkc.relname = ", tableName, sb, parameterValues );

		sb.append( " order by pkn.nspname,pkc.relname, con.conname, pos.n");

		return getExtractionContext().getQueryResults( sb.toString(), parameterValues.toArray(), processor );
	}
}
