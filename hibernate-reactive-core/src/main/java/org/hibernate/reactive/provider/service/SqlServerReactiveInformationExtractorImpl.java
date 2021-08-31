/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.sql.SQLException;

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

	@Override
	protected <T> T processPrimaryKeysResultSet(
			String catalogFilter,
			String schemaFilter,
			Identifier tableName,
			ExtractionContext.ResultSetProcessor<T> processor) throws SQLException {
		throw new NotYetImplementedException(); }

	@Override
	protected <T> T processIndexInfoResultSet(
			String catalog,
			String schema,
			String table,
			boolean unique,
			boolean approximate,
			ExtractionContext.ResultSetProcessor<T> processor) throws SQLException {
		throw new NotYetImplementedException();
	}

	@Override
	protected <T> T processImportedKeysResultSet(
			String catalog, String schema, String table, ExtractionContext.ResultSetProcessor<T> processor)
			throws SQLException {
		throw new NotYetImplementedException();
	}
}
