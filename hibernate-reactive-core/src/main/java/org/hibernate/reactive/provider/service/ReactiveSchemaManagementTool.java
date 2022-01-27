/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.cfg.NotYetImplementedException;
import org.hibernate.dialect.CockroachDB201Dialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MariaDB103Dialect;
import org.hibernate.dialect.MySQL8Dialect;
import org.hibernate.dialect.Oracle12cDialect;
import org.hibernate.dialect.PostgreSQL10Dialect;
import org.hibernate.dialect.SQLServer2012Dialect;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.resource.transaction.spi.DdlTransactionIsolator;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.tool.schema.extract.spi.ExtractionContext;
import org.hibernate.tool.schema.extract.spi.InformationExtractor;
import org.hibernate.tool.schema.internal.HibernateSchemaManagementTool;
import org.hibernate.tool.schema.spi.ExtractionTool;

public class ReactiveSchemaManagementTool extends HibernateSchemaManagementTool {

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		super.injectServices( serviceRegistry );
		setCustomDatabaseGenerationTarget( new ReactiveGenerationTarget( serviceRegistry ) );
	}

	@Override
	public ExtractionTool getExtractionTool() {
		return ReactiveExtractionTool.INSTANCE;
	}

	private static class ReactiveExtractionTool implements ExtractionTool {

		private static final ReactiveExtractionTool INSTANCE = new ReactiveExtractionTool();

		private ReactiveExtractionTool() {
		}

		@Override
		public ExtractionContext createExtractionContext(
				ServiceRegistry serviceRegistry,
				JdbcEnvironment jdbcEnvironment,
				SqlStringGenerationContext sqlStringGenerationContext,
				DdlTransactionIsolator ddlTransactionIsolator,
				ExtractionContext.DatabaseObjectAccess databaseObjectAccess) {
			return new ReactiveImprovedExtractionContextImpl(
					serviceRegistry,
					sqlStringGenerationContext,
					databaseObjectAccess
			);
		}

		public InformationExtractor createInformationExtractor(ExtractionContext extractionContext) {
			final Dialect dialect = extractionContext.getJdbcEnvironment().getDialect();
			if ( dialect instanceof PostgreSQL10Dialect ) {
				return new PostgreSqlReactiveInformationExtractorImpl( extractionContext );
			}
			if ( dialect instanceof CockroachDB201Dialect ) {
				return new PostgreSqlReactiveInformationExtractorImpl( extractionContext );
			}
			else if ( dialect instanceof MySQL8Dialect || dialect instanceof MariaDB103Dialect ) {
				return new MySqlReactiveInformationExtractorImpl( extractionContext );
			}
			else if ( dialect instanceof SQLServer2012Dialect ) {
				return new SqlServerReactiveInformationExtractorImpl( extractionContext );
			}
			else if ( dialect instanceof Oracle12cDialect ) {
				return new OracleSqlReactiveInformationExtractorImpl( extractionContext );
			}
			else {
				throw new NotYetImplementedException(
						"No InformationExtractor for Dialect [" + dialect + "] is implemented yet"
				);
			}
		}
	}
}
