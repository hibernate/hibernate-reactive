/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.HibernateException;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.dialect.CockroachDialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.DialectDelegateWrapper;
import org.hibernate.dialect.MariaDBDialect;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.dialect.OracleDialect;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.dialect.SQLServerDialect;
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
			Dialect dialect = extractionContext.getJdbcEnvironment().getDialect();
			//Allow for wrapped cases:
			if ( dialect instanceof DialectDelegateWrapper ) {
				dialect = ( (DialectDelegateWrapper) dialect ).getWrappedDialect();
			}
			//Now detect the kind of Dialect:
			if ( dialect instanceof PostgreSQLDialect || dialect instanceof CockroachDialect ) {
				return new PostgreSqlReactiveInformationExtractorImpl( extractionContext );
			}
			if ( dialect instanceof MySQLDialect || dialect instanceof MariaDBDialect ) {
				return new MySqlReactiveInformationExtractorImpl( extractionContext );
			}
			if ( dialect instanceof SQLServerDialect ) {
				return new SqlServerReactiveInformationExtractorImpl( extractionContext );
			}
			if ( dialect instanceof OracleDialect ) {
				return new OracleSqlReactiveInformationExtractorImpl( extractionContext );
			}

			throw new HibernateException( "No InformationExtractor for Dialect [" + dialect + "] is implemented yet" );
		}
	}
}
