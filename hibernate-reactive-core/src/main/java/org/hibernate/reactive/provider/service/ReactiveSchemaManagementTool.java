/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.cfg.NotYetImplementedException;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.PostgreSQL10Dialect;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.resource.transaction.spi.DdlTransactionIsolator;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.tool.schema.extract.spi.ExtractionContext;
import org.hibernate.tool.schema.extract.spi.InformationExtractor;
import org.hibernate.tool.schema.internal.HibernateSchemaManagementTool;

public class ReactiveSchemaManagementTool extends HibernateSchemaManagementTool {

	public ExtractionContext createExtractionContext(
			ServiceRegistry serviceRegistry,
			JdbcEnvironment jdbcEnvironment,
			DdlTransactionIsolator ddlTransactionIsolator,
			Identifier defaultCatalog,
			Identifier defaultSchema,
			ExtractionContext.DatabaseObjectAccess databaseObjectAccess) {
		return new ReactiveImprovedExtractionContextImpl(
				serviceRegistry,
				defaultCatalog,
				defaultSchema,
				databaseObjectAccess
		);
	}

	public InformationExtractor createInformationExtractor(ExtractionContext extractionContext) {
		final Dialect dialect = getServiceRegistry().getService( JdbcEnvironment.class ).getDialect();
		if ( dialect instanceof PostgreSQL10Dialect ) {
			return new PostgreSqlReactiveInformationExtractorImpl( extractionContext );
		}
		else {
			throw new NotYetImplementedException(
					"No InformationExtractor for Dialect [" + dialect + "] is implemented yet"
			);
		}
	}
}
