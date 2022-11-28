/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.query.results.ResultSetMappingImpl;
import org.hibernate.reactive.adaptor.impl.ResultSetAdaptor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.sql.results.internal.ReactiveResultSetAccess;
import org.hibernate.reactive.sql.results.spi.ReactiveValuesMappingProducer;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMapping;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMetadata;

public class ReactiveResultSetMapping extends ResultSetMappingImpl implements ReactiveValuesMappingProducer {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveResultSetMapping(String mappingIdentifier) {
		super( mappingIdentifier );
	}

	public ReactiveResultSetMapping(String mappingIdentifier, boolean isDynamic) {
		super( mappingIdentifier, isDynamic );
	}

	public JdbcValuesMapping resolve(JdbcValuesMetadata jdbcResultsMetadata, SessionFactoryImplementor sessionFactory) {
		throw LOG.nonReactiveMethodCall( "reactiveResolve" );
	}

	public CompletionStage<JdbcValuesMapping> reactiveResolve(JdbcValuesMetadata jdbcResultsMetadata, SessionFactoryImplementor sessionFactory) {
		return ( (ReactiveResultSetAccess) jdbcResultsMetadata )
				.getReactiveResultSet()
				.thenApply( resultSet -> super.resolve( (ResultSetAdaptor) resultSet, sessionFactory ) );
	}
}
