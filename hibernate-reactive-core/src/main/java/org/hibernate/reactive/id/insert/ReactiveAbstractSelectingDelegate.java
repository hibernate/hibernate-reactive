/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.insert;

import java.lang.invoke.MethodHandles;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.id.insert.Binder;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;

import static org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor.bind;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;

/**
 * @see org.hibernate.id.insert.AbstractSelectingDelegate
 */
public interface ReactiveAbstractSelectingDelegate extends ReactiveInsertGeneratedIdentifierDelegate {
	Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	String getSelectSQL();

	void bindParameters(Object entity, PreparedStatement ps, SharedSessionContractImplementor session);

	CompletionStage<GeneratedValues> extractReturningValues(ResultSet resultSet, SharedSessionContractImplementor session);

	@Override
	default CompletionStage<GeneratedValues> reactivePerformInsertReturning(
			String insertSQL,
			SharedSessionContractImplementor session,
			Binder binder) {
		Object[] updateParams = bind( binder::bindValues );

		final String selectSQL = getSelectSQL();
		ReactiveConnection reactiveConnection = ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
		return reactiveConnection
				.update( insertSQL, updateParams )
				.thenCompose( updated -> {
					Object[] selectParams = bind( statement -> bindParameters( binder.getEntity(), statement, session ) );
					return reactiveConnection
							.selectJdbc( selectSQL, selectParams )
							.handle( (resultSet, e) -> {
								if ( e != null ) {
									throw LOG.unableToExecutePostInsertIdSelectionQuery( selectSQL, e );
								}
								return resultSet;
							} );
				} )
				.thenCompose( resultSet -> {
					try {
						return extractReturningValues( resultSet, session );
					}
					catch (Throwable e) {
						return failedFuture( LOG.bindParametersForPostInsertIdSelectQueryError( selectSQL, e ) );
					}
				} );
	}
}
