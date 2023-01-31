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

import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.spi.JdbcCoordinator;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.adaptor.impl.PrepareStatementDetailsAdaptor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;

import static org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor.bind;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;

/**
 * @see org.hibernate.id.insert.AbstractSelectingDelegate
 */
public interface ReactiveAbstractSelectingDelegate extends ReactiveInsertGeneratedIdentifierDelegate {
	Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	String getSelectSQL();

	void bindParameters(Object entity, PreparedStatement ps, SharedSessionContractImplementor session);

	Object extractGeneratedValue(ResultSet resultSet, SharedSessionContractImplementor session);

	@Override
	default CompletionStage<Object> reactivePerformInsert(
			PreparedStatementDetails insertStatementDetails,
			JdbcValueBindings jdbcValueBindings,
			Object entity,
			SharedSessionContractImplementor session) {
		final JdbcCoordinator jdbcCoordinator = session.getJdbcCoordinator();
		final JdbcServices jdbcServices = session.getJdbcServices();

		jdbcServices.getSqlStatementLogger().logStatement( insertStatementDetails.getSqlString() );

		Object[] updateParams = bind( statement -> {
			PreparedStatementDetails details = new PrepareStatementDetailsAdaptor( insertStatementDetails, statement, session.getJdbcServices() );
			jdbcValueBindings.beforeStatement( details );
		} );

		final String selectSQL = getSelectSQL();
		ReactiveConnection reactiveConnection = ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
		return reactiveConnection
				.update( insertStatementDetails.getSqlString(), updateParams )
				.thenCompose( updated -> {
					Object[] selectParams = bind( statement -> bindParameters( entity, statement, session ) );
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
						return completedFuture( extractGeneratedValue( resultSet, session ) );
					}
					catch (Throwable e) {
						return failedFuture( LOG.bindParametersForPostInsertIdSelectQueryError( selectSQL, e ) );
					}
				} );
	}

}
