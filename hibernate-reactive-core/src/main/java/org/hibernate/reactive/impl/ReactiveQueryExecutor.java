package org.hibernate.reactive.impl;

import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import org.hibernate.JDBCException;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.adaptor.impl.ResultSetAdaptor;
import org.hibernate.reactive.service.ReactiveConnection;
import org.hibernate.reactive.service.initiator.ReactiveConnectionPoolProvider;
import org.hibernate.type.Type;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * A facade which simplifies execution of SQL against an
 * {@link ReactiveConnection} obtained
 * via the {@link ReactiveConnectionPoolProvider} service.
 */
public class ReactiveQueryExecutor {

	public CompletionStage<Integer> update(String sql, Object[] paramValues,
										   SharedSessionContractImplementor session) {
		Objects.requireNonNull( sql, "Query for update cannot be null" );

		return ( (ReactiveSessionInternal) session ).getReactiveConnection()
				.update( sql, Tuple.wrap( paramValues ) );
	}

	public CompletionStage<Long> updateReturning(String sql, Object[] paramValues,
												 SharedSessionContractImplementor session) {
		Objects.requireNonNull( sql, "Query for update cannot be null" );

		return ( (ReactiveSessionInternal) session ).getReactiveConnection()
				.updateReturning( sql, Tuple.wrap( paramValues ) );
	}

	public CompletionStage<Long> selectLong(String sql, Object[] paramValues,
											SharedSessionContractImplementor session) {
		Objects.requireNonNull( sql, "Select query for cannot be null" );

		return ( (ReactiveSessionInternal) session ).getReactiveConnection()
				.preparedQuery( sql, Tuple.wrap( paramValues ) )
				.thenApply( rowSet -> {
					for (Row row: rowSet) {
						return row.getLong(0);
					}
					return null;
				} );
	}

	/**
	 * @param transformer Convert the result of the query to a list of entities
	 */
	public <T> CompletionStage<T> execute(String sql, QueryParameters queryParameters,
										  SessionImplementor session,
										  Function<ResultSet, T> transformer) {
		Objects.requireNonNull( sql, "Select query for cannot be null" );

		return ( (ReactiveSessionInternal) session ).getReactiveConnection()
				.preparedQuery( sql, asTuple( queryParameters, session ) )
				.thenApply( rowset -> transformer.apply( new ResultSetAdaptor(rowset) ) );
	}

	private Tuple asTuple(QueryParameters queryParameters, SessionImplementor session) {
		PreparedStatementAdaptor adaptor = new PreparedStatementAdaptor();
		Type[] types = queryParameters.getPositionalParameterTypes();
		Object[] values = queryParameters.getPositionalParameterValues();
		int n = 1;
		for (int i = 0; i < types.length; i++) {
			Type type = types[i];
			Object value = values[i];
			try {
				type.nullSafeSet(adaptor, value, n, session);
				n += type.getColumnSpan( session.getSessionFactory() );
			}
			catch (SQLException e) {
				//can never happen
				throw new JDBCException("error binding parameters", e);
			}
		}
		return Tuple.wrap( adaptor.getParametersAsArray() );
	}

}
