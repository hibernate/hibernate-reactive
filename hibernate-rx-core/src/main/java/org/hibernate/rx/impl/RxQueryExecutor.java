package org.hibernate.rx.impl;

import io.vertx.mysqlclient.MySQLClient;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.Tuple;
import org.hibernate.JDBCException;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.rx.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.rx.adaptor.impl.ResultSetAdaptor;
import org.hibernate.rx.service.initiator.RxConnectionPoolProvider;
import org.hibernate.type.Type;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * A facade which simplifies execution of SQL against an
 * {@link org.hibernate.rx.service.RxConnection} obtained
 * via the {@link RxConnectionPoolProvider} service.
 */
public class RxQueryExecutor {

	public CompletionStage<Integer> update(String sql, Object[] paramValues, SessionFactoryImplementor factory) {
		Objects.requireNonNull( "Query for update cannot be null" );
		RxConnectionPoolProvider poolProvider = factory
				.getServiceRegistry()
				.getService( RxConnectionPoolProvider.class );

		return poolProvider.getConnection()
				.preparedQuery( sql, Tuple.wrap( paramValues ) ).thenApply(SqlResult::rowCount);
	}

	public CompletionStage<Long> updateReturning(String sql, Object[] paramValues, SessionFactoryImplementor factory) {
		Objects.requireNonNull( sql, "Query for update cannot be null" );
		RxConnectionPoolProvider poolProvider = factory
				.getServiceRegistry()
				.getService( RxConnectionPoolProvider.class );

		return poolProvider.getConnection()
				.preparedQuery( sql, Tuple.wrap( paramValues ) )
				.thenApply( rows -> {
					RowIterator<Row> iterator = rows.iterator();
					return iterator.hasNext() ?
							iterator.next().getLong(0) :
							rows.property(MySQLClient.LAST_INSERTED_ID);
				});
	}

	public CompletionStage<Long> selectLong(String sql, Object[] paramValues, SessionFactoryImplementor factory) {
		Objects.requireNonNull( "Select query for cannot be null" );
		RxConnectionPoolProvider poolProvider = factory
				.getServiceRegistry()
				.getService(RxConnectionPoolProvider.class);

		return poolProvider.getConnection()
				.preparedQuery( sql, Tuple.wrap( paramValues ) ).thenApply(rowSet -> {
					for (Row row: rowSet) {
						return row.getLong(0);
					}
					return null;
				});
	}

	/**
	 * @param transformer Convert the result of the query to a list of entities
	 */
	public CompletionStage<List<?>> execute(String sql, QueryParameters queryParameters,
										 SessionImplementor session,
										 Function<ResultSet, List<Object>> transformer) {
		Objects.requireNonNull( "Select query for cannot be null" );

		RxConnectionPoolProvider poolProvider = session.getSessionFactory()
				.getServiceRegistry()
				.getService( RxConnectionPoolProvider.class );

		return poolProvider.getConnection()
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
