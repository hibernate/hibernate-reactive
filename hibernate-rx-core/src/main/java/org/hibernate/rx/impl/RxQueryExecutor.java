package org.hibernate.rx.impl;

import io.vertx.mutiny.mysqlclient.MySQLClient;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.mutiny.sqlclient.SqlResult;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.sqlclient.impl.ArrayTuple;
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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * A facade which simplifies execution of SQL against an
 * {@link org.hibernate.rx.service.RxConnection} obtained
 * via the {@link RxConnectionPoolProvider} service.
 */
public class RxQueryExecutor {

	public CompletionStage<Integer> update(String sql, Object[] paramValues, SessionFactoryImplementor factory) {
		RxConnectionPoolProvider poolProvider = factory
				.getServiceRegistry()
				.getService( RxConnectionPoolProvider.class );

		return poolProvider.getConnection()
				.preparedQuery( sql, asTuple(paramValues) ).thenApply(SqlResult::rowCount);
	}

	public CompletionStage<Optional<Integer>> updateReturning(String sql, Object[] paramValues, SessionFactoryImplementor factory) {
		RxConnectionPoolProvider poolProvider = factory
				.getServiceRegistry()
				.getService( RxConnectionPoolProvider.class );

		return poolProvider.getConnection()
				.preparedQuery( sql, asTuple(paramValues) )
				.thenApply( rows -> {
					RowIterator<Row> iterator = rows.iterator();
					Integer id = iterator.hasNext() ?
							iterator.next().getInteger(0) :
							rows.property(MySQLClient.LAST_INSERTED_ID);
					return Optional.ofNullable(id);
				});
	}

	public CompletionStage<Optional<Long>> selectLong(String sql, Object[] paramValues, SessionFactoryImplementor factory) {
		RxConnectionPoolProvider poolProvider = factory
				.getServiceRegistry()
				.getService(RxConnectionPoolProvider.class);

		return poolProvider.getConnection()
				.preparedQuery( sql, asTuple( paramValues) ).thenApply(rowSet -> {
					for (Row row: rowSet) {
						return Optional.ofNullable( row.getLong(0) );
					}
					return Optional.empty();
				});
	}

	/**
	 * @param transformer Convert the result of the query to a list of entities
	 */
	public CompletionStage<List<?>> execute(String sql, QueryParameters queryParameters,
										 SessionImplementor session,
										 Function<ResultSet, List<Object>> transformer) {
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
		return asTuple( adaptor.getParametersAsArray() );
	}

	private Tuple asTuple(Object[] values) {
		// FIXME: report this bug to vertx, we get a CCE if we use Tuple.wrap here
//	    return Tuple.wrap(values);
		ArrayTuple ret = new ArrayTuple( values.length );
		Collections.addAll( ret, values );
		return Tuple.newInstance( ret );
	}

}
