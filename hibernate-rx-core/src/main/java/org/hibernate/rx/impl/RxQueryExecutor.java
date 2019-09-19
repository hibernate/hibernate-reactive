package org.hibernate.rx.impl;

import java.sql.ResultSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.cfg.NotYetImplementedException;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.rx.service.RxConnection;
import org.hibernate.rx.service.initiator.RxConnectionPoolProvider;

import io.vertx.axle.sqlclient.RowSet;
import io.vertx.axle.sqlclient.Tuple;
import io.vertx.sqlclient.impl.ArrayTuple;

// This could be a service
public class RxQueryExecutor {

	public CompletionStage<Integer> update(String sql, Object[] paramValues, SessionFactoryImplementor factory) {
		RxConnectionPoolProvider poolProvider = factory
				.getServiceRegistry()
				.getService( RxConnectionPoolProvider.class );

		RxConnection connection = poolProvider.getConnection();
		return connection.preparedQuery( sql, asTuple( paramValues )).thenApply( res -> res.rowCount() );
	}

		/**
		 *
		 * @param sql
		 * @param queryParameters
		 * @param factory
		 * @param transformer Convert the result of the query to a list of entities
		 * @return
		 */
	public CompletionStage<Optional<Object>> execute(String sql, QueryParameters queryParameters, SessionFactoryImplementor factory, Function<ResultSet, Object> transformer) {
		RxConnectionPoolProvider poolProvider = factory
				.getServiceRegistry()
				.getService( RxConnectionPoolProvider.class );

		RxConnection connection = poolProvider.getConnection();
		return connection.preparedQuery( sql, asTuple( queryParameters ) )
				.thenApply( rowset -> entities( transformer, (RowSet) rowset ) );
	}

	private Optional<Object> entities(
			Function<ResultSet, Object> transformer,
			RowSet rows) {
		PgResultSet resultSet = new PgResultSet( rows );
		List<Object> entities = (List<Object>) transformer.apply( resultSet );
		if ( entities.isEmpty() ) {
			return Optional.empty();
		}
		else if ( entities.size() == 1 ) {
			return Optional.ofNullable( entities.get( 0 ) );
		}
		else {
			throw new NotYetImplementedException( "Returning more than one entity doesn't work at the moment" );
		}
	}

	private Tuple asTuple(QueryParameters queryParameters) {
		Object[] values = queryParameters.getPositionalParameterValues();
		return asTuple( values );
	}

	private Tuple asTuple(Object[] values) {
	    // FIXME: report this bug to vertx, we get a CCE if we use Tuple.wrap here
//	    return Tuple.wrap(values);
	    ArrayTuple ret = new ArrayTuple(values.length);
	    for (Object object : values) {
	        ret.add(object);
        }
	    return Tuple.newInstance(ret);
	}
}
