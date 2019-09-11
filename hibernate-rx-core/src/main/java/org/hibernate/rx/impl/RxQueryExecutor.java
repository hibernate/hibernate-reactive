package org.hibernate.rx.impl;

import java.sql.ResultSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.cfg.NotYetImplementedException;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.rx.service.RxConnection;
import org.hibernate.rx.service.initiator.RxConnectionPoolProvider;

import io.vertx.pgclient.PgConnection;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.impl.ArrayTuple;

// This could be a service
public class RxQueryExecutor {

	public CompletionStage<Object> update(String sql, Object[] paramValues, SessionFactoryImplementor factory) {
		RxConnectionPoolProvider poolProvider = factory
				.getServiceRegistry()
				.getService( RxConnectionPoolProvider.class );

		CompletableFuture queryResult = new CompletableFuture();
		RxConnection connection = poolProvider.getConnection();
		connection.unwrap( PgPool.class ).getConnection( connectionAR -> {
			if ( connectionAR.succeeded() ) {
				SqlConnection pgConnection = connectionAR.result();
				pgConnection.preparedQuery( sql, asTuple( paramValues ), queryAR -> {
					if ( queryAR.succeeded() ) {
						queryResult.complete( queryAR.result() );
						pgConnection.close();
					}
					else {
						queryResult.completeExceptionally( queryAR.cause() );
					}
				} );
			}
			else {
				queryResult.completeExceptionally( connectionAR.cause() );
			}
		} );
		return queryResult;
	}

		/**
		 *
		 * @param sql
		 * @param queryParameters
		 * @param factory
		 * @param transformer Convert the result of the query to a list of entities
		 * @return
		 */
	public CompletionStage<Object> execute(String sql, QueryParameters queryParameters, SessionFactoryImplementor factory, Function<ResultSet, Object> transformer) {
		RxConnectionPoolProvider poolProvider = factory
				.getServiceRegistry()
				.getService( RxConnectionPoolProvider.class );

		CompletableFuture queryResult = new CompletableFuture();
		RxConnection connection = poolProvider.getConnection();
		connection.unwrap( PgPool.class ).getConnection( connectionAR -> {
			if ( connectionAR.succeeded() ) {
				SqlConnection pgConnection = connectionAR.result();
				pgConnection.preparedQuery( sql, asTuple( queryParameters ), queryAR -> {
					if ( queryAR.succeeded() ) {
						try {
							Optional<Object> entities = entities( transformer, queryAR.result() );
							queryResult.complete( entities );
						} catch ( Throwable t) {
							queryResult.completeExceptionally( t );
						} finally {
							pgConnection.close();
						}
					}
					else {
						queryResult.completeExceptionally( queryAR.cause() );
					}
				} );
			}
			else {
				queryResult.completeExceptionally( connectionAR.cause() );
			}
		} );
		return queryResult;
	}

	private Optional<Object> entities(
			Function<ResultSet, Object> transformer,
			RowSet rows) {
		PgResultSet resultSet = new PgResultSet( rows);
		List<Object> entities = (List<Object>) transformer.apply( resultSet );
		if ( entities.isEmpty() ) {
			return Optional.empty();
		}
		else if ( entities.size() == 1 ){
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
	    return ret;
	}
}
