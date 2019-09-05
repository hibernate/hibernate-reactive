package org.hibernate.rx.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.rx.service.RxConnection;
import org.hibernate.rx.service.initiator.RxConnectionPoolProvider;

import io.reactiverse.pgclient.PgConnection;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgRowSet;
import io.reactiverse.pgclient.Tuple;
import io.reactiverse.pgclient.impl.ArrayTuple;

public class RxQueryExecutor {

	public CompletionStage<Object> execute(String sql, QueryParameters queryParameters, SessionFactoryImplementor factory) {
			RxConnectionPoolProvider poolProvider = factory
				.getServiceRegistry()
				.getService( RxConnectionPoolProvider.class );

		CompletableFuture stage = new CompletableFuture();
		try {
			RxConnection connection = poolProvider.getConnection();
			connection.unwrap( PgPool.class ).getConnection( ar1 -> {
				if ( ar1.succeeded() ) {
					PgConnection pgConnection = ar1.result();
					pgConnection.preparedQuery( sql, asTuple( queryParameters ), (ar2) -> {
						if ( ar2.succeeded() ) {
							PgRowSet rows = ar2.result();
							rows.forEach( (row) -> {
								System.out.println( row );
							} );
							stage.complete( rows );
							pgConnection.close();
						}
						else {
							stage.completeExceptionally( ar2.cause() );
						}
					} );
				}
				else {
					stage.completeExceptionally( ar1.cause() );
				}
			} );
			return stage;
		}
		catch (Throwable t) {
			throw new HibernateException( t );
		}
	}

	private Tuple asTuple(QueryParameters queryParameters) {
		ArrayTuple tuple = new ArrayTuple( queryParameters.getPositionalParameterValues().length );
		for ( Object value : queryParameters.getPositionalParameterValues() ) {
			tuple.add( value );
		}
		return tuple;
	}

}
