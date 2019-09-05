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
import io.reactiverse.pgclient.PgPreparedQuery;
import io.reactiverse.pgclient.PgRowSet;

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
					pgConnection.prepare( sql, (ar2) -> {
						if ( ar2.succeeded() ) {
							PgPreparedQuery preparedQuery = ar2.result();
							// TODO: Set parameters
							preparedQuery.execute( (queryResult) -> {
								final PgRowSet result = queryResult.result();
								stage.complete( result );
								pgConnection.close();
							} );
						}
					} );
				}
				else {
					throw new HibernateException( ar1.cause() );
				}
			} );
			return stage;
		}
		catch (Throwable t) {
			throw new HibernateException( t );
		}
	}

}
