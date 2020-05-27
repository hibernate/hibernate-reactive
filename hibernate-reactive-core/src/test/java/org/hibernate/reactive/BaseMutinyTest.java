/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.boot.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.stage.Stage;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runner.RunWith;

import io.smallrye.mutiny.Uni;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public abstract class BaseMutinyTest {

	@Rule
	public Timeout rule = Timeout.seconds( 5 * 60 );

	private Mutiny.Session session;
	private ReactiveConnection connection;
	private org.hibernate.SessionFactory sessionFactory;
	private ReactiveConnectionPool poolProvider;

	protected static void test(TestContext context, Uni<?> cs) {
		// this will be added to TestContext in the next vert.x release
		Async async = context.async();
		cs.convert()
				.toCompletionStage()
				.whenComplete( (res, err) -> {
					if ( res instanceof Stage.Session ) {
						Stage.Session s = (Stage.Session) res;
						if ( s.isOpen() ) {
							s.close();
						}
					}
					if ( err != null ) {
						context.fail( err );
					}
					else {
						async.complete();
					}
				} );
	}

	protected Configuration constructConfiguration() {
		Configuration configuration = new Configuration();
		configuration.setProperty( AvailableSettings.HBM2DDL_AUTO, "create" );
		configuration.setProperty( AvailableSettings.URL, DatabaseConfiguration.getJdbcUrl() );
		configuration.setProperty( AvailableSettings.SHOW_SQL, "true" );
		return configuration;
	}

	@Before
	public void before() {
		StandardServiceRegistry registry = new ReactiveServiceRegistryBuilder()
				.applySettings( constructConfiguration().getProperties() )
				.build();

		sessionFactory = constructConfiguration().buildSessionFactory( registry );
		poolProvider = registry.getService( ReactiveConnectionPool.class );
	}

	@After
	public void after(TestContext context) {
		if ( session != null && session.isOpen() ) {
			session.close();
			session = null;
		}
		if ( connection != null ) {
			try {
				connection.close();
			}
			catch (Exception e) {}
			finally {
				connection = null;
			}
		}

		sessionFactory.close();
	}

	protected Uni<Mutiny.Session> openSession() {
		if ( session != null ) {
			session.close();
		}
		return getSessionFactory().openSession()
				.onItem().invoke( s -> session = s );
	}

	protected Mutiny.SessionFactory getSessionFactory() {
		return sessionFactory.unwrap( Mutiny.SessionFactory.class );
	}

	protected Uni<ReactiveConnection> connection() {
		return Uni.createFrom().completionStage( poolProvider.getConnection() )
				.map( c -> connection = c );
	}

}
