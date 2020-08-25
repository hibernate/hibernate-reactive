/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.smallrye.mutiny.Uni;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public abstract class BaseMutinyTest {

	@Rule
	public Timeout rule = Timeout.seconds( 5 * 60 );

	private Mutiny.Session session;
	private ReactiveConnection connection;
	private org.hibernate.SessionFactory sessionFactory;
	private ReactiveConnectionPool poolProvider;

	protected static void test(TestContext context, Uni<?> uni) {
		Async async = context.async();
		uni.subscribe().with(
				res -> {
					if ( res instanceof Mutiny.Session) {
						Mutiny.Session session = (Mutiny.Session) res;
						if ( session.isOpen() ) {
							session.close();
						}
					}
					async.complete();
				},
				throwable -> {
					context.fail( throwable );
				}
		);
	}

	protected Configuration constructConfiguration() {
		Configuration configuration = new Configuration();
		configuration.setProperty( Settings.HBM2DDL_AUTO, "create" );
		configuration.setProperty( Settings.URL, DatabaseConfiguration.getJdbcUrl() );

		//Use JAVA_TOOL_OPTIONS='-Dhibernate.show_sql=true'
		configuration.setProperty( Settings.SHOW_SQL, System.getProperty(Settings.SHOW_SQL, "false") );
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

	protected Mutiny.Session openSession() {
		if ( session != null ) {
			session.close();
		}
		session = getSessionFactory().openSession();
		return session;
	}

	protected Mutiny.SessionFactory getSessionFactory() {
		return sessionFactory.unwrap( Mutiny.SessionFactory.class );
	}

	protected Uni<ReactiveConnection> connection() {
		return Uni.createFrom().completionStage( poolProvider.getConnection() )
				.map( c -> connection = c );
	}

}
