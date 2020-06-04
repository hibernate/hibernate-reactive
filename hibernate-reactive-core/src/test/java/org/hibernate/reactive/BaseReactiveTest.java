/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.containers.DatabaseConfiguration.DBType;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.provider.service.ReactiveGenerationTarget;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.tool.schema.spi.SchemaManagementTool;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletionStage;

import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

@RunWith(VertxUnitRunner.class)
public abstract class BaseReactiveTest {

	@Rule
	public Timeout rule = Timeout.seconds( 5 * 60 );

	private Stage.Session session;
	private ReactiveConnection connection;
	private org.hibernate.SessionFactory sessionFactory;
	private ReactiveConnectionPool poolProvider;

	protected static void test(TestContext context, CompletionStage<?> cs) {
		// this will be added to TestContext in the next vert.x release
		Async async = context.async();
		cs.whenComplete( (res, err) -> {
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
		configuration.setProperty( Settings.HBM2DDL_AUTO, "create" );
		configuration.setProperty( Settings.URL, DatabaseConfiguration.getJdbcUrl() );
		//Use JAVA_TOOL_OPTIONS='-Dhibernate.show_sql=true'
		configuration.setProperty( Settings.SHOW_SQL, System.getProperty(Settings.SHOW_SQL, "false") );
		return configuration;
	}

	@Before
	public void before() {
		Configuration configuration = constructConfiguration();
		StandardServiceRegistry registry = new ReactiveServiceRegistryBuilder()
				.applySettings( configuration.getProperties() )
				.build();
		mysqlConfiguration( registry );
		sessionFactory = configuration.buildSessionFactory( registry );
		poolProvider = registry.getService( ReactiveConnectionPool.class );
	}

	/*
	 * MySQL doesn't implement 'drop table cascade constraints'.
	 *
	 * The reason this is a problem in our test suite is that we
	 * have lots of different schemas for the "same" table: Pig, Author, Book.
	 * A user would surely only have one schema for each table.
	 */
	private void mysqlConfiguration(StandardServiceRegistry registry) {
		registry.getService( ConnectionProvider.class ); //force the NoJdbcConnectionProvider to load first
		registry.getService( SchemaManagementTool.class )
				.setCustomDatabaseGenerationTarget( new ReactiveGenerationTarget(registry) {
					@Override
					public void prepare() {
						super.prepare();
						if ( dbType() == DBType.MYSQL ) {
							accept("set foreign_key_checks = 0");
						}
					}
					@Override
					public void release() {
						if ( dbType() == DBType.MYSQL ) {
							accept("set foreign_key_checks = 1");
						}
						super.release();
					}
				} );
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

	protected Stage.SessionFactory getSessionFactory() {
		return sessionFactory.unwrap( Stage.SessionFactory.class );
	}

	protected Stage.Session createSession() {
		if ( session != null && session.isOpen() ) {
			session.close();
		}
		session = getSessionFactory().createSession();
		return session;
	}

	protected CompletionStage<Stage.Session> openSession() {
		if ( session != null && session.isOpen() ) {
			session.close();
		}
		return getSessionFactory().openSession().thenApply( s -> session = s );
	}

	protected CompletionStage<ReactiveConnection> connection() {
		return poolProvider.getConnection().thenApply( c -> connection = c );
	}

}
