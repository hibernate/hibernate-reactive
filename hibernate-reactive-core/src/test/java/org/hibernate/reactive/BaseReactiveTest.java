package org.hibernate.reactive;

import java.util.concurrent.CompletionStage;

import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.containers.PostgreSQLDatabase;
import org.hibernate.reactive.service.ReactiveConnection;
import org.hibernate.reactive.service.initiator.ReactiveConnectionPoolProvider;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public abstract class BaseReactiveTest {

	@Rule
	public Timeout rule = Timeout.seconds( 3600 );

	private Stage.Session session;
	private org.hibernate.SessionFactory sessionFactory;
	private ReactiveConnectionPoolProvider poolProvider;

	protected static void test(TestContext context, CompletionStage<?> cs) {
		// this will be added to TestContext in the next vert.x release
		Async async = context.async();
		cs.whenComplete( (res, err) -> {
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
		configuration.setProperty( AvailableSettings.URL, PostgreSQLDatabase.getJdbcUrl() );
		configuration.setProperty( AvailableSettings.SHOW_SQL, "true" );
		return configuration;
	}

	@Before
	public void before() {
		StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
				.applySettings( constructConfiguration().getProperties() )
				.build();

		sessionFactory = constructConfiguration().buildSessionFactory( registry );
		poolProvider = registry.getService( ReactiveConnectionPoolProvider.class );

		// EITHER WAY WORKS:
		// session = sessionFactory.openSession().unwrap(Session.class);
		session = sessionFactory.unwrap( Stage.SessionFactory.class ).openReactiveSession();
	}

	@After
	public void after(TestContext context) {
		if ( session != null ) {
			session.close();
		}

		sessionFactory.close();
	}

	protected CompletionStage<Stage.Session> openSession() {
		return CompletionStages.nullFuture().thenApply(v -> {
			if ( session != null ) {
				session.close();
			}
			session = sessionFactory.unwrap( Stage.SessionFactory.class ).openReactiveSession();
			return session;
		} );
	}

	protected ReactiveConnection connection() {
		return poolProvider.getConnection();
	}

}
