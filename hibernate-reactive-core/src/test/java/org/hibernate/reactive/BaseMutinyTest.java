package org.hibernate.reactive;

import io.smallrye.mutiny.Uni;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.containers.PostgreSQLDatabase;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.service.ReactiveConnection;
import org.hibernate.reactive.service.ReactiveConnectionPool;
import org.hibernate.reactive.stage.Stage;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletionStage;

@RunWith(VertxUnitRunner.class)
public abstract class BaseMutinyTest {

	@Rule
	public Timeout rule = Timeout.seconds( 3600 );

	private Mutiny.Session session;
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
		return sessionFactory.unwrap( Mutiny.SessionFactory.class ).openReactiveSession()
				.onItem().invoke( s -> session = s );
	}

	protected Uni<ReactiveConnection> connection() {
		return Uni.createFrom().completionStage( poolProvider.getConnection() )
				.map( c -> connection = c );
	}

}
