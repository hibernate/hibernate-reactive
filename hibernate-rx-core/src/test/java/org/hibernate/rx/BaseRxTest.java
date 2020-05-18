package org.hibernate.rx;

import java.util.concurrent.CompletionStage;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.rx.containers.PostgreSQLDatabase;
import org.hibernate.rx.service.RxConnection;
import org.hibernate.rx.service.RxGenerationTarget;
import org.hibernate.rx.service.initiator.RxConnectionPoolProvider;
import org.hibernate.rx.util.impl.RxUtil;
import org.hibernate.tool.schema.spi.SchemaManagementTool;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public abstract class BaseRxTest {

	@Rule
	public Timeout rule = Timeout.seconds( 3600 );

	private RxSession session;
	private SessionFactory sessionFactory;
	private RxConnectionPoolProvider poolProvider;

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
		configuration.setProperty( AvailableSettings.HBM2DDL_AUTO, "create-drop" );
		configuration.setProperty( AvailableSettings.URL, PostgreSQLDatabase.getJdbcUrl() );
		configuration.setProperty( AvailableSettings.SHOW_SQL, "true" );
		return configuration;
	}

	@Before
	public void before() {
		StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
				.applySettings( constructConfiguration().getProperties() )
				.build();

		registry.getService( SchemaManagementTool.class )
				.setCustomDatabaseGenerationTarget( new RxGenerationTarget(registry) );

		sessionFactory = constructConfiguration().buildSessionFactory( registry );
		poolProvider = registry.getService( RxConnectionPoolProvider.class );

		// EITHER WAY WORKS:
		// session = sessionFactory.openSession().unwrap(RxSession.class);
		session = sessionFactory.unwrap( RxSessionFactory.class ).openRxSession();
	}

	@After
	public void after(TestContext context) {
		if ( session != null ) {
			session.close();
		}

		sessionFactory.close();
	}

	protected CompletionStage<RxSession> openSession() {
		return RxUtil.nullFuture().thenApply( v -> {
			if ( session != null ) {
				session.close();
			}
			session = sessionFactory.unwrap( RxSessionFactory.class ).openRxSession();
			return session;
		} );
	}

	protected RxConnection connection() {
		return poolProvider.getConnection();
	}

}
