package org.hibernate.rx;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.rx.service.RxConnection;
import org.hibernate.rx.service.initiator.RxConnectionPoolProvider;
import org.hibernate.rx.util.impl.RxUtil;
import org.hibernate.service.ServiceRegistry;
import org.junit.*;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletionStage;

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
		configuration.setProperty( AvailableSettings.URL, "jdbc:postgresql://localhost:5432/hibernate-rx?user=hibernate-rx&password=hibernate-rx" );
//		configuration.setProperty( AvailableSettings.URL, "jdbc:mysql://localhost:3306/hibernaterx?user=hibernate-rx&password=hibernate-rx" );
//		configuration.setProperty( AvailableSettings.DIALECT, MySQL8Dialect.class.getName());
		configuration.setProperty( AvailableSettings.SHOW_SQL, "true" );
		return configuration;
	}

	@Before
	public void before() {
		StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
				.applySettings( constructConfiguration().getProperties() )
				.build();

		sessionFactory = constructConfiguration().buildSessionFactory(registry);
		poolProvider = registry.getService(RxConnectionPoolProvider.class);

		session = sessionFactory.unwrap(RxSessionFactory.class).openRxSession();
	}

	@After
	public void after(TestContext context) {
		if (session != null) {
			session.close();
		}

		sessionFactory.close();
	}

	protected CompletionStage<RxSession> openSession() {
		return RxUtil.nullFuture().thenApply( v -> {
			if (session != null) {
				session.close();
			}
			session = sessionFactory.unwrap( RxSessionFactory.class ).openRxSession();
			return session;
		});
	}

	protected RxConnection connection() {
		return poolProvider.getConnection();
	}

}
