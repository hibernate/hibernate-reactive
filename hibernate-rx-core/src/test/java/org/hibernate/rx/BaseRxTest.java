package org.hibernate.rx;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.BootstrapServiceRegistry;
import org.hibernate.boot.registry.BootstrapServiceRegistryBuilder;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.rx.service.RxConnection;
import org.hibernate.rx.service.initiator.RxConnectionPoolProvider;
import org.hibernate.rx.util.impl.RxUtil;
import org.hibernate.service.ServiceRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runner.RunWith;

import java.util.Properties;
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
		configuration.setProperty( Environment.HBM2DDL_AUTO, "create-drop" );
		configuration.setProperty( AvailableSettings.URL, "jdbc:postgresql://localhost:5432/hibernate-rx?user=hibernate-rx&password=hibernate-rx" );
		configuration.setProperty( AvailableSettings.SHOW_SQL, "true" );
		return configuration;
	}

	protected BootstrapServiceRegistry buildBootstrapServiceRegistry() {
		final BootstrapServiceRegistryBuilder builder = new BootstrapServiceRegistryBuilder();
		builder.applyClassLoader( getClass().getClassLoader() );
		return builder.build();
	}

	protected StandardServiceRegistry buildServiceRegistry(
			BootstrapServiceRegistry bootRegistry,
			Configuration configuration) {
		Properties properties = new Properties();
		properties.putAll( configuration.getProperties() );
		ConfigurationHelper.resolvePlaceHolders( properties );

		StandardServiceRegistryBuilder cfgRegistryBuilder = configuration.getStandardServiceRegistryBuilder();
		StandardServiceRegistryBuilder registryBuilder = new StandardServiceRegistryBuilder( bootRegistry, cfgRegistryBuilder.getAggregatedCfgXml() )
				.applySettings( properties );

		return registryBuilder.build();
	}

	@Before
	public void before() {
		// for now, build the configuration to get all the property settings
		Configuration configuration = constructConfiguration();
		BootstrapServiceRegistry bootRegistry = buildBootstrapServiceRegistry();
		ServiceRegistry serviceRegistry = buildServiceRegistry( bootRegistry, configuration );
		// this is done here because Configuration does not currently support 4.0 xsd
		sessionFactory = configuration.buildSessionFactory( serviceRegistry );
		poolProvider = serviceRegistry.getService(RxConnectionPoolProvider.class);
		session = sessionFactory.unwrap( RxSessionFactory.class ).openRxSession();
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
