package org.hibernate.rx;

import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.hibernate.boot.model.naming.ImplicitNamingStrategyLegacyJpaImpl;
import org.hibernate.boot.registry.BootstrapServiceRegistry;
import org.hibernate.boot.registry.BootstrapServiceRegistryBuilder;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.boot.registry.internal.StandardServiceRegistryImpl;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.service.ServiceRegistry;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(VertxUnitRunner.class)
public class ReactiveSessionTest {

	@Rule
	public Timeout rule = Timeout.seconds( 60 );

	RxHibernateSession session = null;
	SessionFactoryImplementor sessionFactory = null;

	protected Configuration constructConfiguration() {
		Configuration configuration = new Configuration();
		configuration.setProperty( Environment.HBM2DDL_AUTO, "create-drop" );
		configuration.setImplicitNamingStrategy( ImplicitNamingStrategyLegacyJpaImpl.INSTANCE );
		configuration.setProperty( AvailableSettings.DIALECT, "org.hibernate.dialect.PostgreSQL9Dialect" );
		configuration.setProperty( AvailableSettings.DRIVER, "org.postgresql.Driver" );
		configuration.setProperty( AvailableSettings.USER, "hibernate-rx" );
		configuration.setProperty( AvailableSettings.PASS, "hibernate-rx" );
		configuration.setProperty( AvailableSettings.URL, "jdbc:postgresql://localhost:5432/hibernate-rx" );

		configuration.addAnnotatedClass( GuineaPig.class );
		return configuration;
	}

	protected BootstrapServiceRegistry buildBootstrapServiceRegistry() {
		final BootstrapServiceRegistryBuilder builder = new BootstrapServiceRegistryBuilder();
		builder.applyClassLoader( getClass().getClassLoader() );
		return builder.build();
	}

	protected StandardServiceRegistryImpl buildServiceRegistry(BootstrapServiceRegistry bootRegistry, Configuration configuration) {
		Properties properties = new Properties();
		properties.putAll( configuration.getProperties() );
		ConfigurationHelper.resolvePlaceHolders( properties );

		StandardServiceRegistryBuilder cfgRegistryBuilder = configuration.getStandardServiceRegistryBuilder();

		StandardServiceRegistryBuilder registryBuilder = new StandardServiceRegistryBuilder( bootRegistry, cfgRegistryBuilder.getAggregatedCfgXml() )
				.applySettings( properties );

		return (StandardServiceRegistryImpl) registryBuilder.build();
	}

	@Before
	public void init() {
		// for now, build the configuration to get all the property settings
		Configuration configuration = constructConfiguration();
		BootstrapServiceRegistry bootRegistry = buildBootstrapServiceRegistry();
		ServiceRegistry serviceRegistry = buildServiceRegistry( bootRegistry, configuration );
		// this is done here because Configuration does not currently support 4.0 xsd
		sessionFactory = ( SessionFactoryImplementor ) configuration.buildSessionFactory( serviceRegistry );
		session = sessionFactory.unwrap( RxHibernateSessionFactory.class ).openRxSession();
	}

	@Test
	public void testRegularPersist() {
		session.beginTransaction();
		session.persist( new GuineaPig( 2, "Aloi" ) );
		session.getTransaction().commit();
		System.out.println( "Wow!" );
	}

//	@Test
//	public void testRegularFind() {
//		session.be
//		sessionFactoryScope().inTransaction( (session) -> {
//			session.persist( new GuineaPig( 2, "Aloi" ) );
//		} );
//		sessionFactoryScope().inTransaction( (rxSession) -> {
//			GuineaPig guineaPig = session.find( GuineaPig.class, 2 );
//			System.out.println( "Wow!" );
//		} );
//	}

	@Test
	public void testReactivePersist(TestContext testContext) {
		Async async = testContext.async();
		final GuineaPig mibbles = new GuineaPig( 22, "Mibbles" );

		session.beginTransaction();
		RxSession rxSession = session.reactive();
		rxSession.persist( mibbles )
			.whenComplete( (pig, err) -> {
				if ( err == null ) {
					try {
						assertThat( pig ).isNull();
						System.out.println( "Complete persist" );
						session.getTransaction().commit();
						async.complete();
					}
					catch (Throwable t) {
						testContext.fail( t );
					}
				}
				else {
					System.out.println( "Error is not null" );
					testContext.fail( err );
				}
			} );
	}

	private void assertNoError(TestContext testContext, CompletionStage<?> stage) {
		stage.whenComplete( (res, err) -> {
			if (err != null) {
				testContext.fail( err );
			}
		} );
	}

//	@Test
//	public void testReactivePersistAndThenFind(VertxTestContext testContext) {
//		final GuineaPig mibbles = new GuineaPig( 22, "Mibbles" );
//
//		try {
//			// TODO: Tx should be simpler, not EntityTransaction. Allow only setRollback
//			session.reactive().inTransaction( (rx, tx) -> {
//				rx.persist( mibbles );
//				tx.commit();
//				testContext.completeNow();
//			} );
//		}
//		catch (Throwable t) {
//			testContext.failNow( t );
//		}
//		System.out.println( "" );
//
//		assertNoError( testContext, session.reactive().inTransaction( (rxSession, tx) -> {
//			rxSession.find( GuineaPig.class, mibbles.getId() )
//					.whenComplete( (pig, err) ->
//						rxAssert( testContext, () -> assertAll(
//								()-> assertThat(pig).isPresent().hasValue( mibbles ),
//								()-> assertThat(err).isNull() ) ) );
//		} ) );
//	}

	private void rxAssert(TestContext ctx, Runnable r) {
		try {
			r.run();
			ctx.async().complete();
		}
		catch ( Throwable t) {
			ctx.fail( t );
		}
	}

	@Entity
	public static class GuineaPig {
		@Id
		private Integer id;
		private String name;

		public GuineaPig() {
		}

		public GuineaPig(String name) {
			this.id = new Random().nextInt();
			this.name = name;
		}

		public GuineaPig(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			GuineaPig guineaPig = (GuineaPig) o;
			return Objects.equals( name, guineaPig.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}
	}
}
