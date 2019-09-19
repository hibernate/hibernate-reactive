package org.hibernate.rx;

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
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
import org.hibernate.rx.service.RxConnection;
import org.hibernate.rx.service.initiator.RxConnectionPoolProvider;
import org.hibernate.service.ServiceRegistry;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.axle.sqlclient.Tuple;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class ReactiveSessionTest {

	@Rule
	public Timeout rule = Timeout.seconds( 3600 );

	RxHibernateSession session = null;
	SessionFactoryImplementor sessionFactory = null;

	private static void test(TestContext context, CompletionStage<?> cs) {
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

	protected StandardServiceRegistryImpl buildServiceRegistry(
			BootstrapServiceRegistry bootRegistry,
			Configuration configuration) {
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
		sessionFactory = (SessionFactoryImplementor) configuration.buildSessionFactory( serviceRegistry );
		session = sessionFactory.unwrap( RxHibernateSessionFactory.class ).openRxSession();
	}

	@After
	// The current test should have already called context.async().complete();
	public void tearDown(TestContext context) {
		dropTable()
				.whenComplete( (res, err) -> {
					try {
						sessionFactory.close();
					}
					finally {
						context.assertNull( err );
					}
				} )
				.whenComplete( (res, err) -> {
					// DropTable worked but SessionFactory didn't close
					context.assertNull( err );
				} );
	}

	private CompletionStage<String> selectNameFromId(Integer id) {
		return connection().preparedQuery(
				"SELECT name FROM ReactiveSessionTest$GuineaPig WHERE id = $1", Tuple.of( id ) ).thenApply(
				rowSet -> {
					if ( rowSet.size() == 1 ) {
						// Only one result
						return rowSet.iterator().next().getString( 0 );
					}
					else if ( rowSet.size() > 1 ) {
						throw new AssertionError( "More than one result returned: " + rowSet.size() );
					}
					else {
						// Size 0
						return null;
					}
				} );
	}

	private CompletionStage<Integer> populateDB() {
		return connection().update( "INSERT INTO ReactiveSessionTest$GuineaPig (id, name) VALUES (5, 'Aloi')" );
	}

	private CompletionStage<Integer> dropTable() {
		return connection().update( "DROP TABLE ReactiveSessionTest$GuineaPig" );
	}

	private RxConnection connection() {
		RxConnectionPoolProvider poolProvider = sessionFactory.getServiceRegistry()
				.getService( RxConnectionPoolProvider.class );
		return poolProvider.getConnection();
	}

	@Test
	public void reactiveFind(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.thenCompose( v -> session.reactive().find( GuineaPig.class, expectedPig.getId() ) )
						.thenAccept( actualPig -> {
							assertThatPigsAreEqual( context, expectedPig, actualPig );
						} )
		);
	}

	@Test
	public void reactivePersist(TestContext context) {
		RxSession rxSession = session.reactive();
		test(
				context,
				rxSession.persist( new GuineaPig( 10, "Tulip" ) )
						.thenCompose( v -> rxSession.flush() )
						.thenCompose( v -> selectNameFromId( 10 ) )
						.thenAccept( selectRes -> context.assertEquals( "Tulip", selectRes ) )
		);
	}

	@Test
	public void reactiveRemoveTransientEntity(TestContext context) {
		RxSession rxSession = session.reactive();
		test(
				context,
				populateDB()
						.thenCompose( v -> selectNameFromId( 5 ) )
						.thenAccept( name -> context.assertNotNull( name ) )
						.thenCompose( v -> rxSession.remove( new GuineaPig( 5, "Aloi" ) ) )
						.thenCompose( v -> rxSession.flush() )
						.thenCompose( v -> selectNameFromId( 5 ) )
						.thenAccept( ret -> context.assertNull( ret ) )
		);
	}

	@Test
	public void reactiveRemoveManagedEntity(TestContext context) {
		RxSession rxSession = session.reactive();
		test(
				context,
				populateDB()
						.thenCompose( v -> rxSession.find( GuineaPig.class, 5 ) )
						.thenCompose( aloi -> rxSession.remove( aloi.get() ) )
						.thenCompose( v -> rxSession.flush() )
						.thenCompose( v -> selectNameFromId( 5 ) )
						.thenAccept( ret -> context.assertNull( ret )
						)
		);
	}

	@Test
	public void reactiveUpdate(TestContext context) {
		final String NEW_NAME = "Tina";
		RxSession rxSession = session.reactive();
		test(
				context,
				populateDB()
						.thenCompose( v -> rxSession.find( GuineaPig.class, 5 ) )
						.thenAccept( o -> {
							GuineaPig pig = o.orElseThrow( () -> new AssertionError( "Guinea pig not found" ) );
							// Checking we are actually changing the name
							context.assertNotEquals( pig.getName(), NEW_NAME );
							pig.setName( NEW_NAME );
						} )
						.thenCompose( v -> rxSession.flush() )
						.thenCompose( v -> selectNameFromId( 5 ) )
						.thenAccept( name -> context.assertEquals( NEW_NAME, name ) )
		);
	}

	private void assertThatPigsAreEqual(TestContext context, GuineaPig expected, Optional<GuineaPig> actual) {
		context.assertTrue( actual.isPresent() );
		context.assertEquals( expected.getId(), actual.get().getId() );
		context.assertEquals( expected.getName(), actual.get().getName() );
	}

	@Entity
	public static class GuineaPig {
		@Id
		private Integer id;
		private String name;

		public GuineaPig() {
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
			return id + ": " + name;
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
