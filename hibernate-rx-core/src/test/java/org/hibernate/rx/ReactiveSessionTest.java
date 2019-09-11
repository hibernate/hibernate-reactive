package org.hibernate.rx;

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
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
import org.hibernate.rx.service.RxConnectionPoolProviderImpl;
import org.hibernate.rx.service.initiator.RxConnectionPoolProvider;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.spi.Configurable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.axle.pgclient.PgPool;
import io.vertx.axle.sqlclient.RowSet;

@RunWith(VertxUnitRunner.class)
public class ReactiveSessionTest {

	@Rule
	public Timeout rule = Timeout.seconds( 3600 );

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

	private CompletionStage<String> selectNameFromId(Integer id) {
		RxConnectionPoolProvider provider = new RxConnectionPoolProviderImpl();
		( (Configurable) provider ).configure( constructConfiguration().getProperties() );
		RxConnection rxConn = provider.getConnection();
		PgPool client = rxConn.unwrap( PgPool.class );
		return invokeQuery( client, "SELECT name FROM ReactiveSessionTest$GuineaPig WHERE id = " + id ).thenApply( rowSet -> {
				if ( rowSet.size() == 1 ) {
					// Only one result
				    return rowSet.iterator().next().getString(0);
				}
				else if (rowSet.size() > 1) {
					throw new AssertionError( "More than one result returned: " + rowSet.size() );
				}
				else {
					// Size 0
					return null;
				}
		});
	}

	private CompletionStage<RowSet> populateDB(TestContext context) {
		RxConnectionPoolProvider provider = new RxConnectionPoolProviderImpl();
		( (Configurable) provider ).configure( constructConfiguration().getProperties() );
		RxConnection rxConn = provider.getConnection();
		PgPool client = rxConn.unwrap( PgPool.class );

		return invokeQuery( client, "INSERT INTO ReactiveSessionTest$GuineaPig (id, name) VALUES (5, 'Aloi')" );
	}

	private CompletionStage<RowSet> dropTable(TestContext context) {
		RxConnectionPoolProvider provider = new RxConnectionPoolProviderImpl();
		( (Configurable) provider ).configure( constructConfiguration().getProperties() );
		RxConnection rxConn = provider.getConnection();
		PgPool client = rxConn.unwrap( PgPool.class );

		return invokeQuery( client, "DROP TABLE ReactiveSessionTest$GuineaPig" );
	}

	private CompletionStage<RowSet> invokeQuery(PgPool client, String query) {
		// A simple query
	    // FIXME: pretty sure you should not close the entire pool
		return client.query(query).whenComplete((row, t) -> client.close());
	}

	@Test
	public void reactiveFind(TestContext context) {
		Async async = context.async();
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		populateDB( context ).whenComplete( (populate, populateErr) -> {
			context.assertNull( populateErr );

			RxSession rxSession = session.reactive();
			rxSession.find( GuineaPig.class, expectedPig.getId() )
					.whenComplete( (actualPig, pigEx) -> {
						context.assertNull( pigEx );
						assertThatPigsAreEqual( context, expectedPig, actualPig );
					} )
					.whenComplete( (check, checkErr) -> {
						context.assertNull( checkErr );
						dropTable( context )
								.whenComplete( (drop, dropErr) -> {
									context.assertNull( dropErr );
									async.complete();
								} );
					} );
		} );
	}

	private void assertThatPigsAreEqual(TestContext context, GuineaPig expected, Optional<GuineaPig> actual) {
		context.assertTrue( actual.isPresent() );
		context.assertEquals( expected.getId(), actual.get().getId() );
		context.assertEquals( expected.getName(), actual.get().getName() );
	};

	@Test
	public void reactivePersist(TestContext context) {
		Async async = context.async();
		RxSession rxSession = session.reactive();
		rxSession.persist( new GuineaPig( 10, "Tulip" ) )
				.whenComplete( (nope, err) -> {
					context.assertNull( err );

					selectNameFromId( 10 ).whenComplete( (selectRes, selectErr) -> {
						context.assertNull( selectErr );
						context.assertEquals( "Tulip", selectRes );
						async.complete();
					} );
				} );
		session.flush();
	}

	@Test
	public void reactiveRemove(TestContext context) {
		Async async = context.async();
		populateDB( context )
				.whenComplete( (popAR, popErr) -> {
				context.assertNull( popErr );

				RxSession rxSession = session.reactive();
				rxSession.remove( new GuineaPig( 5, "Aloi" ) )
						.whenComplete( (removeAR, removeErr) -> {
							context.assertNull( removeErr );

							selectNameFromId( 5 ).whenComplete( (selectRes, selectErr) -> {
								context.assertNull( selectErr );
								context.assertNull( selectRes );
								async.complete();
							} );
						} );
				session.flush();
		} );
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
