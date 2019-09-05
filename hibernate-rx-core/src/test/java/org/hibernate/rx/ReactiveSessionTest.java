package org.hibernate.rx;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
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

import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgConnection;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgPoolOptions;
import io.reactiverse.pgclient.PgRowSet;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

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

	private CompletionStage<Object> populateDB(TestContext context) {
		RxConnectionPoolProvider provider = new RxConnectionPoolProviderImpl();
		( (Configurable) provider ).configure( constructConfiguration().getProperties() );
		RxConnection rxConn = provider.getConnection();
		PgPool client = rxConn.unwrap( PgPool.class );

		BiConsumer<Object, Throwable> consumer = (o, t) -> {
			if (t != null) {
				System.out.println(t.toString());
				context.fail( t );
			} else {
				System.out.println(o.toString());
			}
		};

		return invokeQueryLater(client, "INSERT INTO ReactiveSessionTest$GuineaPig (id, name) VALUES (5, 'Aloi')" ).whenComplete(consumer);
	}

	private CompletionStage<Object> dropTable(TestContext context) {
		RxConnectionPoolProvider provider = new RxConnectionPoolProviderImpl();
		( (Configurable) provider ).configure( constructConfiguration().getProperties() );
		RxConnection rxConn = provider.getConnection();
		PgPool client = rxConn.unwrap( PgPool.class );

		BiConsumer<Object, Throwable> consumer = (o, t) -> {
			if (t != null) {
				System.out.println(t.toString());
				context.fail( t );
			} else {
				System.out.println(o.toString());
			}
		};

		return invokeQueryLater(client, "DROP TABLE ReactiveSessionTest$GuineaPig" ).whenComplete(consumer);
	}

	private CompletionStage<Object> invokeQueryLater(PgPool client, String query) {//, final BiConsumer<Object, Throwable> consumer) {
		// A simple query
		CompletableFuture c = new CompletableFuture<Object>();

		client.query(query, ar -> {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			if (ar.succeeded()) {
				PgRowSet result = ar.result();
				System.out.println("Got " + result.size() + " rows ");
				c.complete(result);
			} else {
				System.out.println("Failure: " + ar.cause().getMessage());
				c.completeExceptionally(ar.cause());
			}

			// Now close the pool
			client.close();
		});
		return c;
	}

	@Test
	public void reactiveFind(TestContext context) {
		Async async = context.async();
		try {
			populateDB(context).whenComplete( ( create, createEx) -> {
				RxSession rxSession = session.reactive();
				rxSession.find( GuineaPig.class, 1000 ).whenComplete( (pig, pigEx) -> {
					assertThat( pig ).hasValue( new GuineaPig( 1, "Aloi" ) );
					dropTable( context ).whenComplete( ( drop, dropEx) -> {
						async.complete();
					} );
				} );
			});
		}
		catch (Throwable t) {
			context.fail( t );
		}
	}

	@Test
	public void testReactivePersist(TestContext testContext) {
		Async async = testContext.async();
		final GuineaPig mibbles = new GuineaPig( 22, "Mibbles" );

		RxSession rxSession = session.reactive();
		rxSession.find( GuineaPig.class, 22 );
		rxSession.persist( mibbles )
			.whenComplete( (pig, err) -> {
				if ( err == null ) {
					try {
						assertThat( pig ).isNull();
						System.out.println( "Complete persist" );
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
		session.flush();
	}

	private void assertNoError(TestContext testContext, CompletionStage<?> stage) {
		stage.whenComplete( (res, err) -> {
			if (err != null) {
				testContext.fail( err );
			}
		} );
	}

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
