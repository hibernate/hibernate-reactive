package org.hibernate.rx;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.persistence.*;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

@RunWith(VertxUnitRunner.class)
public class BasicTypesTest {

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

	@Rule
	public Timeout rule = Timeout.seconds( 3600 );

	RxHibernateSession session = null;
	SessionFactoryImplementor sessionFactory = null;

	protected Configuration constructConfiguration() {
		Configuration configuration = new Configuration();
		configuration.setProperty( Environment.HBM2DDL_AUTO, "create-drop" );
		configuration.setProperty( AvailableSettings.SHOW_SQL, "true" );
		configuration.setProperty( AvailableSettings.URL, "jdbc:postgresql://localhost:5432/hibernate-rx?user=hibernate-rx&password=hibernate-rx" );

		configuration.addAnnotatedClass( BasicTypes.class );
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
		sessionFactory.close();
	}
//
//	private RxConnection connection() {
//		RxConnectionPoolProvider poolProvider = sessionFactory.getServiceRegistry()
//				.getService( RxConnectionPoolProvider.class );
//		return poolProvider.getConnection();
//	}

	@Test
	public void testBasicTypes(TestContext context) {

		BasicTypes b = new BasicTypes();
		b.string = "Hello World";
		b.decimal = new BigDecimal(12.12d);
		b.integer = BigInteger.valueOf(123L);
		b.bytes =  "hello world".getBytes();
		b.thing = Locale.getDefault();
		b.timeZone = TimeZone.getDefault();
		b.cover = Cover.hard;
		b.binInteger = BigInteger.valueOf(12345L);
		b.basicTypes = new BasicTypes();
		b.localDate = LocalDate.now();
		b.localDateTime = LocalDateTime.now();
		b.localTime = LocalTime.now();
		b.date = new Date(2000,1,1);
		b.thing = new String[] {"hello", "world"};

		RxSession s = session.reactive();
		test( context,
				s.persist(b.basicTypes)
				.thenCompose(v -> s.persist(b))
				.thenCompose(v -> s.flush())
				.thenApply(newSession())
				.thenCompose( s2 ->
					s2.find( BasicTypes.class, b.getId() )
						.thenAccept( bt -> {
							context.assertTrue( bt.isPresent() );
							BasicTypes bb = bt.get();
							context.assertEquals( bb.string, b.string);
							context.assertEquals( bb.decimal.floatValue(), b.decimal.floatValue());
							context.assertEquals( bb.integer, b.integer);
							context.assertTrue( Arrays.equals(bb.bytes, b.bytes) );
							context.assertEquals( bb.timeZone, b.timeZone);
							context.assertEquals( bb.cover, b.cover);
							context.assertEquals( bb.binInteger.floatValue(), b.binInteger.floatValue());
							context.assertEquals( bb.date, b.date );
							context.assertEquals( bb.localDate, b.localDate );
							context.assertEquals( bb.localDateTime, b.localDateTime );
							context.assertEquals( bb.localTime.truncatedTo(ChronoUnit.MINUTES),
									b.localTime.truncatedTo(ChronoUnit.MINUTES) );
							context.assertTrue( bb.thing instanceof String[] );
							context.assertTrue( Objects.deepEquals(bb.thing, b.thing) );
							context.assertEquals( bb.version, 0 );

							bb.string = "Goodbye";
							bb.cover = Cover.soft;
						})
						.thenCompose(vv -> s2.flush())
						.thenCompose(vv -> s2.find( BasicTypes.class, b.getId() ))
						.thenAccept( bt -> {
							context.assertEquals( bt.get().version, 1 );
						}))
				.thenApply(newSession())
				.thenCompose( s3 -> s3.find( BasicTypes.class, b.getId() ) )
				.thenAccept( bt -> {
					BasicTypes bb = bt.get();
					context.assertEquals(bb.version, 1);
					context.assertEquals( bb.string, "Goodbye");
				})
		);
	}

	private Function<Void, RxSession> newSession() {
		return v -> {
			session.close();
			session = sessionFactory.unwrap( RxHibernateSessionFactory.class ).openRxSession();
			return session.reactive();
		};
	}

	enum Cover { hard, soft }

	@Entity
	public static class BasicTypes {
		@Id @GeneratedValue Integer id;
		@Version Integer version;
		String string;

		boolean primitiveBoolean;
		int primitiveInt;
		long primitiveLong;
		float primitiveFloat;
		double primitiveDouble;
		byte primitiveByte;

		Boolean nullBoolean;
		Integer nullInt;
		Long nullLong;
		Float nullFloat;
		Double nullDouble;
		Byte nullByte;

		BigDecimal decimal;
		BigInteger integer;
		byte[] bytes;
		Cover cover;
		@Basic Serializable thing;
		TimeZone timeZone;
		@Temporal(TemporalType.DATE)
		private Date date;
		@Column(name="_localDate")
		private LocalDate localDate;
		@Column(name="_localDateTime")
		private LocalDateTime localDateTime;
		@Column(name="_localTime")
		private LocalTime localTime;
		@Convert(converter = BinInteger.class)
		private BigInteger binInteger;

		@ManyToOne(fetch = FetchType.LAZY)
		BasicTypes basicTypes;

		public BasicTypes() {
		}

		public BasicTypes(Integer id, String string) {
			this.id = id;
			this.string = string;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getString() {
			return string;
		}

		public void setString(String string) {
			this.string = string;
		}

		@Override
		public String toString() {
			return id + ": " + string;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			BasicTypes basicTypes = (BasicTypes) o;
			return Objects.equals(string, basicTypes.string);
		}

		@Override
		public int hashCode() {
			return Objects.hash(string);
		}
	}
}
