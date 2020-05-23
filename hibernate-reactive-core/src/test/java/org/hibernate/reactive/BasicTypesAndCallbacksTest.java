package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.containers.DatabaseConfiguration.DBType;
import org.junit.Test;

import javax.persistence.*;

import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class BasicTypesAndCallbacksTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Basic.class );
		return configuration;
	}

	@Test
	public void testBasicTypes(TestContext context) {
		// TODO @AGG
		// The DB2 driver does not yet support a few types (BigDecimal, BigInteger, LocalTime)
		// so we need to keep a separate copy around for testing DB2 (DB2BasicTest)
		assumeTrue( DatabaseConfiguration.dbType() != DBType.DB2 );

		Basic basik = new Basic("Hello World");
		basik.decimal = new BigDecimal(12.12d);
		basik.integer = BigInteger.valueOf(123L);
		basik.bytes =  "hello world".getBytes();
		basik.thing = Locale.getDefault();
		basik.timeZone = TimeZone.getDefault();
		basik.cover = Cover.hard;
		basik.binInteger = BigInteger.valueOf(12345L);
		basik.parent = new Basic("Parent");
		basik.localDate = LocalDate.now();
		basik.localDateTime = LocalDateTime.now();
		basik.localTime = LocalTime.now();
		basik.date = new Date(2000,Calendar.JANUARY,1);
		basik.thing = new String[] {"hello", "world"};
		basik.embed = new Embed("one", "two");

		test( context,
				openSession()
				.thenCompose(s -> s.persist(basik.parent))
				.thenCompose(s -> s.persist(basik))
				.thenApply(s -> { context.assertTrue(basik.prePersisted && !basik.postPersisted); return s; } )
				.thenApply(s -> { context.assertTrue(basik.parent.prePersisted && !basik.parent.postPersisted); return s; } )
				.thenCompose(s -> s.flush())
				.thenApply(s -> { context.assertTrue(basik.prePersisted && basik.postPersisted); return s; } )
				.thenApply(s -> { context.assertTrue(basik.parent.prePersisted && basik.parent.postPersisted); return s; } )
				.thenCompose(v -> openSession())
				.thenCompose(s2 ->
					s2.find( Basic.class, basik.getId() )
						.thenCompose( basic -> {
							context.assertNotNull( basic );
							context.assertTrue( basic.loaded );
							context.assertEquals( basic.string, basik.string);
							context.assertEquals( basic.decimal.floatValue(), basik.decimal.floatValue());
							context.assertEquals( basic.integer, basik.integer);
							context.assertTrue( Arrays.equals(basic.bytes, basik.bytes) );
							context.assertEquals( basic.timeZone, basik.timeZone);
							context.assertEquals( basic.cover, basik.cover);
							context.assertEquals( basic.binInteger.floatValue(), basik.binInteger.floatValue());
							context.assertEquals( basic.date, basik.date );
							context.assertEquals( basic.localDate, basik.localDate );
							context.assertEquals( basic.localDateTime, basik.localDateTime );
							context.assertEquals( basic.localTime.truncatedTo(ChronoUnit.MINUTES),
									basik.localTime.truncatedTo(ChronoUnit.MINUTES) );
							context.assertTrue( basic.thing instanceof String[] );
							context.assertTrue( Objects.deepEquals(basic.thing, basik.thing) );
							context.assertEquals( basic.embed, basik.embed );
							context.assertEquals( basic.version, 0 );

							basic.string = "Goodbye";
							basic.cover = Cover.soft;
							basic.parent = new Basic("New Parent");
							return s2.persist(basic.parent)
									.thenCompose( v -> s2.flush() )
									.thenAccept(v -> {
										context.assertNotNull( basic );
										context.assertTrue( basic.postUpdated && basic.preUpdated );
										context.assertFalse( basic.postPersisted && basic.prePersisted );
										context.assertTrue( basic.parent.postPersisted && basic.parent.prePersisted );
										context.assertEquals( basic.version, 1 );
									});
						}))
				.thenCompose(v -> openSession())
				.thenCompose(s3 ->
					s3.find( Basic.class, basik.getId() )
						.thenCompose( basic -> {
							context.assertFalse( basic.postUpdated && basic.preUpdated );
							context.assertFalse( basic.postPersisted && basic.prePersisted );
							context.assertEquals( basic.version, 1 );
							context.assertEquals( basic.string, "Goodbye");
							return connection()
									.thenCompose( connection -> connection.execute("update Basic set string = 'Goodnight'") )
									.thenCompose(v -> s3.refresh(basic))
									.thenAccept(v -> context.assertEquals(basic.getString(), "Goodnight"))
									.thenCompose(v -> {
										basik.version = basic.version;
										return s3.merge(basik);
									})
									.thenAccept( b -> context.assertEquals( b.string, "Hello World") )
									.thenCompose(v -> s3.remove(basic))
									.thenAccept(v -> context.assertTrue( !basic.postRemoved && basic.preRemoved ) )
									.thenCompose(v -> s3.flush())
									.thenAccept(v -> context.assertTrue( basic.postRemoved && basic.preRemoved ) );
						}))
				.thenCompose(v -> openSession())
				.thenCompose(s4 ->
						s4.find( Basic.class, basik.getId() )
							.thenAccept(context::assertNull))
		);
	}

	enum Cover { hard, soft }

	@Embeddable
	static class Embed {
		String one;
		String two;

		public Embed(String one, String two) {
			this.one = one;
			this.two = two;
		}

		Embed() {}

		public String getOne() {
			return one;
		}

		public void setOne(String one) {
			this.one = one;
		}

		public String getTwo() {
			return two;
		}

		public void setTwo(String two) {
			this.two = two;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Embed embed = (Embed) o;
			return Objects.equals(one, embed.one) &&
					Objects.equals(two, embed.two);
		}

		@Override
		public int hashCode() {
			return Objects.hash(one, two);
		}
	}

	@Entity @Table(name="Basic")
	public static class Basic {

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

		@Column(name="dessimal")
		BigDecimal decimal;
		@Column(name="inteja")
		BigInteger integer;
		byte[] bytes;
		Cover cover;
		@javax.persistence.Basic
		Serializable thing;
		TimeZone timeZone;
		@Temporal(TemporalType.DATE)
		private Date date;
		@Column(name="localdayte")
		private LocalDate localDate;
		@Column(name="localdaytetyme")
		private LocalDateTime localDateTime;
		@Column(name="localtyme")
		private LocalTime localTime;
		@Convert(converter = BinInteger.class)
		private BigInteger binInteger;

		@ManyToOne(fetch = FetchType.LAZY)
		Basic parent;

		Embed embed;

		@Transient boolean prePersisted;
		@Transient boolean postPersisted;
		@Transient boolean preUpdated;
		@Transient boolean postUpdated;
		@Transient boolean postRemoved;
		@Transient boolean preRemoved;
		@Transient boolean loaded;

		public Basic(String string) {
			this.string = string;
		}

		public Basic(Integer id, String string) {
			this.id = id;
			this.string = string;
		}

		Basic() {}

		@PrePersist
		void prePersist() {
			prePersisted = true;
		}

		@PostPersist
		void postPersist() {
			postPersisted = true;
		}

		@PreUpdate
		void preUpdate() {
			preUpdated = true;
		}

		@PostUpdate
		void postUpdate() {
			postUpdated = true;
		}

		@PreRemove
		void preRemove() {
			preRemoved = true;
		}

		@PostRemove
		void postRemove() {
			postRemoved = true;
		}

		@PostLoad
		void postLoad() {
			loaded = true;
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
			Basic basic = (Basic) o;
			return Objects.equals(string, basic.string);
		}

		@Override
		public int hashCode() {
			return Objects.hash(string);
		}
	}
}
