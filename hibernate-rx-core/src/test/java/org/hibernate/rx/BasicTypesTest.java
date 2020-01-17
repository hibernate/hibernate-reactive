package org.hibernate.rx;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.hibernate.cfg.Configuration;
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

@RunWith(VertxUnitRunner.class)
public class BasicTypesTest extends BaseRxTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( BasicTypes.class );
		return configuration;
	}

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

		test( context,
				openSession()
				.thenCompose(s -> s.persist(b.basicTypes))
				.thenCompose(s -> s.persist(b))
				.thenCompose(s -> s.flush())
				.thenCompose(v -> openSession())
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
				.thenCompose(v -> openSession())
				.thenCompose( s3 -> s3.find( BasicTypes.class, b.getId() ) )
				.thenAccept( bt -> {
					BasicTypes bb = bt.get();
					context.assertEquals(bb.version, 1);
					context.assertEquals( bb.string, "Goodbye");
				})
		);
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
