/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.types;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Version;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;

/**
 * Test types that we expect to work only on selected DBs.
 */
public class BasicTypesForSelectedDBTest extends BaseReactiveTest {

	@Rule
	public DatabaseSelectionRule selectionRule = DatabaseSelectionRule.skipTestsFor( DB2 );

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Basic.class );
		return configuration;
	}

	@After
	public void deleteTable(TestContext context) {
		test( context,
			  getSessionFactory().withSession(
					  session -> session.createQuery( "delete from Basic" ).executeUpdate()
			  )
		);
	}

	@Test
	public void testStringLobType(TestContext context) {
		String text = "hello world once upon a time it was the best of times it was the worst of times goodbye";
		StringBuilder longText = new StringBuilder();
		for ( int i = 0; i < 1000; i++ ) {
			longText.append( text );
		}
		String book = longText.toString();

		Basic basic = new Basic();
		basic.book = book;

		testField( context, basic, found -> context.assertEquals( book, found.book ) );
	}

	@Test
	public void testBytesLobType(TestContext context) {
		String text = "hello world once upon a time it was the best of times it was the worst of times goodbye";
		StringBuilder longText = new StringBuilder();
		for ( int i = 0; i < 1000; i++ ) {
			longText.append( text );
		}
		byte[] pic = longText.toString().getBytes();

		Basic basic = new Basic();
		basic.pic = pic;
		testField( context, basic, found -> context.assertTrue( Objects.deepEquals( pic, found.pic ) ) );
	}

	@Test
	public void testUUIDType(TestContext context) throws Exception {
		Basic basic = new Basic();
		basic.uuid = UUID.fromString( "123e4567-e89b-42d3-a456-556642440000" );

		testField( context, basic, found -> context.assertEquals( basic.uuid, found.uuid ) );
	}

	@Test
	public void testDecimalType(TestContext context) throws Exception {
		Basic basic = new Basic();
		basic.bigDecimal = new BigDecimal( 12.12d );

		testField( context, basic, found -> context.assertEquals( basic.bigDecimal.floatValue(), found.bigDecimal.floatValue() ) );
	}

	@Test
	public void testBigIntegerType(TestContext context) throws Exception {
		Basic basic = new Basic();
		basic.bigInteger = BigInteger.valueOf( 123L);

		testField( context, basic, found -> context.assertEquals( basic.bigInteger, found.bigInteger ) );
	}

	@Test
	public void testLocalTimeType(TestContext context) throws Exception {
		Basic basic = new Basic();
		basic.localTime = LocalTime.now();

		testField( context, basic, found -> context.assertEquals(
				basic.localTime.truncatedTo( ChronoUnit.MINUTES ),
				found.localTime.truncatedTo( ChronoUnit.MINUTES )
		) );
	}

	@Test
	public void testDateAsTimeType(TestContext context) throws Exception {
		Date date = new Date();

		Basic basic = new Basic();
		basic.dateAsTime = date;

		testField( context, basic, found -> {
			SimpleDateFormat timeSdf = new SimpleDateFormat( "HH:mm:ss" );
			context.assertTrue( found.dateAsTime instanceof Time );
			context.assertEquals( timeSdf.format( date ), timeSdf.format( found.dateAsTime ) );
		} );
	}

	/**
	 * Persist the entity, find it and execute the assertions
	 */
	private void testField(TestContext context, Basic original, Consumer<Basic> consumer) {
		test(
				context,
				getSessionFactory().withTransaction( (s, t) -> s.persist( original ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s2 -> s2.find( Basic.class,  original.id )
								.thenAccept( found -> {
									context.assertNotNull( found );
									context.assertEquals( original, found );
									consumer.accept( found );
								} ) )
		);
	}

	@Entity(name="Basic") @Table(name="Basic")
	private static class Basic {

		@Id @GeneratedValue Integer id;
		@Version Integer version;
		String string;

		UUID uuid;

		@Column(name="dessimal")
		BigDecimal bigDecimal;
		@Column(name="inteja")
		BigInteger bigInteger;

		@Column(name="localtyme")
		private LocalTime localTime;
		@Temporal(TemporalType.TIME)
		Date dateAsTime;

		@Lob @Column(length = 100_000) protected byte[] pic;
		@Lob @Column(length = 100_000) protected String book;

		public Basic() {
		}

		public Basic(String string) {
			this.string = string;
		}

		public Basic(String string, byte[] pic, String book) {
			this.string = string;
			this.pic = pic;
			this.book = book;
		}

		public Basic(Integer id, String string) {
			this.id = id;
			this.string = string;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
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
