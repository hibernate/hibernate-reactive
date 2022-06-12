/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.types;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;

/**
 * Test types that we expect to work only on selected DBs.
 */
public class LobTypeTest extends BaseReactiveTest {

	@Rule
	public DatabaseSelectionRule selectionRule = DatabaseSelectionRule.skipTestsFor( DB2 );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Basic.class );
	}

	@Test
	public void testStringLobType(TestContext context) {
		String text = "hello world once upon a time it was the best of times it was the worst of times goodbye";
		StringBuilder longText = new StringBuilder();
		longText.append( text.repeat( 1000 ) );
		String book = longText.toString();

		Basic basic = new Basic();
		basic.book = book;

		testField( context, basic, found -> context.assertEquals( book, found.book ) );
	}

	@Test
	public void testBytesLobType(TestContext context) {
		String text = "hello world once upon a time it was the best of times it was the worst of times goodbye";
		StringBuilder longText = new StringBuilder();
		longText.append( text.repeat( 1000 ) );
		byte[] pic = longText.toString().getBytes();

		Basic basic = new Basic();
		basic.pic = pic;
		testField( context, basic, found -> context.assertTrue( Objects.deepEquals( pic, found.pic ) ) );
	}

	/**
	 * Persist the entity, find it and execute the assertions
	 */
	private void testField(TestContext context, Basic original, Consumer<Basic> consumer) {
		test( context, getSessionFactory()
				.withTransaction( s -> s.persist( original ) )
				.thenCompose( v -> openSession() )
				.thenCompose( s2 -> s2.find( Basic.class, original.id )
						.thenAccept( found -> {
							context.assertEquals( original, found );
							consumer.accept( found );
						} ) )
		);
	}

	@Entity(name="LobEntity")
	@Table(name="LobEntity")
	private static class Basic {

		@Id @GeneratedValue Integer id;
		@Version Integer version;
		String string;

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
