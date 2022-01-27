/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.types;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.testing.DatabaseSelectionRule;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Version;
import java.util.Objects;
import java.util.function.Consumer;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MARIA;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.SQLSERVER;

/**
 * Test types that we expect to work only on selected DBs.
 */
public class StringToJsonTypeTest extends BaseReactiveTest {

	@Rule
	public DatabaseSelectionRule selectionRule = DatabaseSelectionRule.skipTestsFor( DB2, SQLSERVER, MARIA, ORACLE );

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
					  session -> session.createQuery( "delete from JsonEntity" ).executeUpdate()
			  )
		);
	}

	@Test
	public void testJsonType(TestContext context) {
		Basic basic = new Basic();
		basic.json = new JsonObject().put("int", 123).put("str", "hello").encodePrettily();

		testField( context, basic, found -> context.assertEquals( basic.json, found.json) );
	}

	@Test
	public void testNullJsonType(TestContext context) {
		Basic basic = new Basic();
		basic.json = null;

		testField( context, basic, found -> context.assertEquals(basic.json, found.json) );
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

	@Entity(name="JsonEntity")
	@Table(name="JsonEntity")
	private static class Basic {

		@Id @GeneratedValue Integer id;
		@Version Integer version;
		String string;

		@Column(columnDefinition = "json")
		@Convert(converter = StringToJson.class)
		private String json;

		public Basic() {
		}

		public Basic(String string) {
			this.string = string;
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
