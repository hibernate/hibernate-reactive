/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.types;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.annotations.DisabledFor;

import org.junit.jupiter.api.Test;

import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MARIA;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.SQLSERVER;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test types that we expect to work only on selected DBs.
 */
@Timeout(value = 10, timeUnit = MINUTES)

@DisabledFor(value = DB2, reason = "SQLException: The object 'HREACT.JSONENTITY' provided is not defined, SQLCODE=-204  SQLSTATE=42704")
@DisabledFor(value = SQLSERVER, reason = "Unsupported value class: class io.vertx.core.json.JsonObject from vertx.mssqlclient")
@DisabledFor(value = MARIA, reason = " org.hibernate.HibernateException: Expecting raw JDBC value of type `io.vertx.core.json.JsonObject`, but found `java.lang.String` : [{\"int\":123,\"str\":\"hello\"}]")
@DisabledFor(value = ORACLE, reason = "java.sql.SQLException: ORA-17004: Invalid column type: see https://docs.oracle.com/error-help/db/ora-17004/")
public class StringToJsonTypeTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Basic.class );
	}

	@Override
	public CompletionStage<Void> deleteEntities(Class<?>... types) {
		return getSessionFactory()
				.withTransaction( s -> loop( types, entityClass -> s
						.createQuery( "from JsonEntity", entityClass )
						.getResultList()
						.thenCompose( list -> loop( list, entity -> s.remove( entity ) ) ) ) );
	}

	@Test
	public void testJsonType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.json = new JsonObject().put( "int", 123 ).put( "str", "hello" ).encodePrettily();

		testField( context, basic, found -> assertEquals( basic.json, found.json ) );
	}

	@Test
	public void testNullJsonType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.json = null;

		testField( context, basic, found -> assertEquals( basic.json, found.json ) );
	}

	/**
	 * Persist the entity, find it and execute the assertions
	 */
	private void testField(VertxTestContext context, Basic original, Consumer<Basic> consumer) {
		test(
				context,
				getSessionFactory().withTransaction( (s, t) -> s.persist( original ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s2 -> s2.find( Basic.class, original.id )
								.thenAccept( found -> {
									assertNotNull( found );
									assertEquals( original, found );
									consumer.accept( found );
								} ) )
		);
	}

	@Entity(name = "JsonEntity")
	@Table(name = "JsonEntity")
	private static class Basic {

		@Id
		@GeneratedValue
		Integer id;
		@Version
		Integer version;
		String string;

		@Column(columnDefinition = "json")
		@Convert(converter = StringToJsonConverter.class)
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
			return Objects.equals( string, basic.string );
		}

		@Override
		public int hashCode() {
			return Objects.hash( string );
		}
	}
}
