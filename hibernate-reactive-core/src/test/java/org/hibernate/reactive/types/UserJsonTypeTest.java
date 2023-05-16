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

import org.hibernate.annotations.Type;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.testing.DBSelectionExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.SQLSERVER;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test types that we expect to work only on selected DBs.
 */
public class UserJsonTypeTest extends BaseReactiveTest {

	@RegisterExtension
	public DBSelectionExtension selectionRule = DBSelectionExtension.skipTestsFor( DB2, SQLSERVER, ORACLE );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Basic.class );
	}

	@Override
	public CompletionStage<Void> deleteEntities(Class<?>... entities) {
		return getSessionFactory()
				.withTransaction( s -> loop( entities, entityClass -> s
						.createQuery( "from JsonEntity", entityClass )
						.getResultList()
						.thenCompose( list -> loop( list, entity -> s.remove( entity ) ) ) ) );
	}

	@Test
	public void testJsonType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.jsonObj = new JsonObject().put( "int", 123 ).put( "str", "hello" );

		testField( context, basic, found -> assertEquals( basic.jsonObj, found.jsonObj ) );
	}

	@Test
	public void testNullJsonType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.jsonObj = null;

		testField( context, basic, found -> assertEquals( basic.jsonObj, found.jsonObj ) );
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
								} ) ) );
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

		@Type(Json.class)
		@Column(columnDefinition = "json")
		private JsonObject jsonObj;

		public Basic() {
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
