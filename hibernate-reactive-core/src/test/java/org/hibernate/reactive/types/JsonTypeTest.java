/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.types;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.annotations.DisabledFor;
import org.hibernate.reactive.annotations.EnabledFor;
import org.hibernate.type.SqlTypes;

import org.junit.jupiter.api.Test;

import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MARIA;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.SQLSERVER;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test types that we expect to work only on selected DBs.
 */
@Timeout(value = 10, timeUnit = MINUTES)
@DisabledFor(value = DB2, reason = "java.sql.SQLException: The object 'HREACT.JSONENTITY' provided is not defined, SQLCODE=-204  SQLSTATE=42704")
@DisabledFor(value = SQLSERVER, reason = "java.lang.IllegalArgumentException: Unsupported value class: class io.vertx.core.json.JsonObject")
@DisabledFor(value = ORACLE, reason = "java.sql.SQLException: ORA-17004: Invalid column type: https://docs.oracle.com/error-help/db/ora-17004/")
@DisabledFor(value = MARIA, reason = "ORM 6 regression, see: https://github.com/hibernate/hibernate-reactive/issues/1529")
public class JsonTypeTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Basic.class );
	}

	@Test
	@EnabledFor(POSTGRESQL)
	public void nativeQuestionMarkOperatorForPostgres(VertxTestContext context) {
		Basic basic = new Basic();
		basic.jsonAsMap = Map.of( "sport", "Cheese Rolling" );

		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persist( basic ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.createNativeQuery( "select id from JsonEntity where jsonAsMap -> 'sport' \\? 'Cheese Rolling'" )
						.getSingleResult() )
				)
				.invoke( result -> assertThat( result ).isEqualTo( basic.id ) )
		);
	}

	@Test
	@EnabledFor(POSTGRESQL)
	public void nativeQuestionMarkOperatorWithParameterForPostgres(VertxTestContext context) {
		Basic basic = new Basic();
		basic.jsonAsMap = Map.of( "sport", "Chess boxing" );

		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persist( basic ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.createNativeQuery( "select id from JsonEntity where jsonAsMap -> 'sport' \\? ?" )
						.setParameter( 1, "Chess boxing" )
						.getSingleResult() )
				)
				.invoke( result -> assertThat( result ).isEqualTo( basic.id ) )
		);
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
		test( context, getSessionFactory()
				.withTransaction( s -> s.persist( original ) )
				.thenCompose( v -> getSessionFactory().withTransaction( s -> s
						.find( Basic.class, original.id ) )
				)
				.thenAccept( found -> {
					assertThat( found ).isEqualTo( original );
					consumer.accept( found );
				} )
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

		private JsonObject jsonObj;

		@JdbcTypeCode(SqlTypes.JSON)
		Map<String, Object> jsonAsMap;

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
