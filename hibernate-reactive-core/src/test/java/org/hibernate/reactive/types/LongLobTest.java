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

import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.testing.DBSelectionExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.junit5.VertxTestContext;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class LongLobTest extends BaseReactiveTest {

	@RegisterExtension
	public DBSelectionExtension selectionRule = DBSelectionExtension.runOnlyFor( MYSQL );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Basic.class );
	}

	// Test is failing with PG when the session factory set-up uses
	// PostgeSQLDatabase
	// 	public String getJdbcUrl() {
	//		return buildJdbcUrlWithCredentials( address() );
	//	}

	@Test
	public void testLongLobType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.longLob = Long.MAX_VALUE;

		testField( context, basic, found -> assertEquals( Long.MAX_VALUE, found.longLob ) );
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

	@Entity(name = "Basic")
	@Table(name = "Basic")
	private static class Basic {

		@Id
		@GeneratedValue
		Integer id;

		String string;

		@Lob
		@Column(length = 100_000)
		private Long longLob;

		public Basic() {
		}

		public Basic(String string) {
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
