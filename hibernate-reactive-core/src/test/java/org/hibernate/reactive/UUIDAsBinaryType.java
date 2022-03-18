/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static java.util.Arrays.asList;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MARIA;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.testing.DatabaseSelectionRule.runOnlyFor;
import static org.hibernate.reactive.testing.DatabaseSelectionRule.skipTestsFor;

public abstract class UUIDAsBinaryType extends BaseReactiveTest {

	public static class ForMySQLandMariaDBTest extends UUIDAsBinaryType {
		// There's an issue querying for Binary types if the size of the column is different from
		// the size of the parameter: see https://github.com/hibernate/hibernate-reactive/issues/678
		@Rule
		public DatabaseSelectionRule rule = runOnlyFor( MYSQL, MARIA );

		@Override
		protected Collection<Class<?>> annotatedEntities() {
			return List.of( ExactSizeUUIDEntity.class );
		}

		@Test
		public void exactSize(TestContext context) {
			ExactSizeUUIDEntity entityA = new ExactSizeUUIDEntity( "Exact Size A" );
			ExactSizeUUIDEntity entityB = new ExactSizeUUIDEntity( "Exact Size B" );

			test( context, openMutinySession().chain( session -> session
					.persistAll( entityA, entityB )
					.call( session::flush ) )
					.invoke( () -> context.assertNotEquals( entityA.id, entityB.id ) )
					.chain( this::openMutinySession )
					.chain( session -> session.find( ExactSizeUUIDEntity.class, entityA.id, entityB.id ) )
					.invoke( list -> {
						context.assertEquals( list.size(), 2 );
						context.assertTrue( list.containsAll( asList( entityA, entityB ) ) );
					} )
			);
		}

		@Entity
		private static class ExactSizeUUIDEntity {

			@Id
			@GeneratedValue
			@Column(columnDefinition = "binary(16)")
			UUID id;

			@Column(unique = true)
			String name;

			public ExactSizeUUIDEntity() {
			}

			public ExactSizeUUIDEntity(String name) {
				this.name = name;
			}

			@Override
			public boolean equals(Object o) {
				if ( this == o ) {
					return true;
				}
				if ( o == null || getClass() != o.getClass() ) {
					return false;
				}
				ExactSizeUUIDEntity that = (ExactSizeUUIDEntity) o;
				return Objects.equals( name, that.name );
			}

			@Override
			public int hashCode() {
				return Objects.hash( name );
			}

			@Override
			public String toString() {
				return id + ":" + name;
			}
		}

	}

	public static class ForOtherDbsTest extends UUIDAsBinaryType {

		@Rule // Select a UUID field doesn't work with Oracle
		public DatabaseSelectionRule rule = skipTestsFor( MYSQL, MARIA, ORACLE );

		@Override
		protected Collection<Class<?>> annotatedEntities() {
			return List.of( UUIDEntity.class );
		}

		@Test
		public void uuidIdentifierWithMutinyAPI(TestContext context) {
			UUIDEntity entityA = new UUIDEntity( "UUID A" );
			UUIDEntity entityB = new UUIDEntity( "UUID B" );

			test( context, openMutinySession().chain( session -> session
					.persistAll( entityA, entityB )
					.call( session::flush ) )
					.invoke( () -> context.assertNotEquals( entityA.id, entityB.id ) )
					.chain( this::openMutinySession )
					.chain( session -> session.find( UUIDEntity.class, entityA.id, entityB.id ) )
					.invoke( list -> {
						context.assertEquals( list.size(), 2 );
						context.assertTrue( list.containsAll( asList( entityA, entityB ) ) );
					} )
			);
		}

		@Entity
		private static class UUIDEntity {

			@Id
			@GeneratedValue
			UUID id;

			@Column(unique = true)
			String name;

			public UUIDEntity() {
			}

			public UUIDEntity(String name) {
				this.name = name;
			}

			@Override
			public boolean equals(Object o) {
				if ( this == o ) {
					return true;
				}
				if ( o == null || getClass() != o.getClass() ) {
					return false;
				}
				UUIDEntity that = (UUIDEntity) o;
				return Objects.equals( name, that.name );
			}

			@Override
			public int hashCode() {
				return Objects.hash( name );
			}

			@Override
			public String toString() {
				return id + ":" + name;
			}
		}
	}
}
