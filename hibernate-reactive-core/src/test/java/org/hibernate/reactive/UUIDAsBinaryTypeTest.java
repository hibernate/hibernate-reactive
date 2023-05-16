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

import org.hibernate.reactive.testing.DBSelectionExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

import static java.util.Arrays.asList;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MARIA;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;
import static org.hibernate.reactive.testing.DBSelectionExtension.runOnlyFor;
import static org.hibernate.reactive.testing.DBSelectionExtension.skipTestsFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UUIDAsBinaryTypeTest extends BaseReactiveTest {

	//Db2: testUUIDType throws NoStackTraceThrowable: parameter of type BufferImpl cannot be coerced to ByteBuf
	@RegisterExtension
	public final DBSelectionExtension skip = DBSelectionExtension.skipTestsFor( DB2 );

	public static class ForMySQLandMariaDBTest extends UUIDAsBinaryTypeTest {
		// There's an issue querying for Binary types if the size of the column is different from
		// the size of the parameter: see https://github.com/hibernate/hibernate-reactive/issues/678
		// TODO: fix: Maria DB is currently failing in the vertx sql client with the following error
		//  error executing SQL statement [{errorMessage=Data too long for column 'id' at row 1, errorCode=1406, sqlState=22001}] [insert into ExactSizeUUIDEntity (name,id) values (?,?)]
		//  org.hibernate.exception.DataException: error executing SQL statement [{errorMessage=Data too long for column 'id' at row 1, errorCode=1406, sqlState=22001}] [insert into ExactSizeUUIDEntity (name,id) values (?,?)]
		@RegisterExtension
		public DBSelectionExtension dbSelection = runOnlyFor( MYSQL ); //, MARIA );

		@Override
		protected Collection<Class<?>> annotatedEntities() {
			return List.of( ForMySQLandMariaDBTest.ExactSizeUUIDEntity.class );
		}

		@Test
		public void exactSize(VertxTestContext context) {
			ForMySQLandMariaDBTest.ExactSizeUUIDEntity entityA = new ForMySQLandMariaDBTest.ExactSizeUUIDEntity( "Exact Size A" );
			ForMySQLandMariaDBTest.ExactSizeUUIDEntity entityB = new ForMySQLandMariaDBTest.ExactSizeUUIDEntity( "Exact Size B" );

			test( context, openMutinySession().chain( session -> session
							.persistAll( entityA, entityB )
							.call( session::flush ) )
					.invoke( () -> assertNotEquals( entityA.id, entityB.id ) )
					.chain( this::openMutinySession )
					.chain( session -> session.find( ForMySQLandMariaDBTest.ExactSizeUUIDEntity.class, entityA.id, entityB.id ) )
					.invoke( list -> {
						assertEquals( list.size(), 2 );
						assertTrue( list.containsAll( asList( entityA, entityB ) ) );
					} )
			);
		}

		@Entity(name = "ExactSizeUUIDEntity")
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
				ForMySQLandMariaDBTest.ExactSizeUUIDEntity that = (ForMySQLandMariaDBTest.ExactSizeUUIDEntity) o;
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

	public static class ForOtherDbsTest extends UUIDAsBinaryTypeTest {

		@RegisterExtension
		public DBSelectionExtension dbSelection = skipTestsFor( MYSQL, MARIA );

		@Override
		protected Collection<Class<?>> annotatedEntities() {
			return List.of( ForOtherDbsTest.UUIDEntity.class );
		}

		@Test
		public void uuidIdentifierWithMutinyAPI(VertxTestContext context) {
			ForOtherDbsTest.UUIDEntity entityA = new ForOtherDbsTest.UUIDEntity( "UUID A" );
			ForOtherDbsTest.UUIDEntity entityB = new ForOtherDbsTest.UUIDEntity( "UUID B" );

			test( context, openMutinySession().chain( session -> session
							.persistAll( entityA, entityB )
							.call( session::flush ) )
					.invoke( () -> assertNotEquals( entityA.id, entityB.id ) )
					.chain( this::openMutinySession )
					.chain( session -> session.find( ForOtherDbsTest.UUIDEntity.class, entityA.id, entityB.id ) )
					.invoke( list -> {
						assertEquals( list.size(), 2 );
						assertTrue( list.containsAll( asList( entityA, entityB ) ) );
					} )
			);
		}

		@Entity(name = "UUIDEntity")
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
				ForOtherDbsTest.UUIDEntity that = (ForOtherDbsTest.UUIDEntity) o;
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
