/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.reactive.mutiny.Mutiny;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.SQLSERVER;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;

@Timeout(value = 10, timeUnit = MINUTES)

public class MutinyExceptionsTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Person.class );
	}

	@Test
	public void testDuplicateKeyException(VertxTestContext context) {
		test( context, assertThrown( ConstraintViolationException.class, openMutinySession()
				.call( session -> session.persist( new Person( "testFLush1", "unique" ) ) )
				.call( Mutiny.Session::flush )
				.call( session -> session.persist( new Person( "testFlush2", "unique" ) ) )
				.call( Mutiny.Session::flush ) )
				.invoke( MutinyExceptionsTest::assertSqlStateCode )
				.invoke( MutinyExceptionsTest::assertConstraintName )
		);
	}

	private static void assertSqlStateCode(ConstraintViolationException exception) {
		if ( dbType() == SQLSERVER ) {
			// The SQL state code is always null in Sql Server (see https://github.com/eclipse-vertx/vertx-sql-client/issues/1385)
			// We test the vendor code for now
			SQLException sqlException = (SQLException) exception.getCause();
			assertThat( sqlException.getErrorCode() ).isEqualTo( 2601 );
		}
		else {
			assertThat( exception.getSQLState() )
					.as( "Constraint violation SQL state code should start with 23" )
					.matches( "23\\d{3}" );
		}
	}

	private static void assertConstraintName(ConstraintViolationException exception) {
		// DB2 does not return the constraint name
		if ( dbType() != DB2 ) {
			assertThat( exception.getConstraintName() )
					.as( "Failed constraint name should not be null" )
					.isNotNull();
		}
	}

	@Entity(name = "Person")
	@Table(name = "PersonForExceptionWithMutiny")
	public static class Person {

		@Id
		@Column(name = "[name]")
		public String name;

		@Column(unique = true)
		public String uniqueName;

		public Person() {
		}

		public Person(String name, String uniqueName) {
			this.name = name;
			this.uniqueName = uniqueName;
		}

		@Override
		public String toString() {
			return name + ", " + uniqueName;
		}
	}
}
