/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.hibernate.HibernateException;
import org.hibernate.reactive.mutiny.Mutiny;

import org.junit.Test;

import io.vertx.ext.unit.TestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

public class MutinyExceptionsTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Person.class );
	}

	Class<?> getExpectedException() {
		return HibernateException.class;
	}

	@Test
	public void testDuplicateKeyException(TestContext context) {
		test( context, openMutinySession()
				.onItem().call( session -> session.persist( new Person( "testFLush1", "unique" ) ) )
				.onItem().call( Mutiny.Session::flush )
				.onItem().call( session -> session.persist( new Person( "testFlush2", "unique" ) ) )
				.onItem().call( Mutiny.Session::flush )
				.onItem().invoke( ignore -> context.fail( "Expected exception not thrown" ) )
				.onFailure().recoverWithItem( err -> {
					context.assertEquals( getExpectedException(), err.getClass() );
					return null;
				} )
		);
	}

	@Entity(name = "Person")
	public static class Person {

		@Id
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
