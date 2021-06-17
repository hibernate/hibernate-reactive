/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.PersistenceException;

import org.hibernate.HibernateException;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.mutiny.Mutiny;

import org.junit.Test;

import io.vertx.ext.unit.TestContext;

public class MutinyExceptionsTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( AvailableSettings.NATIVE_EXCEPTION_HANDLING_51_COMPLIANCE, "false" );
		configuration.addAnnotatedClass( Person.class );
		return configuration;
	}

	Class<?> getExpectedException() {
		return PersistenceException.class;
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

	@Entity
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

	// I don't think we need to support this case but at the moment it would require more work to
	// disable the behaviour.
	public static class Native51ExceptionHandlingTest extends MutinyExceptionsTest {

		@Override
		Class<?> getExpectedException() {
			return HibernateException.class;
		}

		@Override
		protected Configuration constructConfiguration() {
			Configuration configuration = super.constructConfiguration();
			configuration.setProperty( AvailableSettings.NATIVE_EXCEPTION_HANDLING_51_COMPLIANCE, "true" );
			return configuration;
		}
	}
}
