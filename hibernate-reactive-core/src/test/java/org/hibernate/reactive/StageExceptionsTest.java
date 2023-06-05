/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionException;

import org.hibernate.cfg.Configuration;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PersistenceException;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StageExceptionsTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( MyPerson.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( "native_exception_handling_51_compliance", "false" );
		return configuration;
	}

	@Test
	@Timeout(value = 10, timeUnit = MINUTES)
	public void testDuplicateKeyException(VertxTestContext context) {
		final Class<PersistenceException> expectedException = PersistenceException.class;

		test( context, openSession()
				.thenCompose( session -> session.persist( new MyPerson( "testFLush1", "unique" ) )
						.thenCompose( v -> session.flush() )
						.thenCompose( v -> session.persist( new MyPerson( "testFlush2", "unique" ) ) )
						.thenCompose( v -> session.flush() )
				)
				.handle( (res, err) -> {
					assertNotNull( err );
					assertTrue( err.getClass().isAssignableFrom( CompletionException.class ) );
					assertTrue(
							expectedException.isAssignableFrom( err.getCause().getClass() ),
							"Expected " + expectedException.getName() + " but was " + err
					);
					return null;
				} )
		);
	}

	@Entity(name = "MyPerson")
	public static class MyPerson {

		@Id
		public String name;

		@Column(unique = true)
		public String uniqueName;

		public MyPerson() {
		}

		public MyPerson(String name, String uniqueName) {
			this.name = name;
			this.uniqueName = uniqueName;
		}

		@Override
		public String toString() {
			return name + ", " + uniqueName;
		}
	}

	// TODO: [ORM-6] Check if this property still makes sense
	// I don't think we need to support this case but at the moment it would require more work to
	// disable the behaviour.
	public static class Native51ExceptionHandlingTest extends StageExceptionsTest {
		@Override
		protected Configuration constructConfiguration() {
			Configuration configuration = super.constructConfiguration();
			configuration.setProperty( "native_exception_handling_51_compliance", "true" );
			return configuration;
		}
	}
}
