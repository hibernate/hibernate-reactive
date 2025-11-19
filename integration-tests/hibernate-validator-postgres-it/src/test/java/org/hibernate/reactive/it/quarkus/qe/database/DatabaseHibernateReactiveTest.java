/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.it.quarkus.qe.database;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;



import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.validation.ConstraintViolationException;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
public class DatabaseHibernateReactiveTest extends BaseReactiveIT {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class, Author.class );
	}

	/**
	 * Assert that the expected exception type is thrown.
	 *
	 * @return a {@link Uni} with the expected exception thrown by the uni we were testing.
	 */
	public static <U extends Throwable> Uni<U> assertThrown(Class<U> expectedException, Uni<?> uni) {
		return uni.onItemOrFailure().transform( (s, e) -> {
			assertThat( e ).isInstanceOf( expectedException );
			return (U) e;
		} );
	}

	@Test
	public void nameIsTooLong(VertxTestContext context) {
		test( context, assertThrown( ConstraintViolationException.class, getMutinySessionFactory()
				.withTransaction( s -> {
					Author author = new Author();
					author.setName( "A very long long long long name ... " );
					return s.persist( author );
				} ) )
				.invoke( he -> assertThat( he )
						.hasMessageContaining( "size must be between" )
				)
		);
	}

	@Test
	public void nameIsNull(VertxTestContext context) {
		test( context, assertThrown( ConstraintViolationException.class, getMutinySessionFactory()
				.withTransaction( s -> {
					Author author = new Author();
					author.setName( null );
					return s.persist( author );
				} ) )
				.invoke( he -> assertThat( he )
						.hasMessageContaining( "must not be null" )
				)
		);
	}
}
