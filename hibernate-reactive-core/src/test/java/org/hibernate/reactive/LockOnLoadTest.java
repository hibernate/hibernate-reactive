/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.hibernate.LockMode;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)
public class LockOnLoadTest extends BaseReactiveTest{
	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Person.class );
	}

	@Test
	public void testLockOnLoad(VertxTestContext context) {
		Person person = new Person( 1L, "Davide" );

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persist( person ) )
				.call( () -> getMutinySessionFactory().withSession( session -> session
						.find( Person.class, person.getId() )
						// the issue occurred when trying to find the same entity but upgrading the lock mode
						.chain( p -> session.find( Person.class, person.getId(), LockMode.PESSIMISTIC_WRITE ) )
						.invoke( p -> assertThat( p ).isNotNull() )
				) )
		);
	}

	@Entity(name = "Person")
	@Table(name = "LockOnLoadTest.Person")
	public static class Person {
		@Id
		private Long id;

		private String name;

		public Person() {
		}

		public Person(Long id, String name) {
			this.id = id;
			this.name = name;
		}

		public Long getId() {
			return id;
		}

		public String getName() {
			return name;
		}
	}
}
