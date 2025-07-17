/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.hibernate.annotations.IdGeneratorType;
import org.hibernate.generator.EventType;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.util.impl.CompletionStages;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)
public class BeforeExecutionIdGeneratorTypeTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Person.class );
	}

	@Test
	public void testPersist(VertxTestContext context) {
		Person person = new Person();
		test(
				context, openSession()
						.thenCompose( session -> session.persist( person ) )
						.thenAccept( v -> {
							assertThat( person.getId() ).isNotNull();
						} )
		);
	}

	@Entity(name = "Person")
	public static class Person {
		@Id
		@SimpleId
		long id;

		String name;

		public Person() {
		}

		public Person(String name) {
			this.name = name;
		}

		public long getId() {
			return id;
		}

		public String getName() {
			return name;
		}
	}

	@Target(ElementType.FIELD)
	@Retention(RetentionPolicy.RUNTIME)
	@IdGeneratorType(SimpleGenerator.class)
	public @interface SimpleId {
	}

	public static class SimpleGenerator implements ReactiveIdentifierGenerator<Long> {

		private AtomicLong sequence = new AtomicLong( 1 );

		public SimpleGenerator() {
		}

		@Override
		public boolean generatedOnExecution() {
			return false;
		}

		@Override
		public EnumSet<EventType> getEventTypes() {
			return EnumSet.of( EventType.INSERT );
		}


		@Override
		public CompletionStage<Long> generate(ReactiveConnectionSupplier session, Object entity) {
			return CompletionStages.completedFuture( sequence.getAndIncrement() );
		}
	}

}
