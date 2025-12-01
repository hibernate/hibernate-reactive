/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

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
import jakarta.persistence.Table;
import jakarta.persistence.Tuple;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)
public class BeforeExecutionIdGeneratorTypeTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Person.class );
	}

	@Test
	public void testPersistWithoutTransaction(VertxTestContext context) {
		final Person person = new Person( "Janet" );
		// The id should be set by the persist
		assertThat( person.getId() ).isNull();
		test( context, getMutinySessionFactory()
				// The value won't be persisted on the database, but the id should have been assigned anyway
				.withSession( session -> session.persist( person ) )
				.invoke( () -> assertThat( person.getId() ).isGreaterThan( 0 ) )
			  	// Check that the value has not been saved
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.createNativeQuery( "select * from Person_Table", Tuple.class ).getSingleResultOrNull() )
				)
				.invoke( result -> assertThat( result ).isNull() )
		);
	}

	@Test
	public void testPersistWithTransaction(VertxTestContext context) {
		final Person person = new Person( "Baldrick" );
		// The id should be set by the persist
		assertThat( person.getId() ).isNull();
		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persist( person ) )
				.invoke( () -> assertThat( person.getId() ).isGreaterThan( 0 ) )
				// Check that the value has been saved
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.createQuery( "from Person", Person.class ).getSingleResult() )
				)
				.invoke( p -> {
					// The raw type might not be a Long, so we have to cast it
					assertThat( p.id ).isEqualTo( person.id );
					assertThat( p.name ).isEqualTo( person.name );
				} )

		);
	}

	@Entity(name = "Person")
	@Table(name = "Person_Table")
	public static class Person {
		@Id
		@SimpleId
		Long id;

		String name;

		public Person() {
		}

		public Person(String name) {
			this.name = name;
		}

		public Long getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		@Override
		public boolean equals(Object o) {
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Person person = (Person) o;
			return Objects.equals( name, person.name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
		}

		@Override
		public String toString() {
			return id + ":" + name;
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
