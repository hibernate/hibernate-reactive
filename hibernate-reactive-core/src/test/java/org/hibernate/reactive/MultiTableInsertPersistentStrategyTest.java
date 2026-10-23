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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

import org.hibernate.annotations.IdGeneratorType;
import org.hibernate.boot.model.relational.Database;
import org.hibernate.generator.EventType;
import org.hibernate.id.BulkInsertionCapableIdentifierGenerator;
import org.hibernate.id.OptimizableGenerator;
import org.hibernate.id.enhanced.NoopOptimizer;
import org.hibernate.id.enhanced.Optimizer;
import org.hibernate.reactive.annotations.EnabledFor;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Inheritance;
import jakarta.persistence.InheritanceType;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.COCKROACHDB;
import static org.hibernate.reactive.util.internal.CompletionStages.completedFuture;

/**
 * An HQL insert-select on a joined-inheritance entity whose id generator is not
 * bulk-insertion capable requires the ids to be generated one row at a time.
 * The row numbers are loaded from the id table with
 * {@code ReactiveExecuteWithTemporaryTableHelper#loadInsertedRowNumbers}, which
 * must bind the session uid when the id table is a persistent table
 * (for example on CockroachDB).
 */
@Timeout(value = 10, timeUnit = MINUTES)
@EnabledFor(value = COCKROACHDB, reason = "It uses the persistent id table strategy (with a session uid column) by default")
public class MultiTableInsertPersistentStrategyTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Animal.class, Dog.class );
	}

	@Test
	public void testInsertSelect(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persistAll( new Dog( "Rex", 3 ), new Dog( "Fido", 5 ) ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.createMutationQuery( "insert into Dog (name, age) select d.name || '2', d.age from Dog d" )
						.executeUpdate() ) )
				.invoke( count -> assertThat( count ).isEqualTo( 2 ) )
				.chain( () -> getMutinySessionFactory().withSession( s -> s
						.createSelectionQuery( "select d.name from Dog d order by d.name", String.class )
						.getResultList() ) )
				.invoke( names -> assertThat( names ).containsExactly( "Fido", "Fido2", "Rex", "Rex2" ) )
		);
	}

	@Entity(name = "Animal")
	@Inheritance(strategy = InheritanceType.JOINED)
	public static class Animal {
		@Id
		@AnimalId
		Long id;
		String name;

		public Animal() {
		}

		public Animal(String name) {
			this.name = name;
		}
	}

	@Entity(name = "Dog")
	public static class Dog extends Animal {
		int age;

		public Dog() {
		}

		public Dog(String name, int age) {
			super( name );
			this.age = age;
		}
	}

	@Target(ElementType.FIELD)
	@Retention(RetentionPolicy.RUNTIME)
	@IdGeneratorType(NotBulkCapableGenerator.class)
	public @interface AnimalId {
	}

	/**
	 * A generator that forces Hibernate to generate the ids one row at a time
	 * during a multi-table insert.
	 */
	public static class NotBulkCapableGenerator
			implements ReactiveIdentifierGenerator<Long>, OptimizableGenerator, BulkInsertionCapableIdentifierGenerator {

		private final AtomicLong sequence = new AtomicLong( 1000 );

		@Override
		public boolean generatedOnExecution() {
			return false;
		}

		@Override
		public EnumSet<EventType> getEventTypes() {
			return EnumSet.of( EventType.INSERT );
		}

		@Override
		public void registerExportables(Database database) {
		}

		@Override
		public Optimizer getOptimizer() {
			return new NoopOptimizer( Long.class, 1 );
		}

		@Override
		public boolean supportsBulkInsertionIdentifierGeneration() {
			return false;
		}

		@Override
		public CompletionStage<Long> generate(ReactiveConnectionSupplier session, Object entity) {
			return completedFuture( sequence.getAndIncrement() );
		}
	}
}
