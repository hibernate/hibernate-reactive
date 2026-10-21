/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Inheritance;
import jakarta.persistence.InheritanceType;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)
public class UpsertWithInheritanceTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of(
				JoinedAnimal.class, JoinedDog.class,
				UnionAnimal.class, UnionDog.class,
				SingleTableAnimal.class, SingleTableDog.class
		);
	}

	@Test
	public void testMutinyUpsertWithJoinedInheritance(VertxTestContext context) {
		JoinedDog dog = new JoinedDog( 1L, "Rex", "Labrador" );
		JoinedDog changedDog = new JoinedDog( 1L, "Max", "Labrador" );
		test( context, getMutinySessionFactory().withStatelessTransaction( ss -> ss.upsert( dog ) )
				.call( () -> getMutinySessionFactory().withTransaction( s -> s.find( JoinedDog.class, dog.id ) )
						.invoke( d -> assertThat( d ).isEqualTo( dog ) )
				)
				.call( () -> getMutinySessionFactory().withStatelessTransaction( ss -> ss.upsert( changedDog ) ) )
				.call( () -> getMutinySessionFactory().withTransaction( s -> s.find( JoinedDog.class, dog.id ) )
						.invoke( d -> assertThat( d ).isEqualTo( changedDog ) )
				)
		);
	}

	@Test
	public void testStageUpsertWithJoinedInheritance(VertxTestContext context) {
		JoinedDog dog = new JoinedDog( 2L, "Rex", "Labrador" );
		JoinedDog changedDog = new JoinedDog( 2L, "Max", "Labrador" );
		test( context, getSessionFactory().withStatelessTransaction( ss -> ss.upsert( dog ) )
				.thenCompose( v -> getSessionFactory().withTransaction( s -> s.find( JoinedDog.class, dog.id ) ) )
				.thenAccept( d -> assertThat( d ).isEqualTo( dog ) )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( ss -> ss.upsert( changedDog ) ) )
				.thenCompose( v -> getSessionFactory().withTransaction( s -> s.find( JoinedDog.class, dog.id ) ) )
				.thenAccept( d -> assertThat( d ).isEqualTo( changedDog ) )
		);
	}

	@Test
	public void testMutinyUpsertWithUnionInheritance(VertxTestContext context) {
		UnionDog dog = new UnionDog( 3L, "Rex", "Poodle" );
		UnionDog changedDog = new UnionDog( 3L, "Max", "Poodle" );
		test( context, getMutinySessionFactory().withStatelessTransaction( ss -> ss.upsert( dog ) )
				.call( () -> getMutinySessionFactory().withTransaction( s -> s.find( UnionDog.class, dog.id ) )
						.invoke( d -> assertThat( d ).isEqualTo( dog ) )
				)
				.call( () -> getMutinySessionFactory().withStatelessTransaction( ss -> ss.upsert( changedDog ) ) )
				.call( () -> getMutinySessionFactory().withTransaction( s -> s.find( UnionDog.class, dog.id ) )
						.invoke( d -> assertThat( d ).isEqualTo( changedDog ) )
				)
		);
	}

	@Test
	public void testStageUpsertWithUnionInheritance(VertxTestContext context) {
		UnionDog dog = new UnionDog( 4L, "Rex", "Poodle" );
		UnionDog changedDog = new UnionDog( 4L, "Max", "Poodle" );
		test( context, getSessionFactory().withStatelessTransaction( ss -> ss.upsert( dog ) )
				.thenCompose( v -> getSessionFactory().withTransaction( s -> s.find( UnionDog.class, dog.id ) ) )
				.thenAccept( d -> assertThat( d ).isEqualTo( dog ) )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( ss -> ss.upsert( changedDog ) ) )
				.thenCompose( v -> getSessionFactory().withTransaction( s -> s.find( UnionDog.class, dog.id ) ) )
				.thenAccept( d -> assertThat( d ).isEqualTo( changedDog ) )
		);
	}

	@Test
	public void testMutinyDeleteThenUpsert(VertxTestContext context) {
		JoinedDog dog = new JoinedDog( 5L, "Rex", "Beagle" );
		JoinedDog changedDog = new JoinedDog( 5L, "Max", "Beagle" );
		test( context, getMutinySessionFactory().withStatelessTransaction( ss -> ss.upsert( dog ) )
				.call( () -> getMutinySessionFactory().withStatelessTransaction( ss -> ss.delete( dog ) ) )
				.call( () -> getMutinySessionFactory().withStatelessTransaction( ss -> ss.upsert( changedDog ) ) )
				.call( () -> getMutinySessionFactory().withTransaction( s -> s.find( JoinedDog.class, dog.id ) )
						.invoke( d -> assertThat( d ).isEqualTo( changedDog ) )
				)
		);
	}

	@Test
	public void testStageDeleteThenUpsert(VertxTestContext context) {
		JoinedDog dog = new JoinedDog( 6L, "Rex", "Beagle" );
		JoinedDog changedDog = new JoinedDog( 6L, "Max", "Beagle" );
		test( context, getSessionFactory().withStatelessTransaction( ss -> ss.upsert( dog ) )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( ss -> ss.delete( dog ) ) )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( ss -> ss.upsert( changedDog ) ) )
				.thenCompose( v -> getSessionFactory().withTransaction( s -> s.find( JoinedDog.class, dog.id ) ) )
				.thenAccept( d -> assertThat( d ).isEqualTo( changedDog ) )
		);
	}

	@Test
	public void testMutinyUpsertOnlySubclassFieldWithJoinedInheritance(VertxTestContext context) {
		JoinedDog dog = new JoinedDog( 7L, "Rex", "Labrador" );
		JoinedDog changedDog = new JoinedDog( 7L, "Rex", "Poodle" );
		test( context, getMutinySessionFactory().withStatelessTransaction( ss -> ss.upsert( dog ) )
				.call( () -> getMutinySessionFactory().withStatelessTransaction( ss -> ss.upsert( changedDog ) ) )
				.call( () -> getMutinySessionFactory().withTransaction( s -> s.find( JoinedDog.class, dog.id ) )
						.invoke( d -> assertThat( d ).isEqualTo( changedDog ) )
				)
		);
	}

	@Test
	public void testStageUpsertOnlySubclassFieldWithJoinedInheritance(VertxTestContext context) {
		JoinedDog dog = new JoinedDog( 8L, "Rex", "Labrador" );
		JoinedDog changedDog = new JoinedDog( 8L, "Rex", "Poodle" );
		test( context, getSessionFactory().withStatelessTransaction( ss -> ss.upsert( dog ) )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( ss -> ss.upsert( changedDog ) ) )
				.thenCompose( v -> getSessionFactory().withTransaction( s -> s.find( JoinedDog.class, dog.id ) ) )
				.thenAccept( d -> assertThat( d ).isEqualTo( changedDog ) )
		);
	}

	@Test
	public void testUpsertAllFieldsWithJoinedInheritance(VertxTestContext context) {
		JoinedDog dog = new JoinedDog( 9L, "Rex", "Labrador" );
		JoinedDog changedDog = new JoinedDog( 9L, "Max", "Poodle" );
		test( context, getMutinySessionFactory().withStatelessTransaction( ss -> ss.upsert( dog ) )
				.call( () -> getMutinySessionFactory().withStatelessTransaction( ss -> ss.upsert( changedDog ) ) )
				.call( () -> getMutinySessionFactory().withTransaction( s -> s.find( JoinedDog.class, dog.id ) )
						.invoke( d -> assertThat( d ).isEqualTo( changedDog ) )
				)
		);
	}

	@Test
	public void testUpsertOnlySubclassFieldWithUnionInheritance(VertxTestContext context) {
		UnionDog dog = new UnionDog( 10L, "Rex", "Labrador" );
		UnionDog changedDog = new UnionDog( 10L, "Rex", "Poodle" );
		test( context, getMutinySessionFactory().withStatelessTransaction( ss -> ss.upsert( dog ) )
				.call( () -> getMutinySessionFactory().withStatelessTransaction( ss -> ss.upsert( changedDog ) ) )
				.call( () -> getMutinySessionFactory().withTransaction( s -> s.find( UnionDog.class, dog.id ) )
						.invoke( d -> assertThat( d ).isEqualTo( changedDog ) )
				)
		);
	}

	@Test
	public void testUpsertWithSingleTableInheritance(VertxTestContext context) {
		SingleTableDog dog = new SingleTableDog( 11L, "Rex", "Labrador" );
		SingleTableDog changedDog = new SingleTableDog( 11L, "Rex", "Poodle" );
		test( context, getMutinySessionFactory().withStatelessTransaction( ss -> ss.upsert( dog ) )
				.call( () -> getMutinySessionFactory().withTransaction( s -> s.find( SingleTableDog.class, dog.id ) )
						.invoke( d -> assertThat( d ).isEqualTo( dog ) )
				)
				.call( () -> getMutinySessionFactory().withStatelessTransaction( ss -> ss.upsert( changedDog ) ) )
				.call( () -> getMutinySessionFactory().withTransaction( s -> s.find( SingleTableDog.class, dog.id ) )
						.invoke( d -> assertThat( d ).isEqualTo( changedDog ) )
				)
		);
	}

	@Entity(name = "JoinedAnimal")
	@Table(name = "JoinedAnimal")
	@Inheritance(strategy = InheritanceType.JOINED)
	public static class JoinedAnimal {
		@Id public Long id;
		public String name;

		JoinedAnimal() {}

		JoinedAnimal(Long id, String name) {
			this.id = id;
			this.name = name;
		}
	}

	@Entity(name = "JoinedDog")
	@Table(name = "JoinedDog")
	public static class JoinedDog extends JoinedAnimal {
		public String breed;

		JoinedDog() {}

		JoinedDog(Long id, String name, String breed) {
			super( id, name );
			this.breed = breed;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			JoinedDog dog = (JoinedDog) o;
			return Objects.equals( name, dog.name )
					&& Objects.equals( breed, dog.breed );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name, breed );
		}

		@Override
		public String toString() {
			return id + ":" + name + ":" + breed;
		}
	}

	@Entity(name = "UnionAnimal")
	@Table(name = "UnionAnimal")
	@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
	public static class UnionAnimal {
		@Id public Long id;
		public String name;

		UnionAnimal() {}

		UnionAnimal(Long id, String name) {
			this.id = id;
			this.name = name;
		}
	}

	@Entity(name = "UnionDog")
	@Table(name = "UnionDog")
	public static class UnionDog extends UnionAnimal {
		public String breed;

		UnionDog() {}

		UnionDog(Long id, String name, String breed) {
			super( id, name );
			this.breed = breed;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			UnionDog dog = (UnionDog) o;
			return Objects.equals( name, dog.name )
					&& Objects.equals( breed, dog.breed );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name, breed );
		}

		@Override
		public String toString() {
			return id + ":" + name + ":" + breed;
		}
	}

	@Entity(name = "SingleTableAnimal")
	@Table(name = "SingleTableAnimal")
	@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
	public static class SingleTableAnimal {
		@Id public Long id;
		public String name;

		SingleTableAnimal() {}

		SingleTableAnimal(Long id, String name) {
			this.id = id;
			this.name = name;
		}
	}

	@Entity(name = "SingleTableDog")
	@DiscriminatorValue("Dog")
	public static class SingleTableDog extends SingleTableAnimal {
		public String breed;

		SingleTableDog() {}

		SingleTableDog(Long id, String name, String breed) {
			super( id, name );
			this.breed = breed;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			SingleTableDog dog = (SingleTableDog) o;
			return Objects.equals( name, dog.name )
					&& Objects.equals( breed, dog.breed );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name, breed );
		}

		@Override
		public String toString() {
			return id + ":" + name + ":" + breed;
		}
	}
}
