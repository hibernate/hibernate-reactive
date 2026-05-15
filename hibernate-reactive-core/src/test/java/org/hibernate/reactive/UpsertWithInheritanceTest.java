/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
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
		return List.of( JoinedAnimal.class, JoinedDog.class, UnionAnimal.class, UnionDog.class );
	}

	@Test
	public void testMutinyUpsertWithJoinedInheritance(VertxTestContext context) {
		test( context, getMutinySessionFactory().withStatelessTransaction( ss -> ss
						.upsert( new JoinedDog( 1L, "Rex", "Labrador" ) )
				)
				.call( () -> getMutinySessionFactory().withTransaction( s -> s.find( JoinedDog.class, 1L ) )
						.invoke( d -> assertThat( d.name ).isEqualTo( "Rex" ) )
				)
				.call( () -> getMutinySessionFactory().withStatelessTransaction( ss -> ss
						.upsert( new JoinedDog( 1L, "Max", "Labrador" ) )
				) )
				.call( () -> getMutinySessionFactory().withTransaction( s -> s.find( JoinedDog.class, 1L ) )
						.invoke( d -> assertThat( d.name ).isEqualTo( "Max" ) )
				)
		);
	}

	@Test
	public void testStageUpsertWithJoinedInheritance(VertxTestContext context) {
		test( context, getSessionFactory().withStatelessTransaction( ss -> ss
						.upsert( new JoinedDog( 2L, "Rex", "Labrador" ) )
				)
				.thenCompose( v -> getSessionFactory().withTransaction( s -> s.find( JoinedDog.class, 2L ) ) )
				.thenAccept( d -> assertThat( d.name ).isEqualTo( "Rex" ) )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( ss -> ss
						.upsert( new JoinedDog( 2L, "Max", "Labrador" ) )
				) )
				.thenCompose( v -> getSessionFactory().withTransaction( s -> s.find( JoinedDog.class, 2L ) ) )
				.thenAccept( d -> assertThat( d.name ).isEqualTo( "Max" ) )
		);
	}

	@Test
	public void testMutinyUpsertWithUnionInheritance(VertxTestContext context) {
		test( context, getMutinySessionFactory().withStatelessTransaction( ss -> ss
						.upsert( new UnionDog( 3L, "Rex", "Poodle" ) )
				)
				.call( () -> getMutinySessionFactory().withTransaction( s -> s.find( UnionDog.class, 3L ) )
						.invoke( d -> assertThat( d.name ).isEqualTo( "Rex" ) )
				)
				.call( () -> getMutinySessionFactory().withStatelessTransaction( ss -> ss
						.upsert( new UnionDog( 3L, "Max", "Poodle" ) )
				) )
				.call( () -> getMutinySessionFactory().withTransaction( s -> s.find( UnionDog.class, 3L ) )
						.invoke( d -> assertThat( d.name ).isEqualTo( "Max" ) )
				)
		);
	}

	@Test
	public void testStageUpsertWithUnionInheritance(VertxTestContext context) {
		test( context, getSessionFactory().withStatelessTransaction( ss -> ss
						.upsert( new UnionDog( 4L, "Rex", "Poodle" ) )
				)
				.thenCompose( v -> getSessionFactory().withTransaction( s -> s.find( UnionDog.class, 4L ) ) )
				.thenAccept( d -> assertThat( d.name ).isEqualTo( "Rex" ) )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( ss -> ss
						.upsert( new UnionDog( 4L, "Max", "Poodle" ) )
				) )
				.thenCompose( v -> getSessionFactory().withTransaction( s -> s.find( UnionDog.class, 4L ) ) )
				.thenAccept( d -> assertThat( d.name ).isEqualTo( "Max" ) )
		);
	}

	@Test
	public void testMutinyDeleteThenUpsert(VertxTestContext context) {
		JoinedDog dog = new JoinedDog( 5L, "Rex", "Beagle" );
		test( context, getMutinySessionFactory().withStatelessTransaction( ss -> ss.upsert( dog ) )
				.call( () -> getMutinySessionFactory().withStatelessTransaction( ss -> ss.delete( dog ) ) )
				.call( () -> getMutinySessionFactory().withStatelessTransaction( ss -> ss
						.upsert( new JoinedDog( 5L, "Max", "Beagle" ) )
				) )
				.call( () -> getMutinySessionFactory().withTransaction( s -> s.find( JoinedDog.class, 5L ) )
						.invoke( d -> assertThat( d.name ).isEqualTo( "Max" ) )
				)
		);
	}

	@Test
	public void testStageDeleteThenUpsert(VertxTestContext context) {
		JoinedDog dog = new JoinedDog( 6L, "Rex", "Beagle" );
		test( context, getSessionFactory().withStatelessTransaction( ss -> ss.upsert( dog ) )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( ss -> ss.delete( dog ) ) )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( ss -> ss
						.upsert( new JoinedDog( 6L, "Max", "Beagle" ) )
				) )
				.thenCompose( v -> getSessionFactory().withTransaction( s -> s.find( JoinedDog.class, 6L ) ) )
				.thenAccept( d -> assertThat( d.name ).isEqualTo( "Max" ) )
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
	}
}
