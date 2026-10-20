/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.TransientPropertyValueException;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;

/**
 * Persisting an entity with a non-nullable association to a transient entity
 * must fail when the operation completes, not when the session is flushed.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class UnresolvedInsertActionsTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		// Moon references Planet and Asteroid references Star:
		// the default cleanup must remove the moons and the asteroids first
		return List.of( Moon.class, Planet.class, Asteroid.class, Star.class );
	}

	@Test
	public void persistWithTransientAssociationWithMutiny(VertxTestContext context) {
		final Moon moon = new Moon( 1, "Titan", new Planet( 1, "Saturn" ) );
		test( context, assertThrown(
				IllegalStateException.class,
				// No flush: the operation itself must fail
				getMutinySessionFactory().withSession( s -> s.persist( moon ) )
		).invoke( UnresolvedInsertActionsTest::assertTransientPlanet ) );
	}

	@Test
	public void persistWithTransientAssociationWithStage(VertxTestContext context) {
		final Moon moon = new Moon( 2, "Europa", new Planet( 2, "Jupiter" ) );
		test( context, assertThrown(
				IllegalStateException.class,
				// No flush: the operation itself must fail
				getSessionFactory().withSession( s -> s.persist( moon ) )
		).thenAccept( UnresolvedInsertActionsTest::assertTransientPlanet ) );
	}

	@Test
	public void persistWithPersistedAssociation(VertxTestContext context) {
		final Planet planet = new Planet( 3, "Mars" );
		final Moon moon = new Moon( 3, "Phobos", planet );
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persist( planet ).call( () -> s.persist( moon ) ) )
				.chain( () -> getMutinySessionFactory().withSession( s -> s.find( Moon.class, 3 ) ) )
				.invoke( found -> assertMoon( found, moon ) )
		);
	}

	@Test
	public void persistAllWithAssociationPersistedLaterWithMutiny(VertxTestContext context) {
		final Planet planet = new Planet( 4, "Neptune" );
		final Moon moon = new Moon( 4, "Triton", planet );
		test( context, getMutinySessionFactory()
				// persistAll is a single operation, the order of the entities doesn't matter
				.withTransaction( s -> s.persistAll( moon, planet ) )
				.chain( () -> getMutinySessionFactory().withSession( s -> s.find( Moon.class, 4 ) ) )
				.invoke( found -> assertMoon( found, moon ) )
		);
	}

	@Test
	public void persistMultipleWithAssociationPersistedLaterWithMutiny(VertxTestContext context) {
		final Planet planet = new Planet( 5, "Uranus" );
		final Moon moon = new Moon( 5, "Miranda", planet );
		test( context, getMutinySessionFactory()
				// persistMultiple is a single operation, the order of the entities doesn't matter
				.withTransaction( s -> s.persistMultiple( List.of( moon, planet ) ) )
				.chain( () -> getMutinySessionFactory().withSession( s -> s.find( Moon.class, 5 ) ) )
				.invoke( found -> assertMoon( found, moon ) )
		);
	}

	@Test
	public void persistAllWithAssociationPersistedLaterWithStage(VertxTestContext context) {
		final Planet planet = new Planet( 6, "Earth" );
		final Moon moon = new Moon( 6, "Moon", planet );
		test( context, getSessionFactory()
				// persist(Object...) is a single operation, the order of the entities doesn't matter
				.withTransaction( s -> s.persist( moon, planet ) )
				.thenCompose( v -> getSessionFactory().withSession( s -> s.find( Moon.class, 6 ) ) )
				.thenAccept( found -> assertMoon( found, moon ) )
		);
	}

	@Test
	public void persistAllWithTransientAssociationWithMutiny(VertxTestContext context) {
		final Moon moon = new Moon( 7, "Charon", new Planet( 7, "Pluto" ) );
		final Planet planet = new Planet( 8, "Mercury" );
		test( context, assertThrown(
				IllegalStateException.class,
				// No flush: the operation itself must fail
				getMutinySessionFactory().withSession( s -> s.persistAll( moon, planet ) )
		).invoke( UnresolvedInsertActionsTest::assertTransientPlanet ) );
	}

	// A Star has a generated id, so merge() treats a new Star as transient

	@Test
	public void mergeWithTransientAssociationWithMutiny(VertxTestContext context) {
		final Asteroid asteroid = new Asteroid( 1, "Ceres", new Star( "Sun" ) );
		test( context, assertThrown(
				IllegalStateException.class,
				// No flush: the operation itself must fail
				getMutinySessionFactory().withSession( s -> s.merge( asteroid ) )
		).invoke( UnresolvedInsertActionsTest::assertTransientStar ) );
	}

	@Test
	public void mergeWithTransientAssociationWithStage(VertxTestContext context) {
		final Asteroid asteroid = new Asteroid( 2, "Vesta", new Star( "Sun" ) );
		test( context, assertThrown(
				IllegalStateException.class,
				// No flush: the operation itself must fail
				getSessionFactory().withSession( s -> s.merge( asteroid ) )
		).thenAccept( UnresolvedInsertActionsTest::assertTransientStar ) );
	}

	@Test
	public void mergeWithPersistedAssociation(VertxTestContext context) {
		final Star star = new Star( "Proxima Centauri" );
		final Asteroid asteroid = new Asteroid( 3, "Pallas", star );
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persist( star ) )
				// The star is now detached
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s.merge( asteroid ) ) )
				.chain( () -> getMutinySessionFactory().withSession( s -> s.find( Asteroid.class, 3 ) ) )
				.invoke( found -> {
					assertThat( found ).isEqualTo( asteroid );
					assertThat( found.star ).isEqualTo( star );
				} )
		);
	}

	private static void assertMoon(Moon actual, Moon expected) {
		assertThat( actual ).isEqualTo( expected );
		assertThat( actual.planet ).isEqualTo( expected.planet );
	}

	private static void assertTransientPlanet(IllegalStateException e) {
		assertTransientAssociation( e, Moon.class, "planet" );
	}

	private static void assertTransientStar(IllegalStateException e) {
		assertTransientAssociation( e, Asteroid.class, "star" );
	}

	private static void assertTransientAssociation(IllegalStateException e, Class<?> owner, String property) {
		assertThat( e.getCause() ).isInstanceOf( TransientPropertyValueException.class );
		final TransientPropertyValueException cause = (TransientPropertyValueException) e.getCause();
		assertThat( cause.getPropertyOwnerEntityName() ).isEqualTo( owner.getName() );
		assertThat( cause.getPropertyName() ).isEqualTo( property );
	}

	@Entity(name = "Planet")
	@Table(name = "UnresolvedPlanet")
	public static class Planet {
		@Id
		Integer id;
		String name;

		public Planet() {
		}

		public Planet(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Planet planet = (Planet) o;
			return Objects.equals( name, planet.name );
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

	@Entity(name = "Moon")
	@Table(name = "UnresolvedMoon")
	public static class Moon {
		@Id
		Integer id;
		String name;
		@ManyToOne(optional = false)
		Planet planet;

		public Moon() {
		}

		public Moon(Integer id, String name, Planet planet) {
			this.id = id;
			this.name = name;
			this.planet = planet;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Moon moon = (Moon) o;
			return Objects.equals( name, moon.name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
		}

		@Override
		public String toString() {
			return id + ":" + name + " (" + planet + ")";
		}
	}

	@Entity(name = "Star")
	@Table(name = "UnresolvedStar")
	public static class Star {
		@Id
		@GeneratedValue
		Long id;
		String name;

		public Star() {
		}

		public Star(String name) {
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Star star = (Star) o;
			return Objects.equals( name, star.name );
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

	@Entity(name = "Asteroid")
	@Table(name = "UnresolvedAsteroid")
	public static class Asteroid {
		@Id
		Integer id;
		String name;
		@ManyToOne(optional = false)
		Star star;

		public Asteroid() {
		}

		public Asteroid(Integer id, String name, Star star) {
			this.id = id;
			this.name = name;
			this.star = star;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Asteroid asteroid = (Asteroid) o;
			return Objects.equals( name, asteroid.name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
		}

		@Override
		public String toString() {
			return id + ":" + name + " (" + star + ")";
		}
	}
}
