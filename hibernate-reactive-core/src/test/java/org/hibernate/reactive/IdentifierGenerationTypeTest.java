/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.reactive.testing.DBSelectionExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.testing.DBSelectionExtension.skipTestsFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Check that the generated id can be of a specific type
 *
 * @see org.hibernate.reactive.id.impl.IdentifierGeneration
 */
public class IdentifierGenerationTypeTest extends BaseReactiveTest {

	// Vertx DB2 client is throwing "java.lang.IllegalStateException: Needed to have 6 in buffer but only had 0."
	// TODO:  remove db extension when https://github.com/eclipse-vertx/vertx-sql-client/issues/1208 is resolved
	@RegisterExtension
	public DBSelectionExtension skip = skipTestsFor( DB2 );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( LongEntity.class, IntegerEntity.class, ShortEntity.class );
	}

	/*
	 * Stage API tests
	 */

	@Test
	public void integerIdentifierWithStageAPI(VertxTestContext context) {
		IntegerEntity entityA = new IntegerEntity( "Integer A" );
		IntegerEntity entityB = new IntegerEntity( "Integer B" );

		test( context, openSession()
				.thenCompose( session -> session
						.persist( entityA, entityB )
						.thenCompose( v -> session.flush() ) )
				.thenAccept( v -> assertNotEquals( entityA.id, entityB.id ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( IntegerEntity.class, entityA.id, entityB.id ) )
				.thenAccept( list -> {
					assertEquals( list.size(), 2 );
					assertTrue( list.containsAll( Arrays.asList( entityA, entityB ) ) );
				} )
		);
	}

	@Test
	public void longIdentifierWithStageAPI(VertxTestContext context) {
		LongEntity entityA = new LongEntity( "Long A" );
		LongEntity entityB = new LongEntity( "Long B" );

		test( context, openSession()
				.thenCompose( session -> session
						.persist( entityA, entityB )
						.thenCompose( v -> session.flush() ) )
				.thenAccept( v -> assertNotEquals( entityA.id, entityB.id ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( LongEntity.class, entityA.id, entityB.id ) )
				.thenAccept( list -> {
					assertEquals( list.size(), 2 );
					assertTrue( list.containsAll( Arrays.asList( entityA, entityB ) ) );
				} )
		);
	}

	@Test
	public void shortIdentifierWithStageAPI(VertxTestContext context) {
		ShortEntity entityA = new ShortEntity( "Short A" );
		ShortEntity entityB = new ShortEntity( "Short B" );

		test( context, openSession()
				.thenCompose( session -> session
						.persist( entityA, entityB )
						.thenCompose( v -> session.flush() ) )
				.thenAccept( v -> assertNotEquals( entityA.id, entityB.id ) )
				.thenCompose( v -> openSession() )
				.thenCompose( session -> session.find( ShortEntity.class, entityA.id, entityB.id ) )
				.thenAccept( list -> {
					assertEquals( list.size(), 2 );
					assertTrue( list.containsAll( Arrays.asList( entityA, entityB ) ) );
				} )
		);
	}

	/*
	 * Mutiny API tests
	 */

	@Test
	public void integerIdentifierWithMutinyAPI(VertxTestContext context) {
		IntegerEntity entityA = new IntegerEntity( "Integer A" );
		IntegerEntity entityB = new IntegerEntity( "Integer B" );

		test( context, openMutinySession()
				.chain( session -> session
						.persistAll( entityA, entityB )
						.call( session::flush ) )
				.invoke( () -> assertNotEquals( entityA.id, entityB.id ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( IntegerEntity.class, entityA.id, entityB.id ) )
				.invoke( list -> {
					assertEquals( list.size(), 2 );
					assertTrue( list.containsAll( Arrays.asList( entityA, entityB ) ) );
				} )
		);
	}

	@Test
	public void longIdentifierWithMutinyAPI(VertxTestContext context) {
		LongEntity entityA = new LongEntity( "Long A" );
		LongEntity entityB = new LongEntity( "Long B" );

		test( context, openMutinySession()
				.chain( session -> session
						.persistAll( entityA, entityB )
				.call( session::flush ) )
				.invoke( () -> assertNotEquals( entityA.id, entityB.id ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( LongEntity.class, entityA.id, entityB.id ) )
				.invoke( list -> {
					assertEquals( list.size(), 2 );
					assertTrue( list.containsAll( Arrays.asList( entityA, entityB ) ) );
				} )
		);
	}

	@Test
	public void shortIdentifierWithMutinyAPI(VertxTestContext context) {
		ShortEntity entityA = new ShortEntity( "Short A" );
		ShortEntity entityB = new ShortEntity( "Short B" );

		test( context, openMutinySession()
				.chain( session -> session
						.persistAll( entityA, entityB )
						.call( session::flush ) )
				.invoke( () -> assertNotEquals( entityA.id, entityB.id ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( ShortEntity.class, entityA.id, entityB.id ) )
				.invoke( list -> {
					assertEquals( list.size(), 2 );
					assertTrue( list.containsAll( Arrays.asList( entityA, entityB ) ) );
				} )
		);
	}

	@Entity(name = "LongEntity")
	private static class LongEntity {

		@Id
		@GeneratedValue
		Long id;

		@Column(unique = true)
		String name;

		public LongEntity() {
		}

		public LongEntity(String name) {
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
			LongEntity that = (LongEntity) o;
			return Objects.equals( name, that.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}

		@Override
		public String toString() {
			return id + ":" + name;
		}
	}

	@Entity(name = "IntegerEntity")
	private static class IntegerEntity {

		@Id
		@GeneratedValue
		Integer id;

		@Column(unique = true)
		String name;

		public IntegerEntity() {
		}

		public IntegerEntity(String name) {
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
			IntegerEntity that = (IntegerEntity) o;
			return Objects.equals( name, that.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}

		@Override
		public String toString() {
			return id + ":" + name;
		}
	}

	@Entity(name = "ShortEntity")
	private static class ShortEntity {

		@Id
		@GeneratedValue
		Short id;

		@Column(unique = true)
		String name;

		public ShortEntity() {
		}

		public ShortEntity(String name) {
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
			ShortEntity that = (ShortEntity) o;
			return Objects.equals( name, that.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}

		@Override
		public String toString() {
			return id + ":" + name;
		}
	}
}
