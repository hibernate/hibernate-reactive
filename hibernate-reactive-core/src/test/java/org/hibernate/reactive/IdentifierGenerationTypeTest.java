/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Arrays;
import java.util.Objects;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.stage.Stage;

import org.junit.After;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

/**
 * Check that the generated id can be of a specific type
 *
 * @see org.hibernate.reactive.id.impl.IdentifierGeneration
 */
public class IdentifierGenerationTypeTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( LongEntity.class );
		configuration.addAnnotatedClass( IntegerEntity.class );
		configuration.addAnnotatedClass( ShortEntity.class );
		return configuration;
	}

	@After
	public void cleanDb(TestContext context) {
		test( context, deleteEntities( LongEntity.class, IntegerEntity.class, ShortEntity.class ) );
	}

	/*
	 * Stage API tests
	 */

	@Test
	public void integerIdentifierWithStageAPI(TestContext context) {
		IntegerEntity entityA = new IntegerEntity( "Integer A" );
		IntegerEntity entityB = new IntegerEntity( "Integer B" );

		Stage.Session session = openSession();
		test( context, session
				.persist( entityA, entityB )
				.thenCompose( v -> session.flush() )
				.thenAccept( v -> context.assertNotEquals( entityA.id, entityB.id ) )
				.thenCompose( v -> openSession().find( IntegerEntity.class, entityA.id, entityB.id ) )
				.thenAccept( list -> {
					context.assertEquals( list.size(), 2 );
					context.assertTrue( list.containsAll( Arrays.asList( entityA, entityB ) ) );
				} )
		);
	}

	@Test
	public void longIdentifierWithStageAPI(TestContext context) {
		LongEntity entityA = new LongEntity( "Long A" );
		LongEntity entityB = new LongEntity( "Long B" );

		Stage.Session session = openSession();
		test( context, session
				.persist( entityA, entityB )
				.thenCompose( v -> session.flush() )
				.thenAccept( v -> context.assertNotEquals( entityA.id, entityB.id ) )
				.thenCompose( v -> openSession().find( LongEntity.class, entityA.id, entityB.id ) )
				.thenAccept( list -> {
					context.assertEquals( list.size(), 2 );
					context.assertTrue( list.containsAll( Arrays.asList( entityA, entityB ) ) );
				} )
		);
	}

	@Test
	public void shortIdentifierWithStageAPI(TestContext context) {
		ShortEntity entityA = new ShortEntity( "Short A" );
		ShortEntity entityB = new ShortEntity( "Short B" );

		Stage.Session session = openSession();
		test( context, session
				.persist( entityA, entityB )
				.thenCompose( v -> session.flush() )
				.thenAccept( v -> context.assertNotEquals( entityA.id, entityB.id ) )
				.thenCompose( v -> openSession().find( ShortEntity.class, entityA.id, entityB.id ) )
				.thenAccept( list -> {
					context.assertEquals( list.size(), 2 );
					context.assertTrue( list.containsAll( Arrays.asList( entityA, entityB ) ) );
				} )
		);
	}

	/*
	 * Mutiny API tests
	 */

	@Test
	public void integerIdentifierWithMutinyAPI(TestContext context) {
		IntegerEntity entityA = new IntegerEntity( "Integer A" );
		IntegerEntity entityB = new IntegerEntity( "Integer B" );

		Mutiny.Session session = openMutinySession();
		test( context, session
				.persistAll( entityA, entityB )
				.call( session::flush )
				.invoke( () -> context.assertNotEquals( entityA.id, entityB.id ) )
				.chain( () -> openMutinySession().find( IntegerEntity.class, entityA.id, entityB.id ) )
				.invoke( list -> {
					context.assertEquals( list.size(), 2 );
					context.assertTrue( list.containsAll( Arrays.asList( entityA, entityB ) ) );
				} )
		);
	}

	@Test
	public void longIdentifierWithMutinyAPI(TestContext context) {
		LongEntity entityA = new LongEntity( "Long A" );
		LongEntity entityB = new LongEntity( "Long B" );

		Mutiny.Session session = openMutinySession();
		test( context, session
				.persistAll( entityA, entityB )
				.call( session::flush )
				.invoke( () -> context.assertNotEquals( entityA.id, entityB.id ) )
				.chain( () -> openMutinySession().find( LongEntity.class, entityA.id, entityB.id ) )
				.invoke( list -> {
					context.assertEquals( list.size(), 2 );
					context.assertTrue( list.containsAll( Arrays.asList( entityA, entityB ) ) );
				} )
		);
	}

	@Test
	public void shortIdentifierWithMutinyAPI(TestContext context) {
		ShortEntity entityA = new ShortEntity( "Short A" );
		ShortEntity entityB = new ShortEntity( "Short B" );

		Mutiny.Session session = openMutinySession();
		test( context, session
				.persistAll( entityA, entityB )
				.call( session::flush )
				.invoke( () -> context.assertNotEquals( entityA.id, entityB.id ) )
				.chain( () -> openMutinySession().find( ShortEntity.class, entityA.id, entityB.id ) )
				.invoke( list -> {
					context.assertEquals( list.size(), 2 );
					context.assertTrue( list.containsAll( Arrays.asList( entityA, entityB ) ) );
				} )
		);
	}

	@Entity
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

	@Entity
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

	@Entity
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
