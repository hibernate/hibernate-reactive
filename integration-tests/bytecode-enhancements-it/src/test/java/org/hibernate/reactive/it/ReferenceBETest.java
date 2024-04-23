/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.it;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.Hibernate;
import org.hibernate.LockMode;
import org.hibernate.reactive.it.reference.Author;
import org.hibernate.reactive.it.reference.Book;
import org.hibernate.reactive.stage.Stage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 10, timeUnit = MINUTES)

public class ReferenceBETest extends BaseReactiveIT {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Author.class, Book.class );
	}

	@Override
	public CompletionStage<Void> deleteEntities(Class<?>... entities) {
		return getSessionFactory()
				.withTransaction( s -> loop( entities, entityClass -> s
						.createQuery( "from " + entityName( entityClass ), entityClass )
						.getResultList()
						.thenCompose( list -> loop( list, s::remove ) ) ) );
	}

	private String entityName(Class<?> entityClass) {
		if ( Author.class.equals( entityClass ) ) {
			return "Writer";
		}
		return "Tome";
	}

	@Test
	public void testDetachedEntityReference(VertxTestContext context) {
		final Book goodOmens = new Book( "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch" );

		test( context, getSessionFactory()
				.withSession( s -> s.persist( goodOmens ).thenCompose( v -> s.flush() ) )
				.thenCompose( v -> getSessionFactory().withSession( s -> {
					Book book = s.getReference( Book.class, goodOmens.getId() );
					assertFalse( Hibernate.isInitialized( book ) );
					return s.persist( new Author( "Neil Gaiman", book ) )
							.thenCompose( vv -> s.flush() );
				} ) )
				.thenCompose( v -> getSessionFactory().withSession( s -> {
					Book book = s.getReference( goodOmens );
					assertFalse( Hibernate.isInitialized( book ) );
					return s.persist( new Author( "Terry Pratchett", book ) )
							.thenCompose( vv -> s.flush() );
				} ) )
				.thenCompose( v -> getSessionFactory().withSession( s -> {
					Book book = s.getReference( goodOmens );
					assertFalse( Hibernate.isInitialized( book ) );
					return Stage.fetch( book ).thenCompose( vv -> Stage.fetch( book.getAuthors() ) );
				} ) )
				.thenAccept( optionalAssociation -> {
					assertTrue( Hibernate.isInitialized( optionalAssociation ) );
					assertNotNull( optionalAssociation );
					assertEquals( 2, optionalAssociation.size() );
				} )
		);
	}

	@Test
	public void testDetachedProxyReference(VertxTestContext context) {
		final Book goodOmens = new Book( "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch" );

		test( context, getSessionFactory()
				.withSession( s -> s.persist( goodOmens ).thenCompose( v -> s.flush() ) )
				.thenCompose( v -> getSessionFactory().withSession( s -> {
					Book reference = s.getReference( goodOmens );
					assertFalse( Hibernate.isInitialized( reference ) );
					return completedFuture( reference );
				} ) )
				.thenCompose( reference -> getSessionFactory().withSession( s -> {
					Book book = s.getReference( Book.class, reference.getId() );
					assertFalse( Hibernate.isInitialized( book ) );
					assertFalse( Hibernate.isInitialized( reference ) );
					return s.persist( new Author( "Neil Gaiman", book ) )
							.thenCompose( v -> s.flush() )
							.thenApply( v -> reference );
				} ) )
				.thenCompose( reference -> getSessionFactory().withSession( s -> {
					Book book = s.getReference( reference );
					assertFalse( Hibernate.isInitialized( book ) );
					assertFalse( Hibernate.isInitialized( reference ) );
					return s.persist( new Author( "Terry Pratchett", book ) )
							.thenCompose( v -> s.flush() )
							.thenApply( v -> reference );
				} ) )
				.thenCompose( reference -> getSessionFactory().withSession( s -> {
					Book book = s.getReference( reference );
					assertFalse( Hibernate.isInitialized( book ) );
					assertFalse( Hibernate.isInitialized( reference ) );
					return Stage.fetch( book ).thenCompose( v -> Stage.fetch( book.getAuthors() ) );
				} ) )
				.thenAccept( optionalAssociation -> {
					assertTrue( Hibernate.isInitialized( optionalAssociation ) );
					assertNotNull( optionalAssociation );
					assertEquals( 2, optionalAssociation.size() );
				} )
		);
	}

	@Test
	public void testRemoveDetachedProxy(VertxTestContext context) {
		final Book goodOmens = new Book( "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch" );
		test( context, getSessionFactory()
				.withSession( s -> s.persist( goodOmens ).thenCompose( v -> s.flush() ) )
				.thenCompose( v -> getSessionFactory().withSession( sess -> {
					Book reference = sess.getReference( goodOmens );
					assertFalse( Hibernate.isInitialized( reference ) );
					return completedFuture( reference );
				} ) )
				.thenCompose( reference -> getSessionFactory().withSession( s -> s
						.remove( s.getReference( reference ) ).thenCompose( v -> s.flush() ) )
				)
				.thenCompose( reference -> getSessionFactory().withSession( s -> s
						.find( Book.class, goodOmens.getId() )
						.thenAccept( Assertions::assertNull )
				) )
		);
	}

	@Test
	public void testRemoveWithTransaction(VertxTestContext context) {
		final Book goodOmens = new Book( "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch" );
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persist( goodOmens ) )
				.call( () -> getMutinySessionFactory()
						.withSession( s -> s.find( Book.class, goodOmens.getId() ) )
						.invoke( book -> assertEquals( goodOmens, book ) ) )
				.call( () -> getMutinySessionFactory().withTransaction( s -> s
						.remove( s.getReference( Book.class, goodOmens.getId() ) ) ) )
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.find( Book.class, goodOmens.getId() ) ) )
				.invoke( Assertions::assertNull )
		);
	}

	@Test
	public void testLockDetachedProxy(VertxTestContext context) {
		final Book goodOmens = new Book( "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch" );
		test( context, getSessionFactory().withSession( s -> s
							  .persist( goodOmens ).thenCompose( v -> s.flush() )
					  )
					  .thenCompose( v -> getSessionFactory().withSession( s -> {
						  Book reference = s.getReference( goodOmens );
						  assertFalse( Hibernate.isInitialized( reference ) );
						  return completedFuture( reference );
					  } ) )
					  .thenCompose( reference -> getSessionFactory().withSession( s -> s
							  .lock( s.getReference( reference ), LockMode.PESSIMISTIC_FORCE_INCREMENT )
							  .thenCompose( v -> s.flush() ) )
					  )
					  .thenCompose( v -> getSessionFactory().withSession( s -> s
									  .find( Book.class, goodOmens.getId() ) )
							  .thenAccept( book -> {
								  assertNotNull( book );
								  assertEquals( 2, book.getVersion() );
							  } )
					  )
		);
	}

	@Test
	public void testRefreshDetachedProxy(VertxTestContext context) {
		final Book goodOmens = new Book( "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch" );
		test( context, getSessionFactory().withSession( s -> s
							  .persist( goodOmens ).thenCompose( v -> s.flush() )
					  )
					  .thenCompose( v -> getSessionFactory().withSession( s -> {
						  Book reference = s.getReference( goodOmens );
						  assertFalse( Hibernate.isInitialized( reference ) );
						  return completedFuture( reference );
					  } ) )
					  .thenCompose( reference -> getSessionFactory().withSession( s -> s
							  .refresh( s.getReference( reference ) )
							  .thenAccept( v -> assertTrue( Hibernate.isInitialized( s.getReference( reference ) ) ) )
							  .thenCompose( v -> s.flush() ) )
					  )
					  .thenCompose( v -> getSessionFactory().withSession( s -> s
							  .find( Book.class, goodOmens.getId() )
							  .thenAccept( book -> {
								  assertNotNull( book );
								  assertEquals( 1, book.getVersion() );
							  } )
					  ) )
		);
	}

	@Test
	public void testFetchOfReference(VertxTestContext context) {
		final Book goodOmens = new Book( "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch" );
		final Author neil = new Author( "Neil Gaiman", goodOmens );
		final Author terry = new Author( "Terry Pratchett", goodOmens );
		goodOmens.getAuthors().add( neil );
		goodOmens.getAuthors().add( terry );

		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persistAll( goodOmens, terry, neil ) )
				.call( () -> getMutinySessionFactory().withSession( s -> s
						// Not the most common case, but should be possible
						.fetch( s.getReference( Book.class, goodOmens.getId() ) )
						.chain( reference -> s.fetch( reference.getAuthors() ) )
						.invoke( authors -> assertThat( authors ).containsExactlyInAnyOrder( terry, neil ) )
				) )
		);
	}
}
