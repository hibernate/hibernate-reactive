/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.hibernate.Hibernate;
import org.hibernate.reactive.annotations.EnabledFor;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 10, timeUnit = MINUTES)
@EnabledFor(POSTGRESQL)
public class SessionOperationSerializationTest extends BaseReactiveTest {

	private static final int ENTITY_COUNT = 100;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( SimpleEntity.class, Author.class, Book.class );
	}

	@Test
	public void testConcurrentPersistWithStages(VertxTestContext context) {
		test( context, getSessionFactory().withSession( s -> {
			for ( int i = 0; i < ENTITY_COUNT; i++ ) {
				s.persist( new SimpleEntity( "entity-" + i ) );
				s.createNativeQuery( "select pg_sleep(0.001)" ).getResultList();
			}
			return s.flush()
					.thenCompose( $ -> s.createSelectionQuery(
							"select count(*) from SimpleEntity", Long.class
					).getSingleResult() )
					.thenAccept( count -> assertEquals(
							(long) ENTITY_COUNT, count,
							"All persisted entities should be in the database"
					) );
		} ) );
	}

	@Test
	public void testConcurrentPersistWithMutiny(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( s -> {
			for ( int i = 0; i < ENTITY_COUNT; i++ ) {
				s.persist( new SimpleEntity( "entity-" + i ) );
				s.createNativeQuery( "select pg_sleep(0.001)" ).getResultList();
			}
			return s.flush()
					.chain( () -> s.createSelectionQuery(
							"select count(*) from SimpleEntity", Long.class
					).getSingleResult() )
					.invoke( count -> assertEquals(
							(long) ENTITY_COUNT, count,
							"All persisted entities should be in the database"
					) );
		} ) );
	}

	@Test
	public void testAllOfWithStages(VertxTestContext context) {
		test( context, getSessionFactory().withSession( s -> {
			CompletableFuture<?>[] futures = new CompletableFuture[ENTITY_COUNT];
			for ( int i = 0; i < ENTITY_COUNT; i++ ) {
				futures[i] = s.persist( new SimpleEntity( "entity-" + i ) )
						.toCompletableFuture();
				s.createNativeQuery( "select pg_sleep(0.001)" ).getResultList();
			}
			return CompletableFuture.allOf( futures )
					.thenCompose( $ -> s.flush() )
					.thenCompose( $ -> s.createSelectionQuery(
							"select count(*) from SimpleEntity", Long.class
					).getSingleResult() )
					.thenAccept( count -> assertEquals(
							(long) ENTITY_COUNT, count,
							"All persisted entities should be in the database"
					) );
		} ) );
	}

	@Test
	public void testCombineAllWithMutiny(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( s -> {
			List<Uni<Void>> unis = new ArrayList<>();
			for ( int i = 0; i < ENTITY_COUNT; i++ ) {
				unis.add( s.persist( new SimpleEntity( "entity-" + i ) ) );
				s.createNativeQuery( "select pg_sleep(0.001)" ).getResultList();
			}
			return Uni.combine().all().unis( unis ).discardItems()
					.call( s::flush )
					.chain( () -> s.createSelectionQuery(
							"select count(*) from SimpleEntity", Long.class
					).getSingleResult() )
					.invoke( count -> assertEquals(
							(long) ENTITY_COUNT, count,
							"All persisted entities should be in the database"
					) );
		} ) );
	}

	@Test
	public void testCascadingPersistWithStages(VertxTestContext context) {
		test( context, getSessionFactory().withSession( s -> {
			for ( int i = 0; i < ENTITY_COUNT; i++ ) {
				Author author = new Author( "cascade-stage-" + i );
				author.addBook( new Book( "cs-book-" + i + "-a" ) );
				author.addBook( new Book( "cs-book-" + i + "-b" ) );
				s.persist( author );
				s.createNativeQuery( "select pg_sleep(0.001)" ).getResultList();
			}
			return s.flush()
					.thenCompose( v -> s.createSelectionQuery(
							"select count(*) from SSAuthor where name like 'cascade-stage-%'", Long.class
					).getSingleResult() )
					.thenAccept( count -> assertEquals( ENTITY_COUNT, count.intValue() ) );
		} ) );
	}

	@Test
	public void testCascadingPersistWithMutiny(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( s -> {
			for ( int i = 0; i < ENTITY_COUNT; i++ ) {
				Author author = new Author( "cascade-mutiny-" + i );
				author.addBook( new Book( "cm-book-" + i + "-a" ) );
				author.addBook( new Book( "cm-book-" + i + "-b" ) );
				s.persist( author );
				s.createNativeQuery( "select pg_sleep(0.001)" ).getResultList();
			}
			return s.flush()
					.chain( () -> s.createSelectionQuery(
							"select count(*) from SSAuthor where name like 'cascade-mutiny-%'", Long.class
					).getSingleResult() )
					.invoke( count -> assertEquals( ENTITY_COUNT, count.intValue() ) );
		} ) );
	}

	@Test
	public void testFetchLazyCollectionWithStages(VertxTestContext context) {
		Author author = new Author( "fetch-stage-author" );
		for ( int i = 0; i < 10; i++ ) {
			author.addBook( new Book( "fetch-stage-book-" + i ) );
		}
		test( context, getSessionFactory()
				.withSession( s -> s.persist( author ).thenCompose( $ -> s.flush() ) )
				.thenCompose( $ -> getSessionFactory().withSession( s ->
						s.find( Author.class, author.id )
								.thenCompose( found -> {
									assertNotNull( found );
									assertFalse( Hibernate.isInitialized( found.books ) );
									s.fetch( found.books );
									s.createNativeQuery( "select pg_sleep(0.001)" ).getResultList();
									return s.flush()
											.thenAccept( v -> {
												assertTrue( Hibernate.isInitialized( found.books ) );
												assertEquals( 10, found.books.size() );
											} );
								} )
				) )
		);
	}

	@Test
	public void testFetchLazyCollectionWithMutiny(VertxTestContext context) {
		Author author = new Author( "fetch-mutiny-author" );
		for ( int i = 0; i < 10; i++ ) {
			author.addBook( new Book( "fetch-mutiny-book-" + i ) );
		}
		test( context, getMutinySessionFactory()
				.withSession( s -> s.persist( author ).call( s::flush ) )
				.chain( () -> getMutinySessionFactory().withSession( s ->
						s.find( Author.class, author.id )
								.chain( found -> {
									assertNotNull( found );
									assertFalse( Hibernate.isInitialized( found.books ) );
									return s.fetch( found.books )
											.invoke( books -> assertEquals( 10, books.size() ) );
								} )
				) )
		);
	}

	@Test
	public void testRemoveWithCascadeStages(VertxTestContext context) {
		test( context, getSessionFactory()
				.withSession( s -> {
					for ( int i = 0; i < ENTITY_COUNT; i++ ) {
						Author author = new Author( "remove-author-" + i );
						author.addBook( new Book( "remove-book-" + i + "-a" ) );
						author.addBook( new Book( "remove-book-" + i + "-b" ) );
						s.persist( author );
					}
					return s.flush();
				} )
				.thenCompose( $ -> getSessionFactory().withSession( s ->
						s.createSelectionQuery( "from SSAuthor", Author.class ).getResultList()
								.thenCompose( authors -> {
									for ( Author a : authors ) {
										s.remove( a );
										s.createNativeQuery( "select pg_sleep(0.001)" ).getResultList();
									}
									return s.flush();
								} )
								.thenCompose( $2 -> s.createSelectionQuery(
										"select count(*) from SSAuthor", Long.class
								).getSingleResult() )
								.thenAccept( count -> assertEquals(
										0L, count,
										"All authors should be removed"
								) )
								.thenCompose( $2 -> s.createSelectionQuery(
										"select count(*) from SSBook", Long.class
								).getSingleResult() )
								.thenAccept( count -> assertEquals(
										0L, count,
										"All books should be cascade-removed"
								) )
				) )
		);
	}

	@Test
	public void testRemoveWithCascadeMutiny(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withSession( s -> {
					for ( int i = 0; i < ENTITY_COUNT; i++ ) {
						Author author = new Author( "remove-author-" + i );
						author.addBook( new Book( "remove-book-" + i + "-a" ) );
						author.addBook( new Book( "remove-book-" + i + "-b" ) );
						s.persist( author );
					}
					return s.flush();
				} )
				.chain( () -> getMutinySessionFactory().withSession( s ->
						s.createSelectionQuery( "from SSAuthor", Author.class ).getResultList()
								.chain( authors -> {
									for ( Author a : authors ) {
										s.remove( a );
										s.createNativeQuery( "select pg_sleep(0.001)" ).getResultList();
									}
									return s.flush();
								} )
								.chain( () -> s.createSelectionQuery(
										"select count(*) from SSAuthor", Long.class
								).getSingleResult() )
								.invoke( count -> assertEquals(
										0L, count,
										"All authors should be removed"
								) )
								.chain( () -> s.createSelectionQuery(
										"select count(*) from SSBook", Long.class
								).getSingleResult() )
								.invoke( count -> assertEquals(
										0L, count,
										"All books should be cascade-removed"
								) )
				) )
		);
	}

	@Test
	public void testMergeWithCollectionModificationStages(VertxTestContext context) {
		Author author = new Author( "merge-author" );
		author.addBook( new Book( "original-book" ) );
		test( context, getSessionFactory()
				.withSession( s -> s.persist( author ).thenCompose( $ -> s.flush() ) )
				.thenCompose( $ -> getSessionFactory().withSession( s -> {
					author.name = "updated-author";
					author.addBook( new Book( "new-book-1" ) );
					author.addBook( new Book( "new-book-2" ) );
					s.merge( author );
					s.createNativeQuery( "select pg_sleep(0.001)" ).getResultList();
					return s.flush();
				} ) )
				.thenCompose( $ -> getSessionFactory().withSession( s ->
						s.find( Author.class, author.id )
								.thenCompose( found -> s.fetch( found.books )
										.thenAccept( books -> {
											assertEquals( "updated-author", found.name );
											assertEquals( 3, books.size() );
										} )
								)
				) )
		);
	}

	@Test
	public void testMergeWithCollectionModificationMutiny(VertxTestContext context) {
		Author author = new Author( "merge-author" );
		author.addBook( new Book( "original-book" ) );
		test( context, getMutinySessionFactory()
				.withSession( s -> s.persist( author ).call( s::flush ) )
				.chain( () -> getMutinySessionFactory().withSession( s -> {
					author.name = "updated-author";
					author.addBook( new Book( "new-book-1" ) );
					author.addBook( new Book( "new-book-2" ) );
					s.merge( author );
					s.createNativeQuery( "select pg_sleep(0.001)" ).getResultList();
					return s.flush();
				} ) )
				.chain( () -> getMutinySessionFactory().withSession( s ->
						s.find( Author.class, author.id )
								.chain( found -> s.fetch( found.books )
										.invoke( books -> {
											assertEquals( "updated-author", found.name );
											assertEquals( 3, books.size() );
										} )
								)
				) )
		);
	}

	@Test
	public void testConcurrentQueriesWithStages(VertxTestContext context) {
		test( context, getSessionFactory()
				.withSession( s -> {
					for ( int i = 0; i < 10; i++ ) {
						s.persist( new SimpleEntity( "query-stage-" + i ) );
					}
					return s.flush();
				} )
				.thenCompose( $ -> getSessionFactory().withSession( s -> {
					CompletableFuture<?>[] futures = new CompletableFuture[ENTITY_COUNT];
					for ( int i = 0; i < ENTITY_COUNT; i++ ) {
						if ( i % 3 == 0 ) {
							futures[i] = s.createSelectionQuery(
									"from SimpleEntity where name like 'query-stage-%'", SimpleEntity.class
							).getResultList().toCompletableFuture();
						}
						else if ( i % 3 == 1 ) {
							futures[i] = s.createNativeQuery(
									"select * from simple_entity where name like 'query-stage-%'"
							).getResultList().toCompletableFuture();
						}
						else {
							futures[i] = s.createSelectionQuery(
									"select count(*) from SimpleEntity where name like 'query-stage-%'", Long.class
							).getSingleResult().toCompletableFuture();
						}
					}
					return CompletableFuture.allOf( futures );
				} ) )
		);
	}

	@Test
	public void testConcurrentQueriesWithMutiny(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withSession( s -> {
					for ( int i = 0; i < 10; i++ ) {
						s.persist( new SimpleEntity( "query-mutiny-" + i ) );
					}
					return s.flush();
				} )
				.chain( () -> getMutinySessionFactory().withSession( s -> {
					List<Uni<?>> unis = new ArrayList<>();
					for ( int i = 0; i < ENTITY_COUNT; i++ ) {
						if ( i % 3 == 0 ) {
							unis.add( s.createSelectionQuery(
									"from SimpleEntity where name like 'query-mutiny-%'", SimpleEntity.class
							).getResultList() );
						}
						else if ( i % 3 == 1 ) {
							unis.add( s.createNativeQuery(
									"select * from simple_entity where name like 'query-mutiny-%'"
							).getResultList() );
						}
						else {
							unis.add( s.createSelectionQuery(
									"select count(*) from SimpleEntity where name like 'query-mutiny-%'", Long.class
							).getSingleResult() );
						}
					}
					return Uni.combine().all().unis( unis ).discardItems();
				} ) )
		);
	}

	@Test
	public void testMixedPersistAndQueriesWithStages(VertxTestContext context) {
		test( context, getSessionFactory().withSession( s -> {
			for ( int i = 0; i < ENTITY_COUNT; i++ ) {
				s.persist( new SimpleEntity( "mixed-stage-" + i ) );
				s.createNativeQuery( "select pg_sleep(0.001)" ).getResultList();
				s.createSelectionQuery(
						"select count(*) from SimpleEntity where name like 'mixed-stage-%'", Long.class
				).getSingleResult();
			}
			return s.flush()
					.thenCompose( v -> s.createSelectionQuery(
							"select count(*) from SimpleEntity where name like 'mixed-stage-%'", Long.class
					).getSingleResult() )
					.thenAccept( count -> assertEquals(
							(long) ENTITY_COUNT, count,
							"All persisted entities should be in the database"
					) );
		} ) );
	}

	@Test
	public void testMixedPersistAndQueriesWithMutiny(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( s -> {
			for ( int i = 0; i < ENTITY_COUNT; i++ ) {
				s.persist( new SimpleEntity( "mixed-mutiny-" + i ) );
				s.createNativeQuery( "select pg_sleep(0.001)" ).getResultList();
				s.createSelectionQuery(
						"select count(*) from SimpleEntity where name like 'mixed-mutiny-%'", Long.class
				).getSingleResult();
			}
			return s.flush()
					.chain( () -> s.createSelectionQuery(
							"select count(*) from SimpleEntity where name like 'mixed-mutiny-%'", Long.class
					).getSingleResult() )
					.invoke( count -> assertEquals(
							(long) ENTITY_COUNT, count,
							"All persisted entities should be in the database"
					) );
		} ) );
	}

	@Entity(name = "SimpleEntity")
	@Table(name = "SIMPLE_ENTITY")
	static class SimpleEntity {
		@Id
		@GeneratedValue
		Long id;

		String name;

		SimpleEntity() {
		}

		SimpleEntity(String name) {
			this.name = name;
		}
	}

	@Entity(name = "SSAuthor")
	@Table(name = "SS_AUTHOR")
	static class Author {
		@Id
		@GeneratedValue
		Long id;

		String name;

		@OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY, mappedBy = "author")
		List<Book> books = new ArrayList<>();

		Author() {
		}

		Author(String name) {
			this.name = name;
		}

		void addBook(Book book) {
			books.add( book );
			book.author = this;
		}
	}

	@Entity(name = "SSBook")
	@Table(name = "SS_BOOK")
	static class Book {
		@Id
		@GeneratedValue
		Long id;

		String title;

		@ManyToOne(fetch = FetchType.LAZY)
		Author author;

		Book() {
		}

		Book(String title) {
			this.title = title;
		}
	}
}
