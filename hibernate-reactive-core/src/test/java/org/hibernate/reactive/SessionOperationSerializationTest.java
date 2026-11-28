/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.hibernate.FlushMode;
import org.hibernate.Hibernate;

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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(value = 10, timeUnit = MINUTES)
public class SessionOperationSerializationTest extends BaseReactiveTest {

	private static final int ENTITY_COUNT = 100;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( SimpleEntity.class, Author.class, Book.class );
	}

	// Fire-and-forget persist: Stage API starts work eagerly,
	// so discarding the CompletionStage is fine — enqueue() queues it.
	@Test
	public void testConcurrentPersistWithStages(VertxTestContext context) {
		test( context, getSessionFactory().withSession( s -> {
			for ( int i = 0; i < ENTITY_COUNT; i++ ) {
				s.persist( new SimpleEntity( "entity-" + i ) );
			}
			return s.flush()
					.thenCompose( $ -> s.createSelectionQuery(
							"select count(*) from SimpleEntity", Long.class
					).getSingleResult() )
					.thenAccept( count -> assertThat( count )
							.as( "All persisted entities should be in the database" )
							.isEqualTo( (long) ENTITY_COUNT ) );
		} ) );
	}

	// Mutiny is lazy: Uni must be subscribed. Use Uni.combine() to subscribe all.
	@Test
	public void testCombineAllWithMutiny(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( s -> {
			List<Uni<Void>> unis = new ArrayList<>();
			for ( int i = 0; i < ENTITY_COUNT; i++ ) {
				unis.add( s.persist( new SimpleEntity( "entity-" + i ) ) );
			}
			return Uni.combine().all().unis( unis ).discardItems()
					.call( s::flush )
					.chain( () -> s.createSelectionQuery(
							"select count(*) from SimpleEntity", Long.class
					).getSingleResult() )
					.invoke( count -> assertThat( count )
							.as( "All persisted entities should be in the database" )
							.isEqualTo( (long) ENTITY_COUNT ) );
		} ) );
	}

	@Test
	public void testConcurrentPersistOfMultipleEntitiesWithStages(VertxTestContext context) {
		// Like testConcurrentPersistWithStages, but each call persists several entities
		test( context, getSessionFactory().withSession( s -> {
			for ( int i = 0; i < ENTITY_COUNT; i++ ) {
				s.persist( new SimpleEntity( "entity-" + i + "-a" ), new SimpleEntity( "entity-" + i + "-b" ) );
			}
			return s.flush()
					.thenCompose( $ -> s.createSelectionQuery(
							"select count(*) from SimpleEntity", Long.class
					).getSingleResult() )
					.thenAccept( count -> assertThat( count )
							.as( "All persisted entities should be in the database" )
							.isEqualTo( 2L * ENTITY_COUNT ) );
		} ) );
	}

	@Test
	public void testCombineAllPersistOfMultipleEntitiesWithMutiny(VertxTestContext context) {
		// Like testCombineAllWithMutiny, but each call persists several entities
		test( context, getMutinySessionFactory().withSession( s -> {
			List<Uni<Void>> unis = new ArrayList<>();
			for ( int i = 0; i < ENTITY_COUNT; i++ ) {
				unis.add( s.persistAll( new SimpleEntity( "entity-" + i + "-a" ), new SimpleEntity( "entity-" + i + "-b" ) ) );
			}
			return Uni.combine().all().unis( unis ).discardItems()
					.call( s::flush )
					.chain( () -> s.createSelectionQuery(
							"select count(*) from SimpleEntity", Long.class
					).getSingleResult() )
					.invoke( count -> assertThat( count )
							.as( "All persisted entities should be in the database" )
							.isEqualTo( 2L * ENTITY_COUNT ) );
		} ) );
	}

	@Test
	public void testAllOfWithStages(VertxTestContext context) {
		test( context, getSessionFactory().withSession( s -> {
			CompletableFuture<?>[] futures = new CompletableFuture[ENTITY_COUNT];
			for ( int i = 0; i < ENTITY_COUNT; i++ ) {
				futures[i] = s.persist( new SimpleEntity( "entity-" + i ) )
						.toCompletableFuture();
			}
			return CompletableFuture.allOf( futures )
					.thenCompose( $ -> s.flush() )
					.thenCompose( $ -> s.createSelectionQuery(
							"select count(*) from SimpleEntity", Long.class
					).getSingleResult() )
					.thenAccept( count -> assertThat( count )
							.as( "All persisted entities should be in the database" )
							.isEqualTo( (long) ENTITY_COUNT ) );
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
			}
			return s.flush()
					.thenCompose( v -> s.createSelectionQuery(
							"select count(*) from SSAuthor where name like 'cascade-stage-%'", Long.class
					).getSingleResult() )
					.thenAccept( count -> assertThat( count.intValue() ).isEqualTo( ENTITY_COUNT ) );
		} ) );
	}

	@Test
	public void testCascadingPersistWithMutiny(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( s -> {
			List<Uni<Void>> unis = new ArrayList<>();
			for ( int i = 0; i < ENTITY_COUNT; i++ ) {
				Author author = new Author( "cascade-mutiny-" + i );
				author.addBook( new Book( "cm-book-" + i + "-a" ) );
				author.addBook( new Book( "cm-book-" + i + "-b" ) );
				unis.add( s.persist( author ) );
			}
			return Uni.combine().all().unis( unis ).discardItems()
					.call( s::flush )
					.chain( () -> s.createSelectionQuery(
							"select count(*) from SSAuthor where name like 'cascade-mutiny-%'", Long.class
					).getSingleResult() )
					.invoke( count -> assertThat( count.intValue() ).isEqualTo( ENTITY_COUNT ) );
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
									assertThat( found ).isEqualTo( author );
									assertThat( Hibernate.isInitialized( found.books ) ).isFalse();
									return s.fetch( found.books )
											.thenAccept( books -> {
												assertThat( Hibernate.isInitialized( found.books ) ).isTrue();
												assertThat( books ).containsExactlyInAnyOrderElementsOf( author.books );
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
									assertThat( found ).isEqualTo( author );
									assertThat( Hibernate.isInitialized( found.books ) ).isFalse();
									return s.fetch( found.books )
											.invoke( books -> assertThat( books )
													.containsExactlyInAnyOrderElementsOf( author.books ) );
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
									}
									return s.flush();
								} )
								.thenCompose( $2 -> s.createSelectionQuery(
										"select count(*) from SSAuthor", Long.class
								).getSingleResult() )
								.thenAccept( count -> assertThat( count )
										.as( "All authors should be removed" )
										.isEqualTo( 0L ) )
								.thenCompose( $2 -> s.createSelectionQuery(
										"select count(*) from SSBook", Long.class
								).getSingleResult() )
								.thenAccept( count -> assertThat( count )
										.as( "All books should be cascade-removed" )
										.isEqualTo( 0L ) )
				) )
		);
	}

	@Test
	public void testRemoveWithCascadeMutiny(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withSession( s -> {
					List<Uni<Void>> unis = new ArrayList<>();
					for ( int i = 0; i < ENTITY_COUNT; i++ ) {
						Author author = new Author( "remove-author-" + i );
						author.addBook( new Book( "remove-book-" + i + "-a" ) );
						author.addBook( new Book( "remove-book-" + i + "-b" ) );
						unis.add( s.persist( author ) );
					}
					return Uni.combine().all().unis( unis ).discardItems()
							.call( s::flush );
				} )
				.chain( () -> getMutinySessionFactory().withSession( s ->
						s.createSelectionQuery( "from SSAuthor", Author.class ).getResultList()
								.chain( authors -> {
									List<Uni<Void>> removeUnis = new ArrayList<>();
									for ( Author a : authors ) {
										removeUnis.add( s.remove( a ) );
									}
									return Uni.combine().all().unis( removeUnis ).discardItems();
								} )
								.call( s::flush )
								.chain( () -> s.createSelectionQuery(
										"select count(*) from SSAuthor", Long.class
								).getSingleResult() )
								.invoke( count -> assertThat( count )
										.as( "All authors should be removed" )
										.isEqualTo( 0L ) )
								.chain( () -> s.createSelectionQuery(
										"select count(*) from SSBook", Long.class
								).getSingleResult() )
								.invoke( count -> assertThat( count )
										.as( "All books should be cascade-removed" )
										.isEqualTo( 0L ) )
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
					return s.merge( author ).thenCompose( $2 -> s.flush() );
				} ) )
				.thenCompose( $ -> getSessionFactory().withSession( s ->
						s.find( Author.class, author.id )
								.thenCompose( found -> s.fetch( found.books )
										.thenAccept( books -> {
											assertThat( found ).isEqualTo( author );
											assertThat( books ).containsExactlyInAnyOrderElementsOf( author.books );
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
					return s.merge( author ).call( s::flush );
				} ) )
				.chain( () -> getMutinySessionFactory().withSession( s ->
						s.find( Author.class, author.id )
								.chain( found -> s.fetch( found.books )
										.invoke( books -> {
											assertThat( found ).isEqualTo( author );
											assertThat( books ).containsExactlyInAnyOrderElementsOf( author.books );
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
						if ( i % 2 == 0 ) {
							futures[i] = s.createSelectionQuery(
									"from SimpleEntity where name like 'query-stage-%'", SimpleEntity.class
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
					List<Uni<Void>> unis = new ArrayList<>();
					for ( int i = 0; i < 10; i++ ) {
						unis.add( s.persist( new SimpleEntity( "query-mutiny-" + i ) ) );
					}
					return Uni.combine().all().unis( unis ).discardItems()
							.call( s::flush );
				} )
				.chain( () -> getMutinySessionFactory().withSession( s -> {
					List<Uni<?>> queryUnis = new ArrayList<>();
					for ( int i = 0; i < ENTITY_COUNT; i++ ) {
						if ( i % 2 == 0 ) {
							queryUnis.add( s.createSelectionQuery(
									"from SimpleEntity where name like 'query-mutiny-%'", SimpleEntity.class
							).getResultList() );
						}
						else {
							queryUnis.add( s.createSelectionQuery(
									"select count(*) from SimpleEntity where name like 'query-mutiny-%'", Long.class
							).getSingleResult() );
						}
					}
					return Uni.combine().all().unis( queryUnis ).discardItems();
				} ) )
		);
	}

	@Test
	public void testMixedPersistAndQueriesWithStages(VertxTestContext context) {
		test( context, getSessionFactory().withSession( s -> {
			for ( int i = 0; i < ENTITY_COUNT; i++ ) {
				s.persist( new SimpleEntity( "mixed-stage-" + i ) );
				s.createSelectionQuery(
						"select count(*) from SimpleEntity where name like 'mixed-stage-%'", Long.class
				).getSingleResult();
			}
			return s.flush()
					.thenCompose( v -> s.createSelectionQuery(
							"select count(*) from SimpleEntity where name like 'mixed-stage-%'", Long.class
					).getSingleResult() )
					.thenAccept( count -> assertThat( count )
							.as( "All persisted entities should be in the database" )
							.isEqualTo( (long) ENTITY_COUNT ) );
		} ) );
	}

	@Test
	public void testMixedPersistAndQueriesWithMutiny(VertxTestContext context) {
		test( context, getMutinySessionFactory().withSession( s -> {
			List<Uni<?>> unis = new ArrayList<>();
			for ( int i = 0; i < ENTITY_COUNT; i++ ) {
				unis.add( s.persist( new SimpleEntity( "mixed-mutiny-" + i ) ) );
				unis.add( s.createSelectionQuery(
						"select count(*) from SimpleEntity where name like 'mixed-mutiny-%'", Long.class
				).getSingleResult() );
			}
			return Uni.combine().all().unis( unis ).discardItems()
					.call( s::flush )
					.chain( () -> s.createSelectionQuery(
							"select count(*) from SimpleEntity where name like 'mixed-mutiny-%'", Long.class
					).getSingleResult() )
					.invoke( count -> assertThat( count )
							.as( "All persisted entities should be in the database" )
							.isEqualTo( (long) ENTITY_COUNT ) );
		} ) );
	}

	@Test
	public void testNativeQueryAfterPersistWithStages(VertxTestContext context) {
		// FlushMode.ALWAYS ensures that native queries trigger a flush,
		// which would deadlock if the flush re-enters the serialization queue
		test( context, getSessionFactory().withTransaction( s -> {
			s.setFlushMode( FlushMode.ALWAYS );
			s.persist( new SimpleEntity( "native-stage" ) );
			return s.createNativeQuery( "select count(*) from SIMPLE_ENTITY where name = 'native-stage'", Long.class )
					.getSingleResult()
					.thenAccept( count -> assertThat( count )
							.as( "Native query should see the flushed entity" )
							.isEqualTo( 1L ) );
		} ) );
	}

	@Test
	public void testNativeQueryAfterPersistWithMutiny(VertxTestContext context) {
		test( context, getMutinySessionFactory().withTransaction( s -> {
			s.setFlushMode( FlushMode.ALWAYS );
			return s.persist( new SimpleEntity( "native-mutiny" ) )
						.chain( () -> s.createNativeQuery(
								"select count(*) from SIMPLE_ENTITY where name = 'native-mutiny'", Long.class
						).getSingleResult() )
						.invoke( count -> assertThat( count )
								.as( "Native query should see the flushed entity" )
								.isEqualTo( 1L ) );
		} ) );
	}

	@Test
	public void testOperationOnClosedSessionThrowsImmediately(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( s -> s.close()
						.thenAccept( v -> {
							// The exception is thrown by the method, not reported in the returned stage
							assertThatThrownBy( () -> s.persist( new SimpleEntity( "closed" ) ) )
									.isInstanceOf( IllegalStateException.class );
							assertThatThrownBy( s::flush )
									.isInstanceOf( IllegalStateException.class );
							assertThatThrownBy( () -> s.find( SimpleEntity.class, 1L, 2L ) )
									.isInstanceOf( IllegalStateException.class );
						} )
				)
		);
	}

	@Test
	public void testOperationOnClosedStatelessSessionThrowsImmediately(VertxTestContext context) {
		test( context, openStatelessSession()
				.thenCompose( s -> s.close()
						.thenAccept( v -> {
							// The exception is thrown by the method, not reported in the returned stage
							assertThatThrownBy( () -> s.insert( new SimpleEntity( "closed" ) ) )
									.isInstanceOf( IllegalStateException.class );
							assertThatThrownBy( () -> s.get( SimpleEntity.class, 1L ) )
									.isInstanceOf( IllegalStateException.class );
							assertThatThrownBy( () -> s.refresh( new SimpleEntity( "closed" ) ) )
									.isInstanceOf( IllegalStateException.class );
						} )
				)
		);
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

		@Override
		public boolean equals(Object o) {
			return o instanceof SimpleEntity that && Objects.equals( name, that.name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
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

		@Override
		public boolean equals(Object o) {
			return o instanceof Author that && Objects.equals( name, that.name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
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

		@Override
		public boolean equals(Object o) {
			return o instanceof Book that && Objects.equals( title, that.title );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( title );
		}
	}
}
