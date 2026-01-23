/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.hibernate.annotations.SoftDelete;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Inheritance;
import jakarta.persistence.InheritanceType;
import jakarta.persistence.Table;

import org.hibernate.reactive.util.impl.CompletionStages;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

/**
 * Tests @SoftDelete annotation with JOINED inheritance strategy.
 */
public class SoftDeleteJoinedTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class, SpellBook.class, Novel.class );
	}

	@BeforeEach
	public void populateDB(VertxTestContext context) {
		SpellBook spellBook1 = new SpellBook( 1, "Necronomicon", true );
		SpellBook spellBook2 = new SpellBook( 2, "Book of Shadows", false );
		Novel novel = new Novel( 3, "The Hobbit", "Fantasy" );

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persistAll( spellBook1, spellBook2, novel ) )
		);
	}

	@Override
	protected CompletionStage<Void> cleanDb() {
		return getSessionFactory()
				.withTransaction( s -> s.createNativeQuery( "delete from SpellBookJoined" ).executeUpdate()
						.thenCompose( v -> s.createNativeQuery( "delete from NovelJoined" ).executeUpdate() )
						.thenCompose( v -> s.createNativeQuery( "delete from BookJoined" ).executeUpdate() )
						.thenCompose( CompletionStages::voidFuture ) );
	}

	@Test
	public void testSoftDeleteWithJoined(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				// Initially all books should be available
				.withSession( s -> s
						.createSelectionQuery( "from Book order by id", Book.class )
						.getResultList()
						.invoke( books -> assertThat( books ).hasSize( 3 ) )
				)
				// Delete the first spell book using HQL
				.call( () -> getMutinySessionFactory().withTransaction( s -> s
						.createMutationQuery( "delete from SpellBook where id = 1" )
						.executeUpdate()
				) )
				// After deletion, only 2 books should remain
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createSelectionQuery( "from Book order by id", Book.class )
						.getResultList()
						.invoke( books -> assertThat( books ).hasSize( 2 ) )
				) )
				// The deleted book should not be found
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.find( SpellBook.class, 1 )
						.invoke( book -> assertThat( book ).isNull() )
				) )
				// But it should exist in the native query (soft deleted)
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createNativeQuery( "select id, title, deleted from BookJoined order by id" )
						.getResultList()
						.invoke( rows -> {
							assertThat( rows ).hasSize( 3 );
							Object[] firstRow = (Object[]) rows.get( 0 );
							// Check that the first book is marked as deleted
							if ( dbType() == DB2 ) {
								assertThat( (short) firstRow[2] ).isEqualTo( (short) 1 );
							}
							else {
								assertThat( (boolean) firstRow[2] ).isTrue();
							}
						} )
				) )
		);
	}

	@Test
	public void testHQLDeleteWithJoined(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				// Delete using HQL
				.withTransaction( s -> s
						.createMutationQuery( "delete from SpellBook where forbidden = true" )
						.executeUpdate()
						.invoke( count -> assertThat( count ).isEqualTo( 1 ) )
				)
				// Verify only 2 books remain
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createSelectionQuery( "from Book order by id", Book.class )
						.getResultList()
						.invoke( books -> {
							assertThat( books ).hasSize( 2 );
							assertThat( books.get( 0 ).getId() ).isEqualTo( 2 );
							assertThat( books.get( 1 ).getId() ).isEqualTo( 3 );
						} )
				) )
		);
	}

	@Test
	public void testPolymorphicQuery(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				// Query for all SpellBooks
				.withSession( s -> s
						.createSelectionQuery( "from SpellBook order by id", SpellBook.class )
						.getResultList()
						.invoke( spellBooks -> assertThat( spellBooks ).hasSize( 2 ) )
				)
				// Delete one spell book
				.call( () -> getMutinySessionFactory().withTransaction( s -> s
						.createMutationQuery( "delete from SpellBook where id = 1" )
						.executeUpdate()
				) )
				// Query again - should have only 1 spell book
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createSelectionQuery( "from SpellBook order by id", SpellBook.class )
						.getResultList()
						.invoke( spellBooks -> {
							assertThat( spellBooks ).hasSize( 1 );
							assertThat( spellBooks.get( 0 ).getId() ).isEqualTo( 2 );
						} )
				) )
				// But querying Book should still return 2 (1 SpellBook + 1 Novel)
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createSelectionQuery( "from Book order by id", Book.class )
						.getResultList()
						.invoke( books -> assertThat( books ).hasSize( 2 ) )
				) )
		);
	}

	@Test
	public void testDeleteParentClass(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				// Delete using the parent class reference via HQL
				.withTransaction( s -> s
						.createMutationQuery( "delete from Book where id = 1" )
						.executeUpdate()
				)
				// Verify the book is soft deleted
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.find( Book.class, 1 )
						.invoke( book -> assertThat( book ).isNull() )
				) )
				// Verify through SpellBook entity as well
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.find( SpellBook.class, 1 )
						.invoke( book -> assertThat( book ).isNull() )
				) )
		);
	}

	@Entity(name = "Book")
	@Table(name = "BookJoined")
	@Inheritance(strategy = InheritanceType.JOINED)
	@SoftDelete
	public static class Book {
		@Id
		private Integer id;
		private String title;

		public Book() {
		}

		public Book(Integer id, String title) {
			this.id = id;
			this.title = title;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getTitle() {
			return title;
		}

		public void setTitle(String title) {
			this.title = title;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Book book = (Book) o;
			return Objects.equals( id, book.id ) && Objects.equals( title, book.title );
		}

		@Override
		public int hashCode() {
			return Objects.hash( id, title );
		}

		@Override
		public String toString() {
			return getClass().getSimpleName() + "{id=" + id + ", title='" + title + "'}";
		}
	}

	@Entity(name = "SpellBook")
	@Table(name = "SpellBookJoined")
	public static class SpellBook extends Book {
		private boolean forbidden;

		public SpellBook() {
		}

		public SpellBook(Integer id, String title, boolean forbidden) {
			super( id, title );
			this.forbidden = forbidden;
		}

		public boolean isForbidden() {
			return forbidden;
		}

		public void setForbidden(boolean forbidden) {
			this.forbidden = forbidden;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			if ( !super.equals( o ) ) {
				return false;
			}
			SpellBook spellBook = (SpellBook) o;
			return forbidden == spellBook.forbidden;
		}

		@Override
		public int hashCode() {
			return Objects.hash( super.hashCode(), forbidden );
		}
	}

	@Entity(name = "Novel")
	@Table(name = "NovelJoined")
	public static class Novel extends Book {
		private String genre;

		public Novel() {
		}

		public Novel(Integer id, String title, String genre) {
			super( id, title );
			this.genre = genre;
		}

		public String getGenre() {
			return genre;
		}

		public void setGenre(String genre) {
			this.genre = genre;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			if ( !super.equals( o ) ) {
				return false;
			}
			Novel novel = (Novel) o;
			return Objects.equals( genre, novel.genre );
		}

		@Override
		public int hashCode() {
			return Objects.hash( super.hashCode(), genre );
		}
	}
}
