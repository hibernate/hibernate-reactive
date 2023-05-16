/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.hibernate.Hibernate;
import org.hibernate.LazyInitializationException;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.stage.Stage;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
/**
 * We expect to throw the right exception when a lazy initialization error happens.
 *
 * @see LazyInitializationException
 */
public class LazyInitializationExceptionTest extends BaseReactiveTest {

	private Artist artemisia;

	private Painting sev;

	private Painting liuto;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Painting.class, Artist.class );
	}

	private Artist getArtist() {
		if( artemisia == null ) {
			artemisia = new Artist( "Artemisia Gentileschi" );
			sev = new Painting();
			sev.setAuthor( artemisia );
			sev.setName( "Susanna e i vecchioni" );
			liuto = new Painting();
			liuto.setAuthor( artemisia );
			liuto.setName( "Autoritratto come suonatrice di liuto" );
		}
		return artemisia;
	}

	private Uni<Void> populateDbMutiny() {
		return getMutinySessionFactory().withTransaction( (s, t) -> s.persistAll( getArtist(), sev, liuto) );
	}

	private CompletionStage<Void> populateDb() {
		return getSessionFactory().withTransaction( s -> s.persist(  getArtist(), sev, liuto) );
	}

	@Test
	public void testLazyInitializationExceptionWithMutiny(VertxTestContext context) {
		test( context, assertThrown( LazyInitializationException.class, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( ms -> ms.createQuery( "from Artist", Artist.class ).getSingleResult() )
				.invoke( artist -> artist.getPaintings().size() ) ) )
				.invoke( LazyInitializationExceptionTest::assertLazyInitialization )
		);
	}

	@Test
	public void testLazyInitializationExceptionWithStage(VertxTestContext context) {
		test( context, assertThrown( LazyInitializationException.class, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( ss -> ss.createQuery( "from Artist", Artist.class ).getSingleResult() ))
				.thenAccept( artist -> artist.getPaintings().size() ) )
				.thenAccept( LazyInitializationExceptionTest::assertLazyInitialization )
		);
	}

	private static void assertLazyInitialization(LazyInitializationException e ) {
		assertTrue( e.getMessage()
							.startsWith( "HR000056: Collection cannot be initialized: " + Artist.class.getName() + ".paintings" ) );
	}

	@Test
	public void testLazyInitializationExceptionNotThrownWithMutiny(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session.createQuery( "from Artist", Artist.class ).getSingleResult() )
				// We are checking `.getPaintings()` but not doing anything with it and therefore it should work.
				.invoke( Artist::getPaintings) )
		);
	}

	@Test
	public void testLazyInitializationExceptionNotThrownWithStage(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.createQuery( "from Artist", Artist.class ).getSingleResult() ) )
				// We are checking `.getPaintings()` but not doing anything with it and therefore it should work.
				.thenAccept( Artist::getPaintings )
		);
	}

	@Test
	public void testLazyInitializationWithJoinFetchAndMutiny(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session.createQuery( "from Artist a join fetch a.paintings", Artist.class ).getSingleResult() )
				.onItem().invoke( artist -> {
					assertTrue( Hibernate.isInitialized( artist ) );
					assertEquals( 2, artist.getPaintings().size() );
				} ) ) );
	}

	@Test
	public void testLazyInitializationWithJoinFetch(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session
						.createQuery( "from Artist a join fetch a.paintings", Artist.class )
						.getSingleResult() ) )
				.thenAccept( artist -> {
					assertTrue( Hibernate.isInitialized( artist.paintings ) );
					assertEquals( 2, artist.getPaintings().size() );
				} ) );
	}

	@Test
	public void testLazyInitializationWithMutinyFetch(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> openMutinySession()
				.chain( session -> session.createQuery( "from Artist", Artist.class ).getSingleResult()
				.chain( artist -> Mutiny.fetch( artist.paintings ) ) )
						.invoke( paintings -> {
							assertTrue( Hibernate.isInitialized( paintings ) );
							assertEquals( 2, paintings.size() );
						} )
				)
		);
	}

	@Test
	public void testLazyInitializationWithStageFetch(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( vd -> openSession()
				.thenCompose( session -> session.createQuery( "from Artist", Artist.class ).getSingleResult() ) )
				.thenCompose( artist -> Stage.fetch( artist.paintings )
						.thenAccept( paintings -> {
							assertTrue( Hibernate.isInitialized( paintings ) );
							assertEquals( 2, paintings.size() );
						} )
				)
		);
	}

	@Entity(name = "Painting")
	@Table(name = "painting")
	public static class Painting {
		@Id
		@GeneratedValue(strategy = GenerationType.AUTO)
		Long id;

		String name;

		@ManyToOne(fetch = FetchType.LAZY)
		@JoinColumn(name = "author_id", nullable = false, referencedColumnName = "id")
		Artist author;

		public Painting() {
		}

		public Painting(String name) {
			this.name = name;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Artist getAuthor() {
			return author;
		}

		public void setAuthor(Artist author) {
			this.author = author;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Painting city = (Painting) o;
			return Objects.equals( this.name, city.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name, name );
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append( id );
			builder.append( ':' );
			builder.append( name );
			builder.append( ':' );
			builder.append( author );
			return builder.toString();
		}
	}

	@Entity(name = "Artist")
	@Table(name = "artist")
	public static class Artist {

		@Id
		@GeneratedValue(strategy = GenerationType.AUTO)
		private Long id;

		private String name;

		@OneToMany(mappedBy = "author", fetch = FetchType.LAZY)
		private List<Painting> paintings;

		public Artist() {
		}

		public Artist(String name) {
			this.name = name;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public List<Painting> getPaintings() {
			return paintings;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Artist person = (Artist) o;
			return Objects.equals( name, person.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append( id );
			builder.append( ':' );
			builder.append( name );
			return builder.toString();
		}
	}
}
