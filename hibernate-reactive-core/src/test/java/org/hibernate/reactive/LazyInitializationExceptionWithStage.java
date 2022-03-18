/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.hibernate.LazyInitializationException;

import org.junit.Before;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;


public class LazyInitializationExceptionWithStage extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Artist.class, Painting.class );
	}

	@Before
	public void populateDB(TestContext context) {
		Artist artemisia = new Artist( "Artemisia Gentileschi" );
		test( context, getSessionFactory().withTransaction( (session, tx) -> session.persist( artemisia ) ) );
	}

	@Test
	public void testLazyInitializationException(TestContext context) {
		test( context, openSession()
				.thenCompose( session ->
				  	session.createQuery( "from Artist", Artist.class )
						.getSingleResult()
							.thenAccept( artist -> artist.getPaintings().size() )
							.handle( (ignore, throwable) -> {
								if (throwable == null ) {
									context.fail( "Unexpected success, we expect "
														  + LazyInitializationException.class.getName() );
								}
								else {
									context.assertEquals( CompletionException.class, throwable.getClass() );
									context.assertEquals( LazyInitializationException.class, throwable.getCause().getClass() );
									context.assertEquals(
											"org.hibernate.LazyInitializationException: HR000056: Collection cannot be initialized: org.hibernate.reactive.LazyInitializationExceptionWithStage$Artist.paintings",
											throwable.getMessage() );
								}
								return null;
							} )
				) );
	}

	@Test
	public void testLazyInitializationExceptionNotThrown(TestContext context) {
		test( context, openSession()
			.thenCompose( session -> session.createQuery( "from Artist", Artist.class ).getSingleResult() )
			 // We are checking `.getPaintings()` but not doing anything with it and therefore it should work.
			.thenAccept( Artist::getPaintings )
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

		public void setPaintings(List<Painting> paintings) {
			this.paintings = paintings;
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
