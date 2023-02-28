/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

public class NonNullableManyToOneTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Painting.class, Artist.class, Dealer.class );
	}

	@Before
	public void populateDB(TestContext context) {
		Artist artist = new Artist( "Grand Master Painter" );
		artist.id = 1L;
		Dealer dealer = new Dealer( "Dealer" );
		dealer.id = 1L;
		Painting painting = new Painting( "Mona Lisa" );
		painting.id = 2L;
		artist.addPainting( painting );
		dealer.addPainting( painting );

		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persistAll( painting, artist, dealer ) ) );
	}

	@After
	public void cleanDb(TestContext context) {
		test( context, getSessionFactory()
				.withTransaction( s -> s.createQuery( "delete from Painting" ).executeUpdate()
						.thenCompose( v -> s.createQuery( "delete from Artist" ).executeUpdate() )
						.thenCompose( v -> s.createQuery( "delete from Dealer" ).executeUpdate())) );
	}

	@Test
	public void testNonNullableSuccess(TestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( session -> session
						.createQuery( "from Artist", Artist.class )
						.getSingleResult().chain( a -> session.fetch( a.getPaintings() ) )
						.invoke( paintings -> {
							context.assertNotNull( paintings );
							context.assertEquals( 1, paintings.size() );
							context.assertEquals( "Mona Lisa", paintings.get( 0 ).getName() );
						} ) )
				.chain( () -> getMutinySessionFactory()
						.withTransaction( s1 -> s1
								.createQuery( "from Dealer", Dealer.class )
								.getSingleResult().chain( d -> s1.fetch( d.getPaintings() ) )
								.invoke( paintings -> {
									context.assertNotNull( paintings );
									context.assertEquals( 1, paintings.size() );
									context.assertEquals( "Mona Lisa", paintings.get( 0 ).getName() );
								} )
						)
				)
		);
	}

	@Entity(name = "Painting")
	@Table(name = "painting")
	public static class Painting {
		@Id
		Long id;
		String name;

		@JoinColumn(nullable = false)
		@ManyToOne(optional = true)
		Artist author;

		@JoinColumn(nullable = true)
		@ManyToOne(optional = false)
		Dealer dealer;

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

		public Dealer getDealer() {
			return dealer;
		}

		public void setDealer(Dealer dealer) {
			this.dealer = dealer;
		}
	}

	@Entity(name = "Artist")
	@Table(name = "artist")
	public static class Artist {

		@Id
		Long id;
		String name;

		@OneToMany(mappedBy = "author")
		List<Painting> paintings = new ArrayList<>();

		public Artist() {
		}

		public Artist(String name) {
			this.name = name;
		}

		public void addPainting(Painting painting) {
			this.paintings.add( painting );
			painting.author = this;
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
	}

	@Entity(name = "Dealer")
	@Table(name = "dealer")
	public static class Dealer {

		@Id
		Long id;
		String name;

		@OneToMany(mappedBy = "dealer")
		List<Painting> paintings = new ArrayList<>();

		public Dealer() {
		}

		public Dealer(String name) {
			this.name = name;
		}

		public void addPainting(Painting painting) {
			this.paintings.add( painting );
			painting.dealer = this;
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
	}
}
