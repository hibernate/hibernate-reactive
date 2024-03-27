/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;



import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Timeout(value = 10, timeUnit = MINUTES)

public class NonNullableManyToOneTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Painting.class, Artist.class, Dealer.class );
	}

	@BeforeEach
	public void populateDB(VertxTestContext context) {
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

	@Override
	protected CompletionStage<Void> cleanDb() {
		// the default implementation is too basic to delete this mapping and, because there's only one test, it's
		// enough to drop the table at startup
		return voidFuture();
	}

	@Test
	public void testNonNullableSuccess(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( session -> session
						.createSelectionQuery( "from Artist", Artist.class )
						.getSingleResult().chain( a -> session.fetch( a.getPaintings() ) )
						.invoke( paintings -> {
							assertNotNull( paintings );
							assertEquals( 1, paintings.size() );
							assertEquals( "Mona Lisa", paintings.get( 0 ).getName() );
						} ) )
				.chain( () -> getMutinySessionFactory()
						.withTransaction( s1 -> s1
								.createSelectionQuery( "from Dealer", Dealer.class )
								.getSingleResult().chain( d -> s1.fetch( d.getPaintings() ) )
								.invoke( paintings -> {
									assertNotNull( paintings );
									assertEquals( 1, paintings.size() );
									assertEquals( "Mona Lisa", paintings.get( 0 ).getName() );
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
