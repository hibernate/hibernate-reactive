/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.List;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.hibernate.cfg.Configuration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

public class NonNullableManyToOneTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Dealer.class );
		configuration.addAnnotatedClass( Artist.class );
		configuration.addAnnotatedClass( Painting.class );
		return configuration;
	}

	@Before
	public void populateDB(TestContext context) {
		Artist artist = new Artist( "Grand Master Painter" );
		artist.id = 1L;
		Dealer dealer = new Dealer( "Dealer" );
		dealer.id = 1L;
		Painting painting = new Painting( "Mona Lisa");
		painting.id = 2L;
		artist.addPainting( painting );
		dealer.addPainting( painting );

		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persistAll( painting, artist, dealer ) ) );
	}

	@After
	public void cleanDB(TestContext context) {
		test( context, deleteEntities( "Painting", "Artist" ) );
	}

	@Test
	public void testNonNullableSuccess(TestContext context) {
		test(
				context,
				getMutinySessionFactory().withTransaction( session -> session
								.createQuery( "from Artist", Artist.class )
								.getSingleResult().chain( a -> session.fetch( a.paintings ) )
								.invoke( paintings -> {
									context.assertNotNull( paintings );
									context.assertEquals( 1, paintings.size() );
									context.assertEquals( "Mona Lisa", paintings.get( 0 ).name );
								} ) )
						.chain( () -> getMutinySessionFactory().withTransaction( s1 -> s1
										.createQuery( "from Dealer", Dealer.class )
										.getSingleResult().chain( d -> s1.fetch( d.paintings ) )
										.invoke( paintings -> {
											context.assertNotNull( paintings );
											context.assertEquals( 1, paintings.size() );
											context.assertEquals( "Mona Lisa", paintings.get( 0 ).name );
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

	}
}
