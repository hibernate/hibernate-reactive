/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import static org.assertj.core.api.Assertions.assertThat;

public class EmbeddedIdWithManyTest extends BaseReactiveTest {

	Fruit cherry;
	Fruit apple;
	Fruit banana;

	Flower sunflower;
	Flower chrysanthemum;
	Flower rose;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Flower.class, Fruit.class );
	}

	@BeforeEach
	public void populateDb(VertxTestContext context) {
		Seed seed1 = new Seed( 1 );
		rose = new Flower( seed1, "Rose" );

		cherry = new Fruit( seed1, "Cherry" );
		cherry.addFriend( rose );

		Seed seed2 = new Seed( 2 );
		sunflower = new Flower( seed2, "Sunflower" );

		apple = new Fruit( seed2, "Apple" );
		apple.addFriend( sunflower );

		Seed seed3 = new Seed( 3 );
		chrysanthemum = new Flower( seed3, "Chrysanthemum" );

		banana = new Fruit( seed3, "Banana" );
		banana.addFriend( chrysanthemum );

		test(
				context,
				getMutinySessionFactory().withTransaction( s -> s
						.persistAll( cherry, rose, sunflower, apple, chrysanthemum, banana )
				)
		);
	}

	@Test
	public void testFindWithEmbeddedId(VertxTestContext context) {
		test(
				context, getMutinySessionFactory().withTransaction( s -> s
						.find( Flower.class, chrysanthemum.getSeed() )
						.invoke( flower -> assertThat( flower.getName() ).isEqualTo( chrysanthemum.getName() ) )
				)
		);
	}

	@Test
	public void testSelectQueryWithEmbeddedId(VertxTestContext context) {
		test(
				context, getMutinySessionFactory().withTransaction( s -> s
						.createSelectionQuery( "from Flower", Flower.class )
						.getResultList()
						.invoke( list -> assertThat( list.stream().map( Flower::getName ) )
								.containsExactlyInAnyOrder(
										sunflower.getName(),
										chrysanthemum.getName(),
										rose.getName()
								)
						)
				)
		);
	}

	@Embeddable
	public static class Seed {

		@Column(nullable = false, updatable = false)
		private Integer id;

		public Seed() {
		}

		public Seed(Integer id) {
			this.id = id;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}
	}

	@MappedSuperclass
	public static abstract class Plant {

		@EmbeddedId
		private Seed seed;

		@Column(length = 40, unique = true)
		private String name;

		protected Plant() {
		}

		protected Plant(Seed seed, String name) {
			this.seed = seed;
			this.name = name;
		}

		public Seed getSeed() {
			return seed;
		}

		public void setSeed(Seed seed) {
			this.seed = seed;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}

	@Entity(name = "Fruit")
	@Table(name = "known_fruits")
	public static class Fruit extends Plant {

		@OneToMany(mappedBy = "friend", fetch = FetchType.LAZY)
		private List<Flower> friends = new ArrayList<>();

		public Fruit() {
		}

		public Fruit(Seed seed, String name) {
			super( seed, name );
		}

		public void addFriend(Flower flower) {
			this.friends.add( flower );
			flower.friend = this;
		}

		public List<Flower> getFriends() {
			return friends;
		}

		@Override
		public String toString() {
			return "Fruit{" + getSeed().getId() + "," + getName() + '}';
		}

	}

	@Entity(name = "Flower")
	@Table(name = "known_flowers")
	public static class Flower extends Plant {

		@ManyToOne(fetch = FetchType.LAZY, optional = false)
		@JoinColumn(name = "friend", referencedColumnName = "id", nullable = false)
		private Fruit friend;

		public Flower() {
		}

		public Flower(Seed seed, String name) {
			super( seed, name );
		}

		@Override
		public String toString() {
			return "Flower{" + getSeed().getId() + "," + getName() + '}';
		}

	}

}
