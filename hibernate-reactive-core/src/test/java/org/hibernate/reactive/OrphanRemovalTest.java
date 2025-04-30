/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import static jakarta.persistence.CascadeType.ALL;
import static jakarta.persistence.CascadeType.PERSIST;
import static jakarta.persistence.CascadeType.REMOVE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout( value = 10, timeUnit = MINUTES)
public class OrphanRemovalTest extends BaseReactiveTest {

	@Override
	public CompletionStage<Void> deleteEntities(Class<?>... entities) {
		return getSessionFactory()
				.withTransaction( s -> s
						// Because of the Cascade options, all elements will be deleted
						.createSelectionQuery( "from Shop", Shop.class )
						.getResultList()
						.thenApply( List::toArray )
						.thenCompose( s::remove ) );
	}

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Version.class, Product.class, Shop.class );
	}
	@Test
	public void testOrphan(VertxTestContext context) {
		Product product = new Product( "ap1" );
		product.addVersion( new Version( "Tactical Nuclear Penguin" ) );

		Shop shop = new Shop( "shop" );
		shop.addProduct( product );
		shop.addProduct( new Product( "ap2" ) );
		shop.addProduct( new Product( "ap3" ) );
		shop.addProduct( new Product( "ap4" ) );

		test(
				context,
				getSessionFactory()
						.withTransaction( session -> session.persist( shop ) )
						.thenCompose( v -> getSessionFactory().withTransaction( session -> session
								.createSelectionQuery( "select name from Product", String.class )
								.getResultList()
								.thenAccept( list -> assertThat( list )
										.containsExactlyInAnyOrder( "ap1", "ap2", "ap3", "ap4" ) )
						) )
						.thenCompose( v -> getSessionFactory().withTransaction( session -> session
								.createSelectionQuery( "from Version", Version.class )
								.getResultList()
								.thenAccept( list -> assertThat( list )
										.containsExactlyInAnyOrder( new Version( "Tactical Nuclear Penguin" ) ) )
						) )
						.thenCompose( v -> getSessionFactory().withTransaction( session -> session
								.find( Shop.class, shop.id )
								.thenCompose( foundShop -> session
										.fetch( foundShop.products )
										.thenApply( products -> foundShop )
								)
								.thenAccept( foudnShop -> {
									// update
									foudnShop.products.clear();
									foudnShop.addProduct( new Product( "bp5" ) );
									foudnShop.addProduct( new Product( "bp6" ) );
									foudnShop.addProduct( new Product( "bp7" ) );
								} )
						) )
						.thenCompose( v -> getSessionFactory().withTransaction( session -> session
								.createSelectionQuery( "select name from Product", String.class )
								.getResultList()
								.thenAccept( list -> assertThat( list )
										.containsExactlyInAnyOrder( "bp5", "bp6", "bp7" ) )
						) )
						.thenCompose( v -> getSessionFactory().withTransaction( session -> session
								.createSelectionQuery( "from Version", Version.class )
								.getResultList()
								.thenAccept( list -> assertThat( list ).isEmpty() )
						) )
		);
	}

	@Entity(name = "Version")
	@Table(name = "ORT_ProductVersion")
	public static class Version {
		@Id
		@GeneratedValue
		private long id;

		private String name;

		@ManyToOne
		private Product product;

		Version() {
		}

		public Version(String name) {
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Version version = (Version) o;
			return Objects.equals( name, version.name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
		}

		@Override
		public String toString() {
			return Version.class.getSimpleName() + ":" + id + ":" + name + ":" + product;
		}
	}

	@Entity(name = "Product")
	@Table(name = "ORT_Product")
	public static class Product {

		@Id
		@GeneratedValue
		private long id;
		private String name;

		Product() {
		}

		Product(String name) {
			this.name = name;
		}

		@ManyToOne
		private Shop shop;

		@OneToMany(mappedBy = "product", cascade = ALL)
		private Set<Version> versions = new HashSet<>();

		public void addVersion(Version version) {
			versions.add( version );
			version.product = this;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Product product = (Product) o;
			return Objects.equals( name, product.name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
		}

		@Override
		public String toString() {
			return Product.class.getSimpleName() + ":" + id + ":" + name + ":" + shop;
		}
	}

	@Entity(name = "Shop")
	@Table(name = "ORT_Shop")
	public static class Shop {

		@Id
		@GeneratedValue
		private long id;
		private String name;

		Shop() {
		}

		Shop(String name) {
			this.name = name;
		}

		@OneToMany(mappedBy = "shop", cascade = { PERSIST, REMOVE }, orphanRemoval = true)
		private Set<Product> products = new HashSet<>();

		public long getId() {
			return id;
		}

		public void setId(long id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Set<Product> getProducts() {
			return products;
		}

		public void setProducts(Set<Product> products) {
			this.products = products;
		}

		public void addProduct(Product product) {
			products.add( product );
			product.shop = this;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Shop shop = (Shop) o;
			return Objects.equals( name, shop.name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
		}

		@Override
		public String toString() {
			return Shop.class.getSimpleName() + ":" + id + ":" + name;
		}
	}
}
