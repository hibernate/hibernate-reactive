/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.HashSet;
import java.util.Set;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;

import org.hibernate.cfg.Configuration;

import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static javax.persistence.CascadeType.PERSIST;
import static javax.persistence.CascadeType.REMOVE;
import static org.assertj.core.api.Assertions.assertThat;


public class OrphanRemovalTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Version.class );
		configuration.addAnnotatedClass( Product.class );
		configuration.addAnnotatedClass( Shop.class );
		return configuration;
	}

	@Test
	public void testOrphan(TestContext context) {
		Shop shop = new Shop( "shop" );
		Product product = new Product( "ap1", shop );
		product.addVersion( new Version( product ) );
		shop.addProduct( product );
		shop.addProduct( new Product( "ap2", shop ) );
		shop.addProduct( new Product( "ap3", shop ) );
		shop.addProduct( new Product( "ap4", shop ) );

		test(
				context,
				getSessionFactory()
						.withTransaction( session -> session.persist( shop ) )
						.thenCompose( v -> getSessionFactory()
								.withTransaction( session -> session.find( Shop.class, shop.id )
								.thenCompose( result -> session.fetch( result.products )
										.thenApply( products -> result ) )
										.thenAccept( result -> {
											// update
											result.products.clear();
											result.addProduct( new Product( "bp5", result ) );
											result.addProduct( new Product( "bp6", result ) );
											result.addProduct( new Product( "bp7", result ) );
										} )
						) )
						.thenCompose( v -> getSessionFactory()
								.withTransaction( session -> session
										.createQuery( "select count(*) from Product", Long.class )
										.getSingleResult()
										.thenAccept( result -> context.assertEquals( 3L, result ) )
						) )
						.thenCompose( v -> getSessionFactory()
								.withTransaction( session -> session
										.createQuery( "select name from Product" )
										.getResultList()
										.thenAccept( list -> assertThat( list )
												.containsExactlyInAnyOrder( "bp5", "bp6", "bp7" ) )
						) )
		);
	}

	@Entity(name = "ProductVersion")
	public static class Version {
		@Id
		@GeneratedValue
		private long id;

		@ManyToOne
		private Product product;

		public Version(Product product) {
			this.product = product;
		}

		Version() {
		}

		@Override
		public String toString() {
			return Version.class.getSimpleName() + ":" + id + ":" + product;
		}
	}

	@Entity(name = "Product")
	public static class Product {

		@Id
		@GeneratedValue
		private long id;
		private String name;

		Product(String name, Shop shop) {
			this.name = name;
			this.shop = shop;
		}

		Product() {
		}

		@ManyToOne
		private Shop shop;

		@OneToMany(mappedBy = "product", cascade = { PERSIST, REMOVE })
		private Set<Version> versions = new HashSet<>();

		public void addVersion(Version version) {
			versions.add( version );
		}

		@Override
		public String toString() {
			return Product.class.getSimpleName() + ":" + id + ":" + name + ":" + shop;
		}
	}

	@Entity(name = "Shop")
	public static class Shop {

		@Id
		@GeneratedValue
		private long id;
		private String name;

		Shop(String name) {
			this.name = name;
		}

		Shop() {
		}

		@OneToMany(mappedBy = "shop", cascade = { PERSIST, REMOVE }, orphanRemoval = true)
		private Set<Product> products = new HashSet<>();

		public void addProduct(Product product) {
			products.add( product );
		}

		@Override
		public String toString() {
			return Shop.class.getSimpleName() + ":" + id + ":" + name;
		}
	}

}
