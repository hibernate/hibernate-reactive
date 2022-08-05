/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.SqlStatementTracker;

import org.junit.After;
import org.junit.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.ext.unit.TestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test {@code hibernate.order_updates} and {@code hibernate.order_inserts} configurations.
 *
 * @see  Settings#ORDER_INSERTS
 * @see  Settings#ORDER_UPDATES
 */
public abstract class OrderQueriesTestBase extends BaseReactiveTest {

	public static class OrderUpdatesTest extends OrderQueriesTestBase {

		@Override
		public String isInsertOrdering() {
			return "false";
		}

		@Override
		public String isUpdateOrdering() {
			return "true";
		}

		@Override
		public String getBatchSize() {
			return "0";
		}

		@Override
		public List<String> expectedQueries() {
			return List.of(
					"insert into Product",
					"insert into Category",
					"insert into Product",
					"insert into Category",
					"insert into Product",
					"insert into Category",
					"update Category set name=",
					"update Category set name=",
					"update Category set name=",
					"update Product set name=",
					"update Product set name=",
					"update Product set name="
			);
		}
	}

	public static class OrderInsertsTest extends OrderQueriesTestBase {

		@Override
		public String isInsertOrdering() {
			return "true";
		}

		@Override
		public String isUpdateOrdering() {
			return "false";
		}

		@Override
		public String getBatchSize() {
			return "0";
		}

		@Override
		public List<String> expectedQueries() {
			return List.of(
					"insert into Product",
					"insert into Product",
					"insert into Product",
					"insert into Category",
					"insert into Category",
					"insert into Category",
					"update Category set name=",
					"update Product set name=",
					"update Category set name=",
					"update Product set name=",
					"update Category set name=",
					"update Product set name="
			);
		}
	}

	public static class DisableOrderTest extends OrderQueriesTestBase {

		@Override
		public String isInsertOrdering() {
			return "false";
		}

		@Override
		public String isUpdateOrdering() {
			return "false";
		}

		@Override
		public String getBatchSize() {
			return "0";
		}

		@Override
		public List<String> expectedQueries() {
			return List.of(
					"insert into Product",
					"insert into Category",
					"insert into Product",
					"insert into Category",
					"insert into Product",
					"insert into Category",
					"update Category set name=",
					"update Product set name=",
					"update Category set name=",
					"update Product set name=",
					"update Category set name=",
					"update Product set name="
			);
		}
	}

	public static class OrderInsertsAndUpdatesTest extends OrderQueriesTestBase {

		@Override
		public String isInsertOrdering() {
			return "true";
		}

		@Override
		public String isUpdateOrdering() {
			return "true";
		}

		@Override
		public String getBatchSize() {
			return "0";
		}

		@Override
		public List<String> expectedQueries() {
			return List.of(
					"insert into Product",
					"insert into Product",
					"insert into Product",
					"insert into Category",
					"insert into Category",
					"insert into Category",
					"update Category set name=",
					"update Category set name=",
					"update Category set name=",
					"update Product set name=",
					"update Product set name=",
					"update Product set name="
			);
		}
	}

	// Should be able to group queries when they are ordered, and batch size is set
	public static class OrderInsertsAndUpdatesWithBatchSizeTest extends OrderQueriesTestBase {

		@Override
		public String isInsertOrdering() {
			return "true";
		}

		@Override
		public String isUpdateOrdering() {
			return "true";
		}

		@Override
		public String getBatchSize() {
			return "50";
		}

		@Override
		public List<String> expectedQueries() {
			return List.of(
					"insert into Product",
					"insert into Category",
					"update Category set name=",
					"update Product set name="
			);
		}
	}

	/**
	 * @return value of property {@link Settings#ORDER_INSERTS}
	 */
	public abstract String isInsertOrdering();

	/**
	 * @return value of property {@link Settings#ORDER_UPDATES}
	 */
	public abstract String isUpdateOrdering();

	/**
	 * expected logged queries via {@link SqlStatementTracker#getLoggedQueries()}
	 */
	public abstract List<String> expectedQueries();

	/**
	 * @return value of property {@link Settings#STATEMENT_BATCH_SIZE}
	 */
	public abstract String getBatchSize();

	public SqlStatementTracker sqlTracker;

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( Settings.ORDER_INSERTS, isInsertOrdering() );
		configuration.setProperty( Settings.ORDER_UPDATES, isUpdateOrdering() );
		configuration.setProperty( Settings.STATEMENT_BATCH_SIZE, getBatchSize() );

		// Construct a tracker that collects query statements via the SqlStatementLogger framework.
		// Pass in configuration properties to hand off any actual logging properties
		sqlTracker = new SqlStatementTracker( OrderQueriesTestBase::updatesAndInserts, configuration.getProperties() );

		return configuration;
	}

	private static boolean updatesAndInserts(String s) {
		return s.toLowerCase().startsWith( "update " )
				|| s.toLowerCase().startsWith( "insert " );
	}

	@Override
	protected void addServices(StandardServiceRegistryBuilder builder) {
		sqlTracker.registerService( builder );
	}

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Product.class, Category.class );
	}

	public Uni<Void> populateDB() {
		return getMutinySessionFactory()
				.withTransaction( session -> {
					List<Object> entities = new ArrayList<>();
					for ( int i = 0; i < 3; i++ ) {
						Category category = new Category( i, "Category " + i );
						Product product = new Product( i, "Product " + i, i * 100f );
						entities.add( product );
						entities.add( category );
					}
					return session.persistAll( entities.toArray() );
				} );
	}

	@Test
	public void test(TestContext context) {
		test(
				context,
				populateDB()
						.call( () -> getMutinySessionFactory().withTransaction( session -> {
									Uni<?> loop = Uni.createFrom().voidItem();
									for ( int i = 0; i < 3; i++ ) {
										final int id = i;
										loop = loop
												.chain( () -> session.find( Category.class, id ) )
												.invoke( category -> category.setName( "New " + category.getName() ) )
												.chain( () -> session.find( Product.class, id ) )
												.invoke( product -> {
													product.setName( "New " + product.getName() );
													product.setPrice( 2 * product.getPrice() );
												} );
									}
									return loop;
								} )
								.invoke( this::assertLoggedQueries ) )
		);
	}

	private void assertLoggedQueries() {
		assertThat( sqlTracker.getLoggedQueries().size() )
				.as( "Unexpected number of queries logged" )
				.isEqualTo( expectedQueries().size() );

		for ( int i = 0; i < expectedQueries().size(); i++ ) {
			assertThat( sqlTracker.getLoggedQueries().get( i ) )
					.as( "Unexpected query logged: " + sqlTracker.getLoggedQueries() )
					.startsWith( expectedQueries().get( i ) );
		}
	}

	@After
	public void clearLogger() {
		sqlTracker.clear();
	}

	@Entity(name = "Category")
	@Table(name = "Category")
	static class Category {
		@Id
		private Integer id;

		private String name;

		public Category() {
		}

		public Category(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
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
			Category category = (Category) o;
			return Objects.equals( name, category.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}

		@Override
		public String toString() {
			return id + ":" + name;
		}
	}

	@Entity(name = "Product")
	@Table(name = "Product")
	static class Product {

		@Id
		private Integer id;
		private String name;
		private Float price;

		public Product() {
		}

		public Product(Integer id, String name, Float price) {
			this.id = id;
			this.name = name;
			this.price = price;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Float getPrice() {
			return price;
		}

		public void setPrice(Float price) {
			this.price = price;
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
			return Objects.equals( name, product.name ) && Objects.equals( price, product.price );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name, price );
		}

		@Override
		public String toString() {
			return id + ":" + name + ":" + price;
		}
	}
}
