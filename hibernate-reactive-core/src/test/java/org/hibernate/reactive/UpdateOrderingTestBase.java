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
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.DatabaseSelectionRule;
import org.hibernate.reactive.testing.SqlStatementTracker;

import org.junit.Rule;
import org.junit.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.ext.unit.TestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;

public abstract class UpdateOrderingTestBase extends BaseReactiveTest {
	public enum QUERY_TYPE {
		INSERT,
		UPDATE
	}

	public static class UpdateOrderingTrueTest extends UpdateOrderingTestBase {

		@Override
		public String isUpdateOrdering() {
			return "true";
		}

		@Override
		public String isInsertOrdering() {
			return "false";
		}

		@Override
		public String getBatchSize() {
			return "0";
		}

		@Override
		public QUERY_TYPE queryType() {
			return QUERY_TYPE.UPDATE;
		}

		@Override
		public void assertLoggedQueries() {
			assertThat( sqlTracker.getLoggedQueries() ).hasSize( 6 );
			assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).startsWith( "update Category set name=" );
			assertThat( sqlTracker.getLoggedQueries().get( 1 ) ).startsWith( "update Category set name=" );
			assertThat( sqlTracker.getLoggedQueries().get( 2 ) ).startsWith( "update Category set name=" );
			assertThat( sqlTracker.getLoggedQueries().get( 3 ) ).startsWith( "update Product set name=" );
			assertThat( sqlTracker.getLoggedQueries().get( 4 ) ).startsWith( "update Product set name=" );
			assertThat( sqlTracker.getLoggedQueries().get( 5 ) ).startsWith( "update Product set name=" );
			sqlTracker.clear();
		}
	}

	public static class UpdateInsertsTrueTest extends UpdateOrderingTestBase {

		// Disable DB2 due to vertx-sql-client issue #899
		@Rule
		public DatabaseSelectionRule rule = DatabaseSelectionRule.skipTestsFor( DB2 );

		@Override
		public String isUpdateOrdering() {
			return "false";
		}

		@Override
		public String isInsertOrdering() {
			return "true";
		}

		@Override
		public String getBatchSize() {
			return "0";
		}

		@Override
		public QUERY_TYPE queryType() {
			return QUERY_TYPE.INSERT;
		}

		@Override
		public void assertLoggedQueries() {
			assertThat( sqlTracker.getLoggedQueries() ).hasSize( 6 );
			assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).startsWith( "insert into Product" );
			assertThat( sqlTracker.getLoggedQueries().get( 1 ) ).startsWith( "insert into Product" );
			assertThat( sqlTracker.getLoggedQueries().get( 2 ) ).startsWith( "insert into Product" );
			assertThat( sqlTracker.getLoggedQueries().get( 3 ) ).startsWith( "insert into Category" );
			assertThat( sqlTracker.getLoggedQueries().get( 4 ) ).startsWith( "insert into Category" );
			assertThat( sqlTracker.getLoggedQueries().get( 5 ) ).startsWith( "insert into Category" );
			sqlTracker.clear();
		}
	}

	public static class UpdateOrderingFalseTest extends UpdateOrderingTestBase {

		@Override
		public String isUpdateOrdering() {
			return "false";
		}

		@Override
		public String isInsertOrdering() {
			return "false";
		}

		@Override
		public String getBatchSize() {
			return "0";
		}

		@Override
		public QUERY_TYPE queryType() {
			return QUERY_TYPE.UPDATE;
		}

		@Override
		public void assertLoggedQueries() {
			assertThat( sqlTracker.getLoggedQueries() ).hasSize( 6 );
			assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).startsWith( "update Category set name=" );
			assertThat( sqlTracker.getLoggedQueries().get( 1 ) ).startsWith( "update Product set name=" );
			sqlTracker.clear();
		}
	}

	public static class UpdateInsertsFalseTest extends UpdateOrderingTestBase {

		// Disable DB2 due to vertx-sql-client issue #899
		@Rule
		public DatabaseSelectionRule rule = DatabaseSelectionRule.skipTestsFor( DB2 );

		@Override
		public String isUpdateOrdering() {
			return "false";
		}

		@Override
		public String isInsertOrdering() {
			return "false";
		}

		@Override
		public String getBatchSize() {
			return "0";
		}

		@Override
		public QUERY_TYPE queryType() {
			return QUERY_TYPE.INSERT;
		}

		@Override
		public void assertLoggedQueries() {
			assertThat( sqlTracker.getLoggedQueries() ).hasSize( 6 );
			assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).startsWith( "insert into Product" );
			assertThat( sqlTracker.getLoggedQueries().get( 1 ) ).startsWith( "insert into Category" );
			assertThat( sqlTracker.getLoggedQueries().get( 2 ) ).startsWith( "insert into Product" );
			assertThat( sqlTracker.getLoggedQueries().get( 3 ) ).startsWith( "insert into Category" );
			assertThat( sqlTracker.getLoggedQueries().get( 4 ) ).startsWith( "insert into Product" );
			assertThat( sqlTracker.getLoggedQueries().get( 5 ) ).startsWith( "insert into Category" );
			sqlTracker.clear();
		}
	}

	//	Test that when we set the Batch size only the expected number of queries run.
	//	For example, setting hibernate.jdbc.batch_size to 50 will only log two update queries in this case (when order_updates is true)
	public static class UpdateOrderingBatchSizeTest extends UpdateOrderingTestBase {

		@Override
		public String isUpdateOrdering() {
			return "true";
		}

		@Override
		public String isInsertOrdering() {
			return "true";
		}

		@Override
		public String getBatchSize() {
			return "50";
		}

		@Override
		public QUERY_TYPE queryType() {
			return QUERY_TYPE.UPDATE;
		}

		@Override
		public void assertLoggedQueries() {
			assertThat( sqlTracker.getLoggedQueries() ).hasSize( 2 );
			assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).startsWith( "update Category set name=" );
			assertThat( sqlTracker.getLoggedQueries().get( 1 ) ).startsWith( "update Product set name=" );
			sqlTracker.clear();
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
	 * run checks expected logged queries via {@link SqlStatementTracker#getLoggedQueries()}
	 */
	public abstract void assertLoggedQueries();

	/**
	 * @return value of property {@link Settings#STATEMENT_BATCH_SIZE}
	 */
	public abstract String getBatchSize();

	public abstract QUERY_TYPE queryType();

	public SqlStatementTracker sqlTracker;

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( Settings.ORDER_INSERTS, isInsertOrdering() );
		configuration.setProperty( Settings.ORDER_UPDATES, isUpdateOrdering() );
		configuration.setProperty( Settings.STATEMENT_BATCH_SIZE, getBatchSize() );

		// Construct a tracker that collects query statements via the SqlStatementLogger framework.
		// Pass in configuration properties to hand off any actual logging properties
		// set 'replaceParameters' to true, so we can track list of executed update sql statements
		switch ( queryType() ) {
			case UPDATE:
				sqlTracker = new SqlStatementTracker(
						UpdateOrderingTestBase::onlyUpdates, configuration.getProperties() );
				break;
			case INSERT:
				sqlTracker = new SqlStatementTracker(
						UpdateOrderingTestBase::onlyInserts, configuration.getProperties() );
				break;
			default:
				sqlTracker = new SqlStatementTracker( UpdateOrderingTestBase::nothing, configuration.getProperties() );
		}
		return configuration;
	}

	private static boolean onlyUpdates(String s) {
		return s.toLowerCase().startsWith( "update" );
	}

	private static boolean onlyInserts(String s) {
		return s.toLowerCase().startsWith( "insert" );
	}

	private static boolean nothing(String s) {
		return false;
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
