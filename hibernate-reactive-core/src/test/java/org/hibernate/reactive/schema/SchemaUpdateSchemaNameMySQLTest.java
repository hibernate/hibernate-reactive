/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.schema;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;
import static org.hibernate.tool.schema.JdbcMetadaAccessStrategy.GROUPED;
import static org.hibernate.tool.schema.JdbcMetadaAccessStrategy.INDIVIDUALLY;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import javax.persistence.Table;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

public abstract class SchemaUpdateSchemaNameMySQLTest extends BaseReactiveTest {

	public static class IndividuallySchemaUpdateSchemaNameMySQLTestBase extends SchemaUpdateSchemaNameMySQLTest {

		@Override
		protected Configuration constructConfiguration(String hbm2DdlOption) {
			final Configuration configuration = super.constructConfiguration( hbm2DdlOption );
			configuration.setProperty( Settings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, INDIVIDUALLY.toString() );
			return configuration;
		}
	}

	public static class GroupedSchemaUpdateSchemaNameMySQLTestBase extends SchemaUpdateSchemaNameMySQLTest {

		@Override
		protected Configuration constructConfiguration(String hbm2DdlOption) {
			final Configuration configuration = super.constructConfiguration( hbm2DdlOption );
			configuration.setProperty( Settings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, GROUPED.toString() );
			return configuration;
		}
	}

	protected Configuration constructConfiguration(String hbm2DdlOption) {
		Configuration configuration = constructConfiguration();
		configuration.setProperty( Settings.HBM2DDL_AUTO, hbm2DdlOption );
		configuration.setProperty( Settings.DEFAULT_CATALOG, "hreact" );
		return configuration;
	}

	@Rule
	public DatabaseSelectionRule dbRule = DatabaseSelectionRule.runOnlyFor( MYSQL );

	@Before
	@Override
	public void before(TestContext context) {
		Configuration createHbm2ddlConf = constructConfiguration( "create" );
		createHbm2ddlConf.addAnnotatedClass( SimpleFirst.class );

		test( context, setupSessionFactory( createHbm2ddlConf )
				.thenCompose( v -> factoryManager.stop() ) );
	}

	@After
	@Override
	public void after(TestContext context) {
		final Configuration dropHbm2ddlConf = constructConfiguration( "drop" );
		dropHbm2ddlConf.addAnnotatedClass( SimpleNext.class );

		test( context, factoryManager.stop()
				.thenCompose( v -> setupSessionFactory( dropHbm2ddlConf ) )
				.thenCompose( v -> factoryManager.stop() ) );
	}

	@Test
	public void testSqlAlterWithTableSchemaName(TestContext context) throws Exception {

		final Configuration configuration = constructConfiguration( "update" );
		configuration.addAnnotatedClass( SimpleNext.class );

		test(
				context,
				setupSessionFactory( configuration )
						.thenCompose( v -> getSessionFactory()
								.withTransaction( (session, t) -> session.createQuery( "FROM Simple", SimpleNext.class )
										.getResultList() ) )
						.thenAccept( results -> context.assertTrue( results.isEmpty() ) )
		);
	}

	@Test
	public void testPersistenceOfAlteredSchema(TestContext context) throws Exception {
		final SimpleNext aSimple = new SimpleNext();
		aSimple.id = 9;
		aSimple.setValue( 99 );

		final Configuration configuration = constructConfiguration( "update" );
		configuration.addAnnotatedClass( SimpleNext.class );

		test(
				context,
				setupSessionFactory( configuration )
						.thenCompose( v -> getSessionFactory()
								.withTransaction( (session, t) -> session.persist( aSimple ) ) )
						.thenCompose( v1 -> openSession()
								.thenCompose( s -> s
										.find( SimpleNext.class, aSimple.id )
										.thenAccept( result -> {
											context.assertNotNull( result );
											context.assertEquals( aSimple.getValue(), result.getValue() );
											context.assertEquals( aSimple.data, result.data );
										} )
								)
						)
		);
	}

	@MappedSuperclass
	public static abstract class AbstractSimple {
		@Id
		protected Integer id;
		private Integer value;

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public Integer getValue() {
			return value;
		}

		public void setValue(Integer value) {
			this.value = value;
		}
	}

	@Entity(name = "Simple")
	@Table(name = "Simple", schema = "test")
	public static class SimpleFirst extends AbstractSimple {

	}

	@Entity(name = "Simple")
	@Table(name = "Simple", schema = "test")
	public static class SimpleNext extends AbstractSimple {
		private String data;

		public String getData() {
			return data;
		}

		public void setData(String data) {
			this.data = data;
		}
	}
}
