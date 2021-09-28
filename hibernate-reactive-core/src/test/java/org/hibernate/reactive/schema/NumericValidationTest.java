/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.schema;

import static org.hibernate.tool.schema.JdbcMetadaAccessStrategy.GROUPED;
import static org.hibernate.tool.schema.JdbcMetadaAccessStrategy.INDIVIDUALLY;

import java.math.BigDecimal;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.provider.Settings;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

public abstract class NumericValidationTest extends BaseReactiveTest {

	public static class IndividuallyNumericValidationTestBase extends NumericValidationTest {

		@Override
		protected Configuration constructConfiguration(String hbm2DdlOption) {
			final Configuration configuration = super.constructConfiguration( hbm2DdlOption );
			configuration.setProperty( Settings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, INDIVIDUALLY.toString() );
			return configuration;
		}
	}

	public static class GroupedNumericValidationTestBase extends NumericValidationTest {

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

	@Before
	@Override
	public void before(TestContext context) {

		Configuration configuration = constructConfiguration( "create" );
		configuration.addAnnotatedClass( TestEntity.class );

		test( context, setupSessionFactory( configuration )
				.thenCompose( v -> factoryManager.stop() ) );
	}

	@After
	@Override
	public void after(TestContext context) {

		final Configuration configuration = constructConfiguration( "drop" );
		configuration.addAnnotatedClass( TestEntity.class );

		test( context, factoryManager.stop()
				.thenCompose( v -> setupSessionFactory( configuration ) )
				.thenCompose( v -> factoryManager.stop() ) );
	}

	@Test
	public void testValidationSucceed(TestContext context) {
		Configuration configuration = constructConfiguration( "validate" );
		configuration.addAnnotatedClass( TestEntity.class );

		test(
				context,
				setupSessionFactory( configuration )
						.thenCompose( v -> getSessionFactory()
								.withTransaction(
										(session, t) -> session.createQuery( "FROM TestEntity", TestEntity.class )
												.getResultList() ) )
						.thenAccept( results -> context.assertTrue( results.isEmpty() ) )
		);
	}

	@Test
	public void testDataTypePostgreSql(TestContext context) {
		final TestEntity testEntity = new TestEntity();
		testEntity.id = 9;
		testEntity.number = new BigDecimal( 3.14159 );

		String columnTypeQuery =
				"select data_type from information_schema.columns " +
						"where table_name = 'testentity' and column_name = 'numbervalue';";

		final Configuration configuration = constructConfiguration( "update" );
		configuration.addAnnotatedClass( TestEntity.class );

		if ( DatabaseConfiguration.dbType() != DatabaseConfiguration.DBType.POSTGRESQL ) {
			return;
		}
		test(
				context,
				setupSessionFactory( configuration )
						.thenCompose( v -> getSessionFactory()
								.withTransaction( (session, t) -> session.persist( testEntity ) ) )
						.thenCompose( v1 -> openSession()
								.thenCompose( s -> s
										.find( TestEntity.class, testEntity.id )
										.thenAccept( result -> {
													context.assertNotNull( result );
													context.assertEquals( testEntity.number, testEntity.number );
												}
										).thenCompose( v -> s.createNativeQuery( columnTypeQuery, String.class )
												.getResultList()
												.thenAccept( list -> {
													context.assertEquals( 1, list.size() );
													context.assertTrue( list.get( 0 ).equals( "numeric" ) );
												} )
										)
								)
						)
		);
	}

	@Test
	public void testDataTypeDB2(TestContext context) {
		TestEntity testEntity = new TestEntity();
		testEntity.id = 9;
		testEntity.number = new BigDecimal( 3.14159);

		String columnTypeQuery =
				"SELECT TYPENAME\n" +
						"FROM SYSCAT.COLUMNS where TABSCHEMA = 'HREACT' and TABNAME = 'TESTENTITY' and COLNAME = 'NUMBERVALUE'\n" +
						"ORDER BY TABSCHEMA, TABNAME, COLNAME;";

		final Configuration configuration = constructConfiguration( "validate" );
		configuration.addAnnotatedClass( TestEntity.class );

		if( DatabaseConfiguration.dbType() != DatabaseConfiguration.DBType.DB2 ) {
			return;
		}

		test(
				context,
				setupSessionFactory( configuration )
						.thenCompose( v -> getSessionFactory()
								.withTransaction( (session, t) -> session.persist( testEntity ) ) )
						.thenCompose( v1 -> openSession()
								.thenCompose( s -> s
										.find( TestEntity.class, testEntity.id )
										.thenAccept( result -> {
											context.assertNotNull( result );
											context.assertEquals( testEntity.number, testEntity.number );
										}
								).thenCompose( v -> s.createNativeQuery( columnTypeQuery, String.class )
												.getResultList()
												.thenAccept( list -> {
													context.assertEquals( 1, list.size() );
													context.assertTrue( list.get( 0 ).equals( "DECIMAL" ) );
												} )
										)
						)
				)
		);
	}

	@Entity(name = "TestEntity")
	public static class TestEntity {
		@Id
		public Integer id;

		@Column(name = "numberValue")
		BigDecimal number;
	}
}
