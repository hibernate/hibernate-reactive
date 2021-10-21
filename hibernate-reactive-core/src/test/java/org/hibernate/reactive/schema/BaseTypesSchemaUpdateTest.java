/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.schema;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.tool.schema.JdbcMetadaAccessStrategy.INDIVIDUALLY;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

public class BaseTypesSchemaUpdateTest extends BaseReactiveTest {

	protected Configuration constructConfiguration(String hbm2DdlOption) {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( Settings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, INDIVIDUALLY.toString() );
		configuration.setProperty( Settings.HBM2DDL_AUTO, hbm2DdlOption );
		return configuration;
	}

	@Rule
	public DatabaseSelectionRule dbRule = DatabaseSelectionRule.skipTestsFor( DB2 );

	@Before
	@Override
	public void before(TestContext context) {
		Configuration configuration = constructConfiguration( "create" );
		configuration.addAnnotatedClass( BaseFourColumnEntity.class );
		configuration.setProperty( Settings.SHOW_SQL, System.getProperty(Settings.SHOW_SQL, "true") );

		test( context, setupSessionFactory( configuration )
				.thenCompose( v -> factoryManager.stop() ) );
	}

	@After
	@Override
	public void after(TestContext context) {
		final Configuration configuration = constructConfiguration( "drop" );
		configuration.addAnnotatedClass( BaseFourColumnEntity.class );

		test( context, factoryManager.stop()
				.thenCompose( v -> setupSessionFactory( configuration ) )
				.thenCompose( v -> factoryManager.stop() ) );
	}

	private String getDatatypeQuery( String actualTableName ) {
		return DatabaseConfiguration.getDatatypeQuery( actualTableName, null );
	}

	@Test
	public void testSchemaUpdateForOneColumnAdded(TestContext context) {
		FiveColumnEntity testEntity = new FiveColumnEntity();

		final Configuration configuration = constructConfiguration( "update" );
		configuration.addAnnotatedClass( FiveColumnEntity.class );

		test(
				context,
				setupSessionFactory( configuration )
						.thenCompose( v -> getSessionFactory().withTransaction( (session, t) -> session.persist( testEntity ) ) )
						.thenCompose( v1 -> openSession()
								.thenCompose( s -> s
										.find( FiveColumnEntity.class, testEntity.id )
										.thenAccept( result -> context.assertNotNull( result ) )
										.thenCompose( v -> s
												.createNativeQuery(
														getDatatypeQuery( testEntity.getEntityName()), String.class )
												.getResultList()
												.thenAccept( result -> context.assertEquals(result.size(), 5) )
										)
								)
						)
		);
	}

	@Test
	public void testSchemaUpdateForOneColumnRemoved(TestContext context) {
		ThreeColumnEntity testEntity = new ThreeColumnEntity();

		final Configuration configuration = constructConfiguration( "update" );
		configuration.addAnnotatedClass( ThreeColumnEntity.class );

		test(
				context,
				setupSessionFactory( configuration )
						.thenCompose( v -> getSessionFactory().withTransaction( (session, t) -> session.persist( testEntity ) ) )
						.thenCompose( v1 -> openSession()
								.thenCompose( s -> s
										.find( ThreeColumnEntity.class, testEntity.id )
										.thenAccept( result -> context.assertNotNull( result ) )
										.thenCompose( v -> s
												.createNativeQuery(
														getDatatypeQuery( testEntity.getEntityName()), String.class )
												.getResultList()
												.thenAccept( result -> context.assertEquals(result.size(), 3) )
										)
								)
						)
		);
	}

	@Entity(name = "ColumnsUpdateTestEntity")
	public static class BaseFourColumnEntity {
		@Id
		@GeneratedValue
		Integer id;

		String columnOne;
		String columnTwo;
		String columnThree;

		public String getEntityName() {
			return "ColumnsUpdateTestEntity";
		}
	}

	@Entity(name = "ColumnsUpdateTestEntity")
	public static class ThreeColumnEntity {
		@Id
		@GeneratedValue
		Integer id;

		String columnOne;
		String columnTwo;

		public String getEntityName() {
			return "ColumnsUpdateTestEntity";
		}
	}

	@Entity(name = "ColumnsUpdateTestEntity")
	public static class FiveColumnEntity {
		@Id
		@GeneratedValue
		Integer id;

		String columnOne;
		String columnTwo;
		String columnThree;
		String columnFour;

		public String getEntityName() {
			return "ColumnsUpdateTestEntity";
		}
	}
}
