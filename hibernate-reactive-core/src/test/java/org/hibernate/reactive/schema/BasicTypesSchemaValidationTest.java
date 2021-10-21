/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.schema;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.tool.schema.JdbcMetadaAccessStrategy.INDIVIDUALLY;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

/**
 * This test checks the schema datatypes for a defined entity by querying the information_schema.columns table
 * The TestableDatabase interface contains an enum of potential data types.
 * Implementors of the TestableDatabase (i.e. PostgreSQLDatabase) implement methods to get the specific query that
 * should return the expected schema type on the DB
 */
public class BasicTypesSchemaValidationTest extends BaseReactiveTest {

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
		configuration.addAnnotatedClass( BasicTypesTestEntity.class );
		configuration.setProperty( Settings.SHOW_SQL, System.getProperty(Settings.SHOW_SQL, "true") );

		test( context, setupSessionFactory( configuration )
				.thenCompose( v -> factoryManager.stop() ) );
	}

	@After
	@Override
	public void after(TestContext context) {

		final Configuration configuration = constructConfiguration( "drop" );
		configuration.addAnnotatedClass( BasicTypesTestEntity.class );

		test( context, factoryManager.stop()
				.thenCompose( v -> setupSessionFactory( configuration ) )
				.thenCompose( v -> factoryManager.stop() ) );
	}

	@Test
	public void testValidationSucceed(TestContext context) {
		Configuration configuration = constructConfiguration( "validate" );
		configuration.addAnnotatedClass( BasicTypesTestEntity.class );

		test(
				context,
				setupSessionFactory( configuration )
						.thenCompose( v -> getSessionFactory()
								.withTransaction(
										(session, t) -> session.createQuery( "FROM BasicTypesTestEntity", BasicTypesTestEntity.class )
												.getResultList() ) )
						.thenAccept( results -> context.assertTrue( results.isEmpty() ) )
		);
	}
}
