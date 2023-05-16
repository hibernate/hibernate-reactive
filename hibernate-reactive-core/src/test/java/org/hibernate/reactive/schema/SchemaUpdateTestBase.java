/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.schema;

import java.util.concurrent.CompletionStage;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.testing.DBSelectionExtension;
import org.hibernate.reactive.util.impl.CompletionStages;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.junit5.VertxTestContext;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.SQLSERVER;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.hibernate.tool.schema.JdbcMetadaAccessStrategy.GROUPED;
import static org.hibernate.tool.schema.JdbcMetadaAccessStrategy.INDIVIDUALLY;

/**
 * Schema update will run different queries when the table already exists or
 * when columns are missing.
 */
public abstract class SchemaUpdateTestBase extends BaseReactiveTest {

	public static class IndividuallyStrategyTest extends SchemaUpdateTestBase {

		@Override
		protected Configuration constructConfiguration(String hbm2DdlOption) {
			final Configuration configuration = super.constructConfiguration( hbm2DdlOption );
			configuration.setProperty( Settings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, INDIVIDUALLY.toString() );
			return configuration;
		}
	}

	public static class GroupedStrategyTest extends SchemaUpdateTestBase {

		@Override
		protected Configuration constructConfiguration(String hbm2DdlOption) {
			final Configuration configuration = super.constructConfiguration( hbm2DdlOption );
			configuration.setProperty( Settings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, GROUPED.toString() );
			return configuration;
		}
	}

	@RegisterExtension
	public DBSelectionExtension dbRule = DBSelectionExtension.skipTestsFor( DB2 );

	protected Configuration constructConfiguration(String action) {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( Settings.HBM2DDL_AUTO, action );
		configuration.addAnnotatedClass( BasicTypesTestEntity.class );
		return configuration;
	}

	@Override
	public void before(VertxTestContext context) {
		// For these tests we create the factory when we need it
	}

	@Override
	public CompletionStage<Void> cleanDb() {
		getSessionFactory().close();
		return CompletionStages.voidFuture();
	}

	/**
	 * Test missing columns creation during schema update
	 */
	@Test
	public void testMissingColumnsCreation(VertxTestContext context) {
		test( context,
			  setupSessionFactory( constructConfiguration( "drop" ) )
					  .thenCompose( v -> getSessionFactory().withTransaction( SchemaUpdateTestBase::createTable ) )
					  .whenComplete( (u, throwable) -> factoryManager.stop() )
					  .thenCompose( vv -> setupSessionFactory( constructConfiguration( "update" ) )
							  .thenCompose( u -> getSessionFactory().withSession( SchemaUpdateTestBase::checkAllColumnsExist ) ) )
		);
	}

	/**
	 * Test table creation during schema update
	 */
	@Test
	public void testWholeTableCreation(VertxTestContext context) {
		test( context,
			setupSessionFactory( constructConfiguration( "drop" ) )
				.whenComplete( (u, throwable) -> factoryManager.stop() )
				.thenCompose( v -> setupSessionFactory( constructConfiguration( "update" ) )
					.thenCompose( vv -> getSessionFactory().withSession( SchemaUpdateTestBase::checkAllColumnsExist ) ) )
		);
	}

	// I don't think it's possible to create a table without columns, so we add
	// a column that's not mapped by the entity.
	// We expect the other columns to be created during the update schema phase.
	private static CompletionStage<Integer> createTable(Stage.Session session, Stage.Transaction transaction) {
		return session
				.createNativeQuery( "create table " + BasicTypesTestEntity.TABLE_NAME + " (unmapped_column " + columnType() + ")" )
				.executeUpdate();
	}

	private static String columnType() {
		return dbType() == SQLSERVER ? "int" : "integer";
	}

	/**
	 * 	The table is empty, we just want to check that a query runs without errors.
	 * 	The query throws an exception if one of the columns is missing
 	 */
	private static CompletionStage<BasicTypesTestEntity> checkAllColumnsExist(Stage.Session session) {
		return session.find( BasicTypesTestEntity.class, 10 );
	}
}
