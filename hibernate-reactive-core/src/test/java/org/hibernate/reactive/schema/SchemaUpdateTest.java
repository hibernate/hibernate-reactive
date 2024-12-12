/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.schema;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.hibernate.HibernateException;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.annotations.DisabledFor;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.stage.Stage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.cfg.SchemaToolingSettings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.SQLSERVER;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.tool.schema.JdbcMetadaAccessStrategy.GROUPED;
import static org.hibernate.tool.schema.JdbcMetadaAccessStrategy.INDIVIDUALLY;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Schema update will run different queries when the table already exists or
 * when columns are missing.
 */
@DisabledFor(value = DB2, reason = "No InformationExtractor for Dialect [org.hibernate.dialect.DB2Dialect..]")
public class SchemaUpdateTest extends BaseReactiveTest {

	static Stream<Arguments> settings() {
		return Stream.of(
				arguments( INDIVIDUALLY.toString(), null ),
				arguments( GROUPED.toString(), null ),
				arguments( INDIVIDUALLY.toString(), "VARBINARY" ),
				arguments( GROUPED.toString(), "VARBINARY" )
		);
	}

	@Override
	public CompletionStage<Void> deleteEntities(Class<?>... entities) {
		return voidFuture();
	}

	protected Configuration constructConfiguration(String action, String strategy, String type) {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( Settings.HBM2DDL_AUTO, action );
		configuration.setProperty( HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, strategy );
		configuration.addAnnotatedClass( BasicTypesTestEntity.class );
		if ( type != null ) {
			// The entity we are using for testing has some arrays. The default behaviour is to store them as XML and
			// the Vert.x client doesn't support it at the moment.
			configuration.setProperty( "hibernate.type.preferred_array_jdbc_type", "VARBINARY" );
		}
		return configuration;
	}

	@Override
	public void before(VertxTestContext context) {
		// Do nothing, we prepare everything in the test so that we can use a parameterized test
	}

	@AfterEach
	@Override
	public void after(VertxTestContext context) {
		super.after( context );
		closeFactory( context );
	}

	/**
	 * Test creation of missing columns
	 */
	@ParameterizedTest
	@MethodSource("settings")
	@Timeout(value = 10, timeUnit = MINUTES)
	public void testMissingColumnsCreation(final String strategy, final String type, VertxTestContext context) {
		final Supplier<CompletionStage<?>> testSupplier = () -> setupSessionFactory( constructConfiguration( "drop", strategy, type ) )
				.thenCompose( v -> getSessionFactory().withTransaction( SchemaUpdateTest::createTable ) )
				.whenComplete( (u, throwable) -> factoryManager.stop() )
				.thenCompose( vv -> setupSessionFactory( constructConfiguration( "update", strategy, type ) ) )
				.thenCompose( u -> getSessionFactory().withSession( SchemaUpdateTest::checkAllColumnsExist ) );
		if ( dbType() == SQLSERVER && type == null ) {
			test( context, assertThrown( HibernateException.class, testSupplier.get() )
						  .thenAccept( e -> assertThat( e.getMessage() ).startsWith( "HR000081: " ) ) );
		}
		else {
			test( context, testSupplier.get() );
		}
	}

	/**
	 * Test creation of missing table
	 */
	@ParameterizedTest
	@MethodSource("settings")
	@Timeout(value = 10, timeUnit = MINUTES)
	public void testWholeTableCreation(final String strategy, final String type, VertxTestContext context) {
		final Supplier<CompletionStage<?>> testSupplier = () -> setupSessionFactory( constructConfiguration( "drop", strategy, type ) )
				.whenComplete( (u, throwable) -> factoryManager.stop() )
				.thenCompose( v -> setupSessionFactory( constructConfiguration( "update", strategy, type ) )
						.thenCompose( vv -> getSessionFactory().withSession( SchemaUpdateTest::checkAllColumnsExist ) ) );
		if ( dbType() == SQLSERVER && type == null ) {
			test( context, assertThrown( HibernateException.class, testSupplier.get() )
					.thenAccept( e -> assertThat( e.getMessage() ).startsWith( "HR000081: " ) ) );
		}
		else {
			test( context, testSupplier.get() );
		}
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
	 * The table is empty, we just want to check that a query runs without errors.
	 * The query throws an exception if one of the columns is missing
	 */
	private static CompletionStage<BasicTypesTestEntity> checkAllColumnsExist(Stage.Session session) {
		return session.find( BasicTypesTestEntity.class, 10 );
	}
}
