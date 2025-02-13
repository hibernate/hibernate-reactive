/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.schema;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.temptable.TemporaryTable;
import org.hibernate.query.sqm.mutation.internal.temptable.GlobalTemporaryTableStrategy;
import org.hibernate.query.sqm.mutation.internal.temptable.PersistentTableStrategy;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.annotations.EnabledFor;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.SqlStatementTracker;
import org.hibernate.reactive.util.impl.CompletionStages;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Inheritance;
import jakarta.persistence.InheritanceType;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.COCKROACHDB;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Test enabling and disabling of strategies for the creation of temporary tables to store ids.
 *
 * @see GlobalTemporaryTableStrategy
 * @see PersistentTableStrategy
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class TemporaryIdTableStrategyTest extends BaseReactiveTest {
	private static SqlStatementTracker sqlStatementTracker;

	final static Dialect[] dialect = new Dialect[1];

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return Set.of( Engineer.class, Doctor.class, Person.class );
	}

	public static Stream<Arguments> settings() {
		return Stream.of(
				arguments( true, 1, true, 1 ),
				arguments( true, 1, false, 0 ),
				// I'm assuming Hibernate won't drop the id tables if they haven't been created
				arguments( false, 0, true, 0 ),
				arguments( false, 0, false, 0 )
		);
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( Settings.HBM2DDL_AUTO, "create" );
		// Collect all the logs, we are going to filter them later
		sqlStatementTracker = new SqlStatementTracker( s -> true, configuration.getProperties() );
		return configuration;
	}

	@Override
	public CompletionStage<Void> deleteEntities(Class<?>... entities) {
		// Deleting entities is not necessary for this test
		return voidFuture();
	}

	@Override
	public void before(VertxTestContext context) {
		// We need to start and close our own session factories for the test
	}

	@AfterEach
	@Override
	public void after(VertxTestContext context) {
		sqlStatementTracker.clear();
		super.after( context );
	}

	@Override
	protected void addServices(StandardServiceRegistryBuilder builder) {
		if ( sqlStatementTracker != null ) {
			sqlStatementTracker.registerService( builder );
		}
	}

	@ParameterizedTest(name = "Global Temporary tables - create: {0}, drop: {2}")
	@MethodSource("settings")
	@EnabledFor(value = ORACLE, reason = "It uses GlobalTemporaryTableStrategy by default")
	public void testGlobalTemporaryTablesStrategy(
			boolean enableCreateIdTables,
			// Expected number of temporary tables created
			int expectedTempTablesCreated,
			boolean enableDropIdTables,
			// Expected number of temporary tables dropped
			int expectedTempTablesDropped,
			VertxTestContext context) {
		Configuration configuration = constructConfiguration();
		configuration.setProperty( GlobalTemporaryTableStrategy.CREATE_ID_TABLES, enableCreateIdTables );
		configuration.setProperty( GlobalTemporaryTableStrategy.DROP_ID_TABLES, enableDropIdTables );

		testTemporaryIdTablesCreationAndDropping( configuration, expectedTempTablesCreated, expectedTempTablesDropped, context );
	}

	@ParameterizedTest(name = "Persistent tables - create: {0}, drop: {2}")
	@MethodSource("settings")
	@EnabledFor(value = COCKROACHDB, reason = "It uses PersistentTemporaryTableStrategy by default")
	public void testPersistentTemporaryTablesStrategy(
			boolean enableCreateIdTables,
			// Expected number of temporary tables created
			int expectedTempTablesCreated,
			boolean enableDropIdTables,
			// Expected number of temporary tables dropped
			int expectedTempTablesDropped,
			VertxTestContext context) {

		Configuration configuration = constructConfiguration();
		configuration.setProperty( PersistentTableStrategy.CREATE_ID_TABLES, enableCreateIdTables );
		configuration.setProperty( PersistentTableStrategy.DROP_ID_TABLES, enableDropIdTables );

		testTemporaryIdTablesCreationAndDropping( configuration, expectedTempTablesCreated, expectedTempTablesDropped, context );
	}

	private void testTemporaryIdTablesCreationAndDropping(
			Configuration configure,
			int expectedTempTablesCreated,
			int expectedTempTablesDropped,
			VertxTestContext context) {
		test( context, setupSessionFactory( configure )
				.thenAccept( v -> {
					dialect[0] = getDialect();
					assertThat( commandsCount( dialect[0].getTemporaryTableCreateCommand() ) )
							.as( "Unexpected number of temporary tables for ids CREATED" )
							.isEqualTo( expectedTempTablesCreated );
					sqlStatementTracker.clear();
				} )
				// to ensure the factory is always closed even in case of exceptions
				.handle( CompletionStages::handle )
				.thenCompose( this::closeFactory )
				.thenAccept( v -> assertThat( commandsCount( dialect[0].getTemporaryTableDropCommand() ) )
						.as( "Unexpected number of temporary tables for ids DROPPED" )
						.isEqualTo( expectedTempTablesDropped ) )
		);
	}

	// Always try to close the factory without losing the original error (if there was one)
	private CompletionStage<Void> closeFactory(CompletionStages.CompletionStageHandler<Void, Throwable> handler) {
		return factoryManager.stop()
				.handle( CompletionStages::handle )
				.thenCompose( factoryHandler -> handler
						.getResultAsCompletionStage()
						// When there's already an exception, we don't care about errors closing the factory
						.thenCompose( factoryHandler::getResultAsCompletionStage ) );
	}

	private static long commandsCount(String temporaryTableCommand) {
		return sqlStatementTracker.getLoggedQueries().stream()
				.filter( q -> q.startsWith( temporaryTableCommand ) && q.contains( TemporaryTable.ID_TABLE_PREFIX ) )
				.count();
	}

	@Entity(name = "Person")
	@Inheritance(strategy = InheritanceType.JOINED)
	public static class Person {

		@Id
		@GeneratedValue
		private Long id;

		private String name;

		private boolean employed;

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public boolean isEmployed() {
			return employed;
		}

		public void setEmployed(boolean employed) {
			this.employed = employed;
		}
	}

	@Entity(name = "Doctor")
	public static class Doctor extends Person {
	}

	@Entity(name = "Engineer")
	public static class Engineer extends Person {

		private boolean fellow;

		public boolean isFellow() {
			return fellow;
		}

		public void setFellow(boolean fellow) {
			this.fellow = fellow;
		}
	}
}
