/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.schema;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.temptable.TemporaryTable;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.SqlStatementTracker;
import org.hibernate.reactive.util.impl.CompletionStages;

import org.junit.jupiter.api.AfterEach;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Inheritance;
import jakarta.persistence.InheritanceType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public abstract class AbstractTemporaryTableTest extends BaseReactiveTest {
	private static SqlStatementTracker sqlStatementTracker;

	final static Dialect[] dialect = new Dialect[1];

	@FunctionalInterface
	protected interface CheckTemporaryTableCommandExecution {
		void check();
	}

	@Override
	public void before(VertxTestContext context) {
	}

	@AfterEach
	@Override
	public void after(VertxTestContext context) {
		sqlStatementTracker.clear();
		super.after( context );
	}

	@Override
	public CompletionStage<Void> deleteEntities(Class<?>... entities) {
		return voidFuture();
	}

	@Override
	protected void addServices(StandardServiceRegistryBuilder builder) {
		if ( sqlStatementTracker != null ) {
			sqlStatementTracker.registerService( builder );
		}
	}

	protected void testTemporaryTableCreation(
			Consumer<Configuration> configure,
			CheckTemporaryTableCommandExecution temporaryTableCreationCheck,
			CheckTemporaryTableCommandExecution temporaryTableDopCheck,
			VertxTestContext context) {
		test( context, setupSessionFactory( constructConfiguration( configure ) )
				.thenCompose( v -> getSessionFactory().withSession( s -> {
					dialect[0] = getDialect();
					temporaryTableCreationCheck.check();
					sqlStatementTracker.clear();
					return voidFuture();
				} ) )
				// to ensure the factory is always closed even in case of exceptions
				.handle( CompletionStages::handle )
				.thenCompose( handler -> factoryManager.stop()
						.handle( CompletionStages::handle )
						// returns the exception thrown before the factory was stopped (if there was one)
						.thenCompose( unused -> handler.getResultAsCompletionStage() )
						.thenAccept( v -> {
							temporaryTableDopCheck.check();
						} )
				)
		);
	}

	private Configuration constructConfiguration(Consumer<Configuration> configure) {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( Settings.HBM2DDL_AUTO, "create" );
		configuration.addAnnotatedClass( Engineer.class );
		configuration.addAnnotatedClass( Doctor.class );
		configuration.addAnnotatedClass( Person.class );

		sqlStatementTracker = new SqlStatementTracker(
				AbstractTemporaryTableTest::filter,
				configuration.getProperties()
		);

		configure.accept( configuration );
		return configuration;
	}

	protected static void assertNumberOfTemporaryTableCreated(int expectedNumberOfCreatedTable, String errorMessage) {
		assertThat( getNumberOfCommandExecuted( dialect[0].getTemporaryTableCreateCommand() ) )
				.as( errorMessage )
				.isEqualTo( expectedNumberOfCreatedTable );
	}

	protected static void assertNumberOfTemporaryTableDropped(int expectedNumberOfCreatedTable, String errorMessage) {
		assertThat( getNumberOfCommandExecuted( dialect[0].getTemporaryTableDropCommand() ) )
				.as( errorMessage )
				.isEqualTo( expectedNumberOfCreatedTable );
	}

	private static long getNumberOfCommandExecuted(String temporaryTableCommand) {
		List<String> loggedQueries = sqlStatementTracker.getLoggedQueries();
		return loggedQueries.stream().filter( q -> q.contains( temporaryTableCommand ) && q.contains( TemporaryTable.ID_TABLE_PREFIX )).count();
	}

	private static boolean filter(String s) {
		String[] accepted = { "create", "drop" };
		for ( String valid : accepted ) {
			if ( s.toLowerCase().startsWith( valid ) ) {
				return true;
			}
		}
		return false;
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
