/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.testing.SqlStatementTracker;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.assertj.core.api.Condition;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

/**
 * Same as Hibernate ORM org.hibernate.orm.test.stateless.UpsertTest
 * <p>
 * These tests are in a separate class because we need to skip the execution on some databases,
 * but once this has been resolved, they could be in {@link ReactiveStatelessSessionTest}.
 * </p>
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class UpsertTest extends BaseReactiveTest {

	private static SqlStatementTracker sqlTracker;

	// A condition to check that entities are persisted using a merge operator when the database actually supports it.
	private final static Condition<String> IS_USING_MERGE = new Condition<>(
			s -> s.toLowerCase().startsWith( "merge into" ),
			"insertions or updates without using the merge operator"
	);

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		sqlTracker = new SqlStatementTracker( UpsertTest::filter, configuration.getProperties() );
		return configuration;
	}

	@Override
	protected void addServices(StandardServiceRegistryBuilder builder) {
		sqlTracker.registerService( builder );
	}

	private static boolean filter(String s) {
		String[] accepted = {"insert ", "update ", "merge "};
		for ( String valid : accepted ) {
			if ( s.toLowerCase().startsWith( valid ) ) {
				return true;
			}
		}
		return false;
	}

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Record.class );
	}

	@Test
	public void testMutinyUpsert(VertxTestContext context) {
		test( context, getMutinySessionFactory().withStatelessTransaction( ss -> ss
							  .upsert( new Record( 123L, "hello earth" ) )
							  .call( () -> ss.upsert( new Record( 456L, "hello mars" ) ) )
							  .invoke( this::assertQueries )
					  )
					  .call( v -> getMutinySessionFactory().withStatelessTransaction( ss -> ss
									  .createSelectionQuery( "from Record order by id", Record.class )
									  .getResultList() )
							  .invoke( results -> {
								  assertThat( results ).containsExactly(
										  new Record( 123L, "hello earth" ),
										  new Record( 456L, "hello mars" )
								  );
							  } )
					  )
					  .call( () -> getMutinySessionFactory().withStatelessTransaction( ss -> ss
							  .upsert( new Record( 123L, "goodbye earth" ) )
					  ) )
					  .invoke( this::assertQueries )
					  .call( v -> getMutinySessionFactory()
							  .withStatelessTransaction( ss -> ss
									  .createSelectionQuery( "from Record order by id", Record.class )
									  .getResultList() )
							  .invoke( results -> assertThat( results ).containsExactly(
									  new Record( 123L, "goodbye earth" ),
									  new Record( 456L, "hello mars" )
							  ) )
					  )
		);
	}

	@Test
	public void testStageUpsert(VertxTestContext context) {
		test( context, getSessionFactory().withStatelessTransaction( ss -> ss
							  .upsert( new Record( 123L, "hello earth" ) )
							  .thenCompose( v -> ss.upsert( new Record( 456L, "hello mars" ) ) )
					  )
					  .thenAccept( v -> this.assertQueries() )
					  .thenCompose( v -> getSessionFactory().withStatelessTransaction( ss -> ss
									  .createSelectionQuery( "from Record order by id", Record.class ).getResultList() )
							  .thenAccept( results -> assertThat( results ).containsExactly(
									  new Record( 123L, "hello earth" ),
									  new Record( 456L, "hello mars" )
							  ) )
					  )
					  .thenCompose( v -> getSessionFactory().withStatelessTransaction( ss -> ss
							  .upsert( new Record( 123L, "goodbye earth" ) )
					  ) )
					  .thenAccept( v -> this.assertQueries() )
					  .thenCompose( v -> getSessionFactory().withStatelessTransaction( ss -> ss
									  .createSelectionQuery( "from Record order by id", Record.class ).getResultList() )
							  .thenAccept( results -> assertThat( results ).containsExactly(
									  new Record( 123L, "goodbye earth" ),
									  new Record( 456L, "hello mars" )
							  ) )
					  )
		);
	}

	private void assertQueries() {
		if ( hasMergeOperator() ) {
			assertThat( sqlTracker.getLoggedQueries() ).have( IS_USING_MERGE );
		}
		else {
			// This might be overkill, but it's still helpful in case more databases are going to support
			// the merge operator, and we need to update the documentation or warn people about it.
			assertThat( sqlTracker.getLoggedQueries() ).doNotHave( IS_USING_MERGE );
		}
	}

	private boolean hasMergeOperator() {
		return switch ( dbType() ) {
			case SQLSERVER, ORACLE, POSTGRESQL, DB2 -> true;
			default -> false;
		};
	}

	@Entity(name = "Record")
	@Table(name = "Record")
	public static class Record {
		@Id
		public Long id;
		public String message;

		Record(Long id, String message) {
			this.id = id;
			this.message = message;
		}

		Record() {
		}

		public Long getId() {
			return id;
		}

		public String getMessage() {
			return message;
		}

		public void setMessage(String msg) {
			message = msg;
		}

		// Equals and HashCode for simplifying the test assertions,
		// not to be taken as an example or for production.
		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Record record = (Record) o;
			return Objects.equals( id, record.id ) && Objects.equals( message, record.message );
		}

		@Override
		public int hashCode() {
			return Objects.hash( id, message );
		}
	}
}
