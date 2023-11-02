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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

/**
 * Same as Hibernate ORM org.hibernate.orm.test.stateless.UpsertTest
 * <p>
 *     These tests are in a separate class because we need to skip the execution on some databases,
 *     but once this has been resolved, they could be in {@link ReactiveStatelessSessionTest}.
 * </p>
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class UpsertTest extends BaseReactiveTest {
	private static SqlStatementTracker sqlTracker;

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
		String[] accepted = { "insert ", "update ", "merge " };
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
						.invoke( v -> assertThat( verifySqlForDB() ).isTrue() )
					  )
					  .call( v -> getMutinySessionFactory().withStatelessTransaction( ss -> ss
									  .createQuery( "from Record order by id", Record.class ).getResultList() )
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
					  .call( v -> getMutinySessionFactory().withStatelessTransaction( ss -> ss
									  .createQuery( "from Record order by id", Record.class ).getResultList() )
							  .invoke( results -> assertThat( results ).containsExactly(
									  new Record( 123L, "goodbye earth" ),
									  new Record( 456L, "hello mars" )
							  ) )
					  )
		);
	}

	@Test
	public void testMutinyUpsertWithEntityName(VertxTestContext context) {
		test( context, getMutinySessionFactory().withStatelessTransaction( ss -> ss
							  .upsert( Record.class.getName(), new Record( 123L, "hello earth" ) )
							  .call( () -> ss.upsert( Record.class.getName(), new Record( 456L, "hello mars" ) ) )
						.invoke( v -> assertThat( verifySqlForDB() ).isTrue() )
					  )
					  .call( v -> getMutinySessionFactory().withStatelessTransaction( ss -> ss
									  .createQuery( "from Record order by id", Record.class ).getResultList() )
							  .invoke( results -> assertThat( results ).containsExactly(
									  new Record( 123L, "hello earth" ),
									  new Record( 456L, "hello mars" )
							  ) )
					  )
					  .call( () -> getMutinySessionFactory().withStatelessTransaction( ss -> ss
							  .upsert( Record.class.getName(), new Record( 123L, "goodbye earth" ) )
					  ) )
					  .call( v -> getMutinySessionFactory().withStatelessTransaction( ss -> ss
									  .createQuery( "from Record order by id", Record.class ).getResultList() )
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
						.thenApply( v -> assertThat( verifySqlForDB() ).isTrue() )
					  )
					  .thenCompose( v -> getSessionFactory().withStatelessTransaction( ss -> ss
									  .createQuery( "from Record order by id", Record.class ).getResultList() )
							  .thenAccept( results -> assertThat( results ).containsExactly(
									  new Record( 123L, "hello earth" ),
									  new Record( 456L, "hello mars" )
							  ) )
					  )
					  .thenCompose( v -> getSessionFactory().withStatelessTransaction( ss -> ss
							  .upsert( new Record( 123L, "goodbye earth" ) )
					  ) )
					  .thenCompose( v -> getSessionFactory().withStatelessTransaction( ss -> ss
									  .createQuery( "from Record order by id", Record.class ).getResultList() )
							  .thenAccept( results -> assertThat( results ).containsExactly(
									  new Record( 123L, "goodbye earth" ),
									  new Record( 456L, "hello mars" )
							  ) )
					  )
		);
	}

	@Test
	public void testStageUpsertWithEntityName(VertxTestContext context) {
		test( context, getSessionFactory().withStatelessTransaction( ss -> ss
							  .upsert( Record.class.getName(), new Record( 123L, "hello earth" ) )
							  .thenCompose( v -> ss.upsert( Record.class.getName(), new Record( 456L, "hello mars" ) ) )
						.thenApply( v -> assertThat( verifySqlForDB() ).isTrue() )
					  )
					  .thenCompose( v -> getSessionFactory().withStatelessTransaction( ss -> ss
									  .createQuery( "from Record order by id", Record.class ).getResultList() )
							  .thenAccept( results -> assertThat( results ).containsExactly(
									  new Record( 123L, "hello earth" ),
									  new Record( 456L, "hello mars" )
							  ) )
					  )
					  .thenCompose( v -> getSessionFactory().withStatelessTransaction( ss -> ss
							  .upsert( Record.class.getName(), new Record( 123L, "goodbye earth" ) )
					  ) )
					  .thenCompose( v -> getSessionFactory().withStatelessTransaction( ss -> ss
									  .createQuery( "from Record order by id", Record.class ).getResultList() )
							  .thenAccept( results -> assertThat( results ).containsExactly(
									  new Record( 123L, "goodbye earth" ),
									  new Record( 456L, "hello mars" )
							  ) )
					  )
		);
	}



	private boolean verifySqlForDB() {
		String DB_NAME = dbType().getDialectClass().getName().toUpperCase();
		if ( DB_NAME.contains( "DB2" ) || DB_NAME.contains( "MARIA" ) || DB_NAME.contains( "MYSQL" ) || DB_NAME.contains( "COCKROACH" ) ) {
			return foundQuery( "update" ) && foundQuery( "insert" );
		}
		if ( DB_NAME.contains( "SQLSERVER" ) || DB_NAME.contains( "MSSQL" ) || DB_NAME.contains( "POSTGRES" ) || DB_NAME.contains(
				"ORACLE" ) ) {
			return foundQuery( "merge" );
		}
		return false;
	}

	private boolean foundQuery( String startsWithFragment ) {
		for ( int i = 0; i < sqlTracker.getLoggedQueries().size(); i++ ) {
			if ( sqlTracker.getLoggedQueries().get( i ).startsWith( startsWithFragment ) ) {
				return true;
			}
		}
		return false;
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
