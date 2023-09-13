/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.reactive.testing.DBSelectionExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.COCKROACHDB;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MARIA;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.testing.DBSelectionExtension.skipTestsFor;

/**
 * Same as Hibernate ORM org.hibernate.orm.test.stateless.UpsertTest
 * <p>
 *     These tests are in a separate class because we need to skip the execution on some databases,
 *     but once this has been resolved, they could be in {@link ReactiveStatelessSessionTest}.
 * </p>
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class UpsertTest extends BaseReactiveTest {

	/**
	 * Something is missing in HR to make it work for these databases.
 	 */
	@RegisterExtension
	public DBSelectionExtension dbSelection = skipTestsFor( COCKROACHDB, DB2, MARIA, MYSQL, ORACLE );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Record.class );
	}

	@Test
	public void testMutinyUpsert(VertxTestContext context) {
		test( context, getMutinySessionFactory().withStatelessTransaction( ss -> ss
							  .upsert( new Record( 123L, "hello earth" ) )
							  .call( () -> ss.upsert( new Record( 456L, "hello mars" ) ) )
					  )
					  .call( v -> getMutinySessionFactory().withStatelessTransaction( ss -> ss
									  .createQuery( "from Record order by id", Record.class ).getResultList() )
							  .invoke( results -> assertThat( results ).containsExactly(
									  new Record( 123L, "hello earth" ),
									  new Record( 456L, "hello mars" )
							  ) )
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
