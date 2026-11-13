/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.annotations.EnabledFor;
import org.hibernate.reactive.mutiny.Mutiny;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

/**
 * Check that the lock timeout set for a query is reset to the previous value of the connection
 * when the query fails, and when several sessions run the same query at the same time.
 * Unlike {@link LockTimeoutTest}, it reads the setting of the connection after the query.
 */
@EnabledFor(POSTGRESQL)
public class LockTimeoutRestoreTest extends BaseReactiveTest {

	private static final String LOCKING_QUERY = "from LockTimeoutBook";
	private static final String LOCK_TIMEOUT = "select current_setting('lock_timeout', true)";

	private final Book book = new Book( 1L, "The Hobbit" );

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( AvailableSettings.JAKARTA_LOCK_TIMEOUT, 1000 );
		return configuration;
	}

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class );
	}

	@Test
	public void testLockTimeoutRestoredAfterFailure(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persist( book ) )
				// The other transaction keeps the row locked, so the query fails after the lock timeout
				.call( () -> getMutinySessionFactory().withTransaction( (session, tx) -> session
						.find( Book.class, book.id, LockModeType.PESSIMISTIC_WRITE )
						.call( () -> getMutinySessionFactory().openSession()
								.chain( other -> lockingQuery( other )
										.onItemOrFailure().transformToUni( (result, failure) -> {
											assertThat( failure ).isNotNull();
											return lockTimeout( other );
										} )
										.invoke( timeout -> assertThat( timeout ).isEqualTo( "0" ) )
										.eventually( other::close ) ) ) ) )
		);
	}

	@Test
	public void testLockTimeoutRestoredWithConcurrentQueries(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persist( book ) )
				.chain( () -> Uni.combine().all()
						.unis( getMutinySessionFactory().openSession(), getMutinySessionFactory().openSession() )
						.asTuple() )
				.chain( sessions -> {
					final Mutiny.Session first = sessions.getItem1();
					final Mutiny.Session second = sessions.getItem2();
					// Different lock timeouts on the two connections
					return first.createNativeQuery( "set lock_timeout = 5000" ).executeUpdate()
							// The same query, so the same query plan, at the same time on both connections
							.chain( () -> Uni.combine().all()
									.unis( lockingQuery( first ), lockingQuery( second ) )
									.discardItems() )
							.chain( () -> lockTimeout( first ) )
							.invoke( timeout -> assertThat( timeout ).isEqualTo( "5s" ) )
							.chain( () -> lockTimeout( second ) )
							.invoke( timeout -> assertThat( timeout ).isEqualTo( "0" ) )
							.eventually( first::close )
							.eventually( second::close );
				} )
		);
	}

	private static Uni<List<Book>> lockingQuery(Mutiny.Session session) {
		return session.createSelectionQuery( LOCKING_QUERY, Book.class )
				.setLockMode( LockModeType.PESSIMISTIC_WRITE )
				.getResultList();
	}

	private static Uni<String> lockTimeout(Mutiny.Session session) {
		return session.createNativeQuery( LOCK_TIMEOUT, String.class ).getSingleResult();
	}

	@Entity(name = "LockTimeoutBook")
	@Table(name = "LockTimeoutBook")
	public static class Book {
		@Id
		Long id;
		String title;

		public Book() {
		}

		public Book(Long id, String title) {
			this.id = id;
			this.title = title;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			return Objects.equals( title, ( (Book) o ).title );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( title );
		}
	}
}
