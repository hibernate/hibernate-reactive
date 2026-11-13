/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.reactive.stage.Stage;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.TableGenerator;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * The request for a new block of ids locks the row of the table until the end of the transaction:
 * check that a session holding the lock doesn't wait for a session that is waiting for the lock.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class BlockTableGeneratorLockTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( LockedId.class );
	}

	@Test
	public void testSessionHoldingTheLockNeedsAnotherBlock(VertxTestContext context) {
		final LockedId first1 = new LockedId( "First session, first block" );
		final LockedId first2 = new LockedId( "First session, second block" );
		final LockedId second1 = new LockedId( "Second session, first block" );
		final LockedId second2 = new LockedId( "Second session, second block" );

		final CompletableFuture<Void> secondSessionWaitsForLock = new CompletableFuture<>();
		final CompletableFuture<Void> secondSessionCompleted = new CompletableFuture<>();

		test( context, inTransaction( first -> first
				// Requests a block: the first session holds the lock and there is one id left
				.persist( first1 )
				.thenCompose( v -> {
					inTransaction( second -> second
							// Uses the id left in the block
							.persist( second1 )
							.thenCompose( vv -> {
								// Requests a block: it has to wait for the first session to commit
								final CompletionStage<Void> persist = second.persist( second2 );
								secondSessionWaitsForLock.complete( null );
								return persist;
							} )
					).whenComplete( (vv, throwable) -> {
						if ( throwable != null ) {
							secondSessionWaitsForLock.completeExceptionally( throwable );
							secondSessionCompleted.completeExceptionally( throwable );
						}
						else {
							secondSessionCompleted.complete( null );
						}
					} );
					return secondSessionWaitsForLock;
				} )
				// Needs another block while the second session is waiting for the lock
				.thenCompose( v -> first.persist( first2 ) ) )
				.thenCompose( v -> secondSessionCompleted )
				.thenCompose( v -> openSession() )
				.thenCompose( s -> s.createSelectionQuery( "from LockedId", LockedId.class ).getResultList() )
				.thenAccept( found -> assertThat( found )
						.containsExactlyInAnyOrder( first1, first2, second1, second2 ) )
		);
	}

	// Each call needs its own session: SessionFactory#withTransaction would reuse the current one
	private CompletionStage<Void> inTransaction(Function<Stage.Session, CompletionStage<Void>> work) {
		return getSessionFactory().openSession().thenCompose( session -> session
				.withTransaction( tx -> work.apply( session ) )
				.handle( (v, throwable) -> throwable )
				.thenCompose( throwable -> session.close()
						.thenCompose( v -> throwable == null ? voidFuture() : failedFuture( throwable ) ) ) );
	}

	@Entity(name = "LockedId")
	@Table(name = "locked_id")
	@TableGenerator(name = "lockedTab", table = "locked_id_tab", allocationSize = 2)
	public static class LockedId {
		@Id
		@GeneratedValue(generator = "lockedTab")
		Long id;
		String string;

		public LockedId() {
		}

		public LockedId(String string) {
			this.string = string;
		}

		@Override
		public String toString() {
			return id + ": " + string;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			LockedId lockedId = (LockedId) o;
			return Objects.equals( string, lockedId.string );
		}

		@Override
		public int hashCode() {
			return Objects.hash( string );
		}
	}
}
