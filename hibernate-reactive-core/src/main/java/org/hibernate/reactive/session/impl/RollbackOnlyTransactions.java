/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import java.lang.invoke.MethodHandles;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.reactive.engine.spi.ReactiveSharedSessionContractImplementor;
import org.hibernate.reactive.logging.impl.Log;

import jakarta.persistence.RollbackException;

import static org.hibernate.reactive.logging.impl.LoggerFactory.make;

/**
 * Internal helpers for transactions marked for rollback only.
 * <p>
 * In Hibernate ORM, {@link org.hibernate.engine.spi.SharedSessionContractImplementor#markForRollbackOnly()}
 * marks the {@link org.hibernate.Transaction}, but Hibernate Reactive doesn't use it, so
 * {@link ReactiveSessionImpl} and {@link ReactiveStatelessSessionImpl} keep track of it instead.
 * These methods access that state without adding it to the session interfaces.
 * <p>
 * Committing a transaction marked for rollback only behaves like
 * {@code JdbcResourceLocalTransactionCoordinatorImpl#commitRollbackOnly()} in Hibernate ORM:
 * the transaction is rolled back and, if JPA transaction compliance is enabled,
 * the commit fails with a {@link RollbackException}.
 */
public final class RollbackOnlyTransactions {

	private static final Log LOG = make( Log.class, MethodHandles.lookup() );

	private RollbackOnlyTransactions() {
	}

	/**
	 * Whether {@link org.hibernate.engine.spi.SharedSessionContractImplementor#markForRollbackOnly()}
	 * was called during the current transaction. This happens, for example, when an operation fails.
	 */
	public static boolean isMarkedForRollbackOnly(ReactiveSharedSessionContractImplementor session) {
		if ( session instanceof ReactiveSessionImpl impl ) {
			return impl.isMarkedForRollbackOnly();
		}
		if ( session instanceof ReactiveStatelessSessionImpl impl ) {
			return impl.isMarkedForRollbackOnly();
		}
		return false;
	}

	/**
	 * Clear the state of {@link #isMarkedForRollbackOnly}, when a new transaction begins.
	 */
	public static void clearMarkedForRollbackOnly(ReactiveSharedSessionContractImplementor session) {
		if ( session instanceof ReactiveSessionImpl impl ) {
			impl.clearMarkedForRollbackOnly();
		}
		else if ( session instanceof ReactiveStatelessSessionImpl impl ) {
			impl.clearMarkedForRollbackOnly();
		}
	}

	/**
	 * @return true if the transaction must be rolled back instead of committed
	 */
	public static boolean rollbackOnCommit(ReactiveSharedSessionContractImplementor session) {
		if ( isMarkedForRollbackOnly( session ) ) {
			LOG.onCommitMarkedRollbackOnlyRollingBack();
			return true;
		}
		return false;
	}

	/**
	 * @return the exception to throw after the rollback, or null if the commit should not fail
	 */
	public static RuntimeException commitFailure(SessionFactoryImplementor factory) {
		return factory.getSessionFactoryOptions().getJpaCompliance().isJpaTransactionComplianceEnabled()
				? new RollbackException( "Transaction was marked for rollback only" )
				: null;
	}
}
