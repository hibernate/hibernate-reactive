/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool;

import org.hibernate.Incubating;

/**
 * Coordinates transaction management for reactive connections, similar to
 * Hibernate ORM's {@link org.hibernate.resource.transaction.spi.TransactionCoordinator}.
 * <p>
 * This abstraction allows transactions to be managed either by Hibernate Reactive
 * itself (resource-local mode) or by an external framework (managed mode), such as
 * Quarkus managing transactions at a higher level.
 * <p>
 * When transactions are externally managed, Hibernate Reactive delegates the
 * transaction lifecycle management to the external coordinator.
 *
 * @see org.hibernate.reactive.pool.impl.ResourceLocalTransactionCoordinator
 */
@Incubating
public interface ReactiveTransactionCoordinator {

	/**
	 * Indicates whether transactions are being handled externally to Hibernate Reactive.
	 * <p>
	 * The external coordinator is responsible for:
	 * <ul>
	 *     <li>Beginning transactions before providing connections</li>
	 *     <li>Committing or rolling back transactions on session close</li>
	 *     <li>Properly closing physical connections</li>
	 * </ul>
	 *
	 * @return {@code true} if transactions are externally managed, {@code false} otherwise
	 */
	boolean isExternallyManaged();
}
