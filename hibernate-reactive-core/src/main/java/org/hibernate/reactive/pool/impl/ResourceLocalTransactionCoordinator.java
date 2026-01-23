/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import org.hibernate.Incubating;
import org.hibernate.reactive.pool.ReactiveTransactionCoordinator;

/**
 * Default transaction coordinator implementation where Hibernate Reactive
 * manages the complete transaction lifecycle.
 * <p>
 * In this mode, Hibernate Reactive:
 * <ul>
 *     <li>Begins, commits, and rolls back transactions</li>
 *     <li>Validates that no transaction is active when closing connections</li>
 *     <li>Automatically rolls back and throws an exception if a connection
 *         is closed with an active transaction</li>
 * </ul>
 * <p>
 * This is the standard mode for applications where Hibernate Reactive has
 * full control over transaction boundaries.
 */
@Incubating
public class ResourceLocalTransactionCoordinator implements ReactiveTransactionCoordinator {

	/**
	 * Singleton instance of the resource-local transaction coordinator.
	 */
	public static final ResourceLocalTransactionCoordinator INSTANCE = new ResourceLocalTransactionCoordinator();

	private ResourceLocalTransactionCoordinator() {
		// Singleton - use INSTANCE
	}

	@Override
	public boolean isExternallyManaged() {
		return false;
	}
}
