/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.mutation;

import org.hibernate.persister.entity.mutation.DeleteCoordinator;

/**
 * A reactive {@link DeleteCoordinator} that allows the creation of a {@link ReactiveScopedDeleteCoordinator} scoped
 * to a single delete operation.
 */
public interface ReactiveDeleteCoordinator extends DeleteCoordinator {

	ReactiveScopedDeleteCoordinator makeScopedCoordinator();
}
