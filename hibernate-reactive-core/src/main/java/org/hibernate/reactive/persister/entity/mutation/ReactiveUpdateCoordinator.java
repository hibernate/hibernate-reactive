/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.mutation;

import org.hibernate.persister.entity.mutation.UpdateCoordinator;

public interface ReactiveUpdateCoordinator extends UpdateCoordinator {

	ReactiveScopedUpdateCoordinator makeScopedCoordinator();

}
