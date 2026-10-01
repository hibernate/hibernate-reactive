/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.mutation;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;

/**
 * Scoped to a single operation, so that we can keep
 * instance scoped state.
 *
 * @see org.hibernate.persister.entity.mutation.DeleteCoordinator
 * @see ReactiveDeleteCoordinator
 */
public interface ReactiveScopedDeleteCoordinator {

	CompletionStage<Void> reactiveDelete(
			Object entity,
			Object id,
			Object version,
			SharedSessionContractImplementor session);

}
