/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.mutation;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.mutation.DeleteCoordinator;

/**
 * With this interface we can have multiple delete coordinators that extend {@link org.hibernate.persister.entity.mutation.AbstractDeleteCoordinator}.
 *
 * @see ReactiveDeleteCoordinatorSoft
 * @see ReactiveDeleteCoordinatorStandard
 */
public interface ReactiveDeleteCoordinator extends DeleteCoordinator {

	CompletionStage<Void> reactiveDelete(Object entity, Object id, Object version, SharedSessionContractImplementor session);
}
