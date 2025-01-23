/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.spi;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.EntityMappingType;

/**
 * Reactive version of {@link org.hibernate.loader.ast.spi.AfterLoadAction}
 */
public interface ReactiveAfterLoadAction {
	/**
	 * @see org.hibernate.loader.ast.spi.AfterLoadAction#afterLoad(Object, EntityMappingType, SharedSessionContractImplementor)
	 *
	 * The action trigger - the {@code entity} is being loaded
	 */
	CompletionStage<Void> reactiveAfterLoad(
			Object entity,
			EntityMappingType entityMappingType,
			SharedSessionContractImplementor session);
}
