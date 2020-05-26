/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.entity;

import org.hibernate.LockOptions;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.entity.UniqueEntityLoader;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

/**
 * A reactific {@link UniqueEntityLoader}, the contract implemented
 * by all reactive entity loaders, including batch loaders.
 *
 * @author Gavin King
 */
public interface ReactiveUniqueEntityLoader extends UniqueEntityLoader {

	@Override
	CompletionStage<Object> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session);

	@Override
	CompletionStage<Object> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session, LockOptions lockOptions);

	@Override
	CompletionStage<Object> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session, Boolean readOnly);

	@Override
	CompletionStage<Object> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session, LockOptions lockOptions, Boolean readOnly);
}
