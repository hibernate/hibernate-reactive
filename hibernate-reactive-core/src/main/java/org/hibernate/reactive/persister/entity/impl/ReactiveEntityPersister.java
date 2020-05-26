/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.impl;

import org.hibernate.HibernateException;
import org.hibernate.LockOptions;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.MultiLoadOptions;
import org.hibernate.reactive.loader.entity.ReactiveUniqueEntityLoader;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * A reactive {@link EntityPersister}. Supports non-blocking
 * insert/update/delete operations.
 *
 * @see ReactiveAbstractEntityPersister
 */
public interface ReactiveEntityPersister extends EntityPersister {

	/**
	 * Insert the given instance state without blocking.
	 *
	 * @see EntityPersister#insert(Serializable, Object[], Object, SharedSessionContractImplementor)
	 */
	CompletionStage<?> insertReactive(
			Serializable id,
			Object[] fields,
			Object object,
			SharedSessionContractImplementor session);

	/**
	 * Insert the given instance state without blocking.
	 *
	 * @see EntityPersister#insert(Object[], Object, SharedSessionContractImplementor)
	 */
	CompletionStage<Serializable> insertReactive(
			Object[] fields,
			Object object,
			SharedSessionContractImplementor session);

	/**
	 * Delete the given instance without blocking.
	 *
	 * @see EntityPersister#delete(Serializable, Object, Object, SharedSessionContractImplementor)
	 */
	CompletionStage<?> deleteReactive(
			Serializable id,
			Object version,
			Object object,
			SharedSessionContractImplementor session)
					throws HibernateException;

	/**
	 * Update the given instance state without blocking.
	 *
	 * @see EntityPersister#update(Serializable, Object[], int[], boolean, Object[], Object, Object, Object, SharedSessionContractImplementor)
	 */
	CompletionStage<?> updateReactive(
			Serializable id,
			Object[] fields, int[] dirtyFields,
			boolean hasDirtyCollection,
			Object[] oldFields, Object oldVersion,
			Object object,
			Object rowId,
			SharedSessionContractImplementor session)
					throws HibernateException;

	CompletionStage<List<Object>> reactiveMultiLoad(
	 		Serializable[] ids,
			SessionImplementor session,
			MultiLoadOptions loadOptions);

	CompletionStage<Object> reactiveLoad(Serializable id, Object optionalObject, LockOptions lockOptions, SharedSessionContractImplementor session);

	CompletionStage<Object> reactiveLoad(Serializable id, Object optionalObject, LockOptions lockOptions, SharedSessionContractImplementor session, Boolean readOnly);

	ReactiveUniqueEntityLoader getAppropriateLoader(LockOptions lockOptions, SharedSessionContractImplementor session);

	CompletionStage<Boolean> reactiveIsTransient(Object entity, SessionImplementor session);
}
