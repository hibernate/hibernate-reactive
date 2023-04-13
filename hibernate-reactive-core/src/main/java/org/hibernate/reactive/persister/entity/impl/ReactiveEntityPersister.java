/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.impl;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.bytecode.BytecodeLogging;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.loader.ast.spi.MultiIdLoadOptions;
import org.hibernate.persister.entity.EntityPersister;

import jakarta.persistence.metamodel.Attribute;


/**
 * A reactive {@link EntityPersister}. Supports non-blocking insert/update/delete operations.
 *
 * @see ReactiveAbstractEntityPersister
 */
public interface ReactiveEntityPersister extends EntityPersister {

	/**
	 * Insert the given instance state without blocking.
	 *
	 * @see EntityPersister#insert(Object, Object[], Object, SharedSessionContractImplementor)
	 */
	CompletionStage<Void> insertReactive(Object id, Object[] fields, Object object, SharedSessionContractImplementor session);

	/**
	 * Insert the given instance state without blocking.
	 *
	 * @see EntityPersister#insert(Object[], Object, SharedSessionContractImplementor)
	 */
	CompletionStage<Object> insertReactive(Object[] fields, Object object, SharedSessionContractImplementor session);

	/**
	 * Delete the given instance without blocking.
	 *
	 * @see EntityPersister#delete(Object, Object, Object, SharedSessionContractImplementor)
	 */
	CompletionStage<Void> deleteReactive(Object id, Object version, Object object, SharedSessionContractImplementor session);

	/**
	 * Update the given instance state without blocking.
	 *
	 * @see EntityPersister#update(Object, Object[], int[], boolean, Object[], Object, Object, Object, SharedSessionContractImplementor)
	 */
	CompletionStage<Void> updateReactive(
			final Object id,
			final Object[] values,
			int[] dirtyAttributeIndexes,
			final boolean hasDirtyCollection,
			final Object[] oldValues,
			final Object oldVersion,
			final Object object,
			final Object rowId,
			final SharedSessionContractImplementor session);

	/**
	 * Obtain a pessimistic lock without blocking
	 */
	CompletionStage<Void> reactiveLock(
			Object id,
			Object version,
			Object object,
			LockOptions lockOptions,
			SharedSessionContractImplementor session);

	<K> CompletionStage<? extends List<?>> reactiveMultiLoad(
			K[] ids,
			EventSource session,
			MultiIdLoadOptions loadOptions);

	CompletionStage<Object> reactiveLoad(
			Object id,
			Object optionalObject,
			LockMode lockMode,
			SharedSessionContractImplementor session);

	CompletionStage<Object> reactiveLoad(
			Object id,
			Object optionalObject,
			LockOptions lockOptions,
			SharedSessionContractImplementor session);

	CompletionStage<Object> reactiveLoad(
			Object id,
			Object optionalObject,
			LockOptions lockOptions,
			SharedSessionContractImplementor session,
			Boolean readOnly);

	CompletionStage<Object> reactiveLoadByUniqueKey(
			String propertyName,
			Object uniqueKey,
			SharedSessionContractImplementor session);

	CompletionStage<Object> reactiveLoadByUniqueKey(
			String propertyName,
			Object uniqueKey,
			Boolean readOnly,
			SharedSessionContractImplementor session);

	CompletionStage<Object> reactiveLoadEntityIdByNaturalId(
			Object[] naturalIdValues,
			LockOptions lockOptions,
			SharedSessionContractImplementor session);

	CompletionStage<Object> reactiveGetCurrentVersion(Object id, SharedSessionContractImplementor session);

	/**
	 * @see EntityPersister#processInsertGeneratedProperties(Object, Object, Object[], SharedSessionContractImplementor)
	 */
	CompletionStage<Void> reactiveProcessInsertGenerated(
			Object id,
			Object entity,
			Object[] state,
			SharedSessionContractImplementor session);

	/**
	 * @see EntityPersister#processUpdateGeneratedProperties(Object, Object, Object[], SharedSessionContractImplementor)
	 */
	CompletionStage<Void> reactiveProcessUpdateGenerated(
			Object id,
			Object entity,
			Object[] state,
			SharedSessionContractImplementor session);

	/**
	 * Get the current database state of the object, in a "hydrated" form, without resolving identifiers
	 *
	 * @return null if there is no row in the database
	 */
	CompletionStage<Object[]> reactiveGetDatabaseSnapshot(Object id, SharedSessionContractImplementor session);

	<E, T> CompletionStage<T> reactiveInitializeLazyProperty(
			Attribute<E, T> field,
			E entity,
			SharedSessionContractImplementor session);

	<E, T> CompletionStage<T> reactiveInitializeLazyProperty(
			String field,
			E entity,
			SharedSessionContractImplementor session);

	CompletionStage<Object> reactiveInitializeEnhancedEntityUsedAsProxy(
			Object entity,
			String nameOfAttributeBeingAccessed,
			SharedSessionContractImplementor session);

	/**
	 * @see org.hibernate.bytecode.enhance.spi.interceptor.EnhancementAsProxyLazinessInterceptor#forceInitialize(Object, String, SharedSessionContractImplementor, boolean)
	 */
	//TODO find somewhere else for this function to live
	static CompletionStage<Object> forceInitialize(
			Object target,
			String attributeName,
			Object entityId,
			String entityName,
			SharedSessionContractImplementor session) {
		BytecodeLogging.LOGGER.tracef(
				"EnhancementAsProxyLazinessInterceptor#forceInitialize : %s#%s -> %s )",
				entityName,
				entityId,
				attributeName
		);

		final ReactiveEntityPersister persister = (ReactiveEntityPersister)
				session.getFactory().getMappingMetamodel()
						.getEntityDescriptor( entityName );
		return persister.reactiveInitializeEnhancedEntityUsedAsProxy( target, attributeName, session );
	}

}
