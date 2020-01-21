package org.hibernate.rx.persister.entity.impl;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.RxEntityPersisterImpl;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

/**
 * A reactive decorator for an {@link AbstractEntityPersister}.
 * Supports non-blocking insert/update/delete operations.
 */
public interface RxEntityPersister {

	static RxEntityPersister get(EntityPersister persister) {
		return new RxEntityPersisterImpl((AbstractEntityPersister) persister);
	}

	EntityPersister getPersister();

	RxIdentifierGenerator getIdentifierGenerator();

	/**
	 * Insert the given instance state without blocking.
	 * 
	 * @see EntityPersister#insert(Serializable, Object[], Object, SharedSessionContractImplementor)
	 */
	CompletionStage<?> insertRx(Serializable id,
			Object[] fields,
			Object object,
			SharedSessionContractImplementor session);

	/**
	 * Delete the given instance without blocking.
	 *
	 * @see EntityPersister#delete(Serializable, Object, Object, SharedSessionContractImplementor) 
	 */
	CompletionStage<?> deleteRx(
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
	CompletionStage<?> updateRx(Serializable id,
			Object[] fields, int[] dirtyFields,
			boolean hasDirtyCollection,
			Object[] oldFields, Object oldVersion,
			Object object,
			Object rowId,
			SharedSessionContractImplementor session)
					throws HibernateException;
}
