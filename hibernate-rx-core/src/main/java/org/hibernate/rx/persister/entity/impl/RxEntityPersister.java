package org.hibernate.rx.persister.entity.impl;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.MultiLoadOptions;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * A reactive {@link EntityPersister}. Supports non-blocking
 * insert/update/delete operations.
 *
 * @see RxAbstractEntityPersister
 */
public interface RxEntityPersister extends EntityPersister {

	//TODO: we only support Long for now, but eventually
	//      we need to do something more general
	RxIdentifierGenerator<?> getRxIdentifierGenerator();
	
	/**
	 * Insert the given instance state without blocking.
	 *
	 * @see EntityPersister#insert(Serializable, Object[], Object, SharedSessionContractImplementor)
	 */
	CompletionStage<?> insertRx(
			Serializable id,
			Object[] fields,
			Object object,
			SharedSessionContractImplementor session);

	/**
	 * Insert the given instance state without blocking.
	 *
	 * @see EntityPersister#insert(Object[], Object, SharedSessionContractImplementor)
	 */
	CompletionStage<Serializable> insertRx(
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
	CompletionStage<?> updateRx(
			Serializable id,
			Object[] fields, int[] dirtyFields,
			boolean hasDirtyCollection,
			Object[] oldFields, Object oldVersion,
			Object object,
			Object rowId,
			SharedSessionContractImplementor session)
					throws HibernateException;

	 CompletionStage<List<?>> rxMultiLoad(
	 		Serializable[] ids,
			SessionImplementor session,
			MultiLoadOptions loadOptions);

	/**
	 * Reactive {@link #setPropertyValues(Object, Object[])}
	 * <p>
	 *     If a value is a {@link CompletionStage} makes sure it completes before setting the value in the object.
	 *     If a value is an {@link java.util.Optional} set the underlying value or null if one is not present.
	 * </p>
	 * @return A {@link CompletionStage} that completes when all values are set in the object
	 */
	CompletionStage<Void> setRxPropertyValues(Object object, Object[] values);
}
