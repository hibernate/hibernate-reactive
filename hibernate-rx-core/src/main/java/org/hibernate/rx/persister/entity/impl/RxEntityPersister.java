package org.hibernate.rx.persister.entity.impl;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.RxEntityPersisterImpl;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

public interface RxEntityPersister {

	static RxEntityPersister get(EntityPersister persister) {
		return new RxEntityPersisterImpl((AbstractEntityPersister) persister);
	}

	EntityPersister getPersister();

	CompletionStage<?> insertRx(Serializable id,
			Object[] fields,
			Object object,
			SharedSessionContractImplementor session);

	// Should it return the id?
	CompletionStage<?> insertRx(Object[] fields,
			Object object,
			SharedSessionContractImplementor session)
					throws HibernateException;

	CompletionStage<?> deleteRx(
			Serializable id,
			Object version,
			Object object,
			SharedSessionContractImplementor session)
					throws HibernateException;

	CompletionStage<?> updateRx(Serializable id,
			Object[] fields, int[] dirtyFields,
			boolean hasDirtyCollection,
			Object[] oldFields, Object oldVersion,
			Object object,
			Object rowId,
			SharedSessionContractImplementor session)
					throws HibernateException;
}
