package org.hibernate.rx.persister.entity.impl;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.EntityPersister;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

public interface RxEntityPersister {

	static RxEntityPersister get(EntityPersister persister) {
		return new RxEntityPersisterImpl((AbstractEntityPersister) persister);
	}

	EntityPersister getPersister();

	RxIdentifierGenerator getIdentifierGenerator();

	CompletionStage<?> insertRx(Serializable id,
			Object[] fields,
			Object object,
			SharedSessionContractImplementor session);

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
