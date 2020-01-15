package org.hibernate.rx.persister.impl;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

public interface RxEntityPersister extends EntityPersister {

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
