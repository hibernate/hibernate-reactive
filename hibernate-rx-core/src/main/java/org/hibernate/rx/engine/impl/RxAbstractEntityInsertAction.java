package org.hibernate.rx.engine.impl;

import org.hibernate.engine.internal.NonNullableTransientDependencies;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.rx.engine.spi.RxExecutable;

import java.io.Serializable;

public interface RxAbstractEntityInsertAction extends RxExecutable, Comparable, Serializable {
	boolean isEarlyInsert();
	NonNullableTransientDependencies findNonNullableTransientEntities();
	SharedSessionContractImplementor getSession();
	boolean isVeto();
	void makeEntityManaged();
	Object getInstance();
	String getEntityName();
	Object[] getState();
	EntityPersister getPersister();
}
