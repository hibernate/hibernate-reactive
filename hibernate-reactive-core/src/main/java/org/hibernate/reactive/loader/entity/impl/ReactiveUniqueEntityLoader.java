package org.hibernate.reactive.loader.entity.impl;

import org.hibernate.HibernateException;
import org.hibernate.LockOptions;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.entity.UniqueEntityLoader;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

public interface ReactiveUniqueEntityLoader extends UniqueEntityLoader {

	@Override
	CompletionStage<Object> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session);

	@Override
	CompletionStage<Object> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session, LockOptions lockOptions);

	@Override
	default CompletionStage<Object> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session, Boolean readOnly) throws HibernateException {
		return load( id, optionalObject, session );
	}

	@Override
	default CompletionStage<Object> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session, LockOptions lockOptions, Boolean readOnly) {
		return load( id, optionalObject, session, lockOptions );
	}
}
