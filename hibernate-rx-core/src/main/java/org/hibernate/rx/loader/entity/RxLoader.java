package org.hibernate.rx.loader.entity;

import java.io.Serializable;

import org.hibernate.HibernateException;
import org.hibernate.LockOptions;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.entity.UniqueEntityLoader;
import org.hibernate.rx.boot.RxHibernateSessionFactoryBuilder;

public class RxLoader implements UniqueEntityLoader {

	@Override
	public Object load(
			Serializable id, Object optionalObject, SharedSessionContractImplementor session)
			throws HibernateException {
		return null;
	}

	@Override
	public Object load(
			Serializable id, Object optionalObject, SharedSessionContractImplementor session, LockOptions lockOptions) {
		return null;
	}
}
