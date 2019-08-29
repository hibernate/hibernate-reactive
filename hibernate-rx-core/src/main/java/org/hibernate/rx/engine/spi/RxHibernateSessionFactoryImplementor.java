package org.hibernate.rx.engine.spi;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.rx.RxHibernateSession;
import org.hibernate.rx.RxHibernateSessionFactory;

public interface RxHibernateSessionFactoryImplementor extends RxHibernateSessionFactory, SessionFactoryImplementor {

	@Override
	RxHibernateSessionBuilderImplementor withOptions();

	@Override
	RxHibernateSession openRxSession() throws HibernateException;
}
