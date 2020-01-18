package org.hibernate.rx.engine.spi;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.RxSessionFactory;

public interface RxSessionFactoryImplementor extends RxSessionFactory, SessionFactoryImplementor {

	@Override
	RxSessionBuilderImplementor withOptions();

	@Override
	RxSession openRxSession() throws HibernateException;
}
