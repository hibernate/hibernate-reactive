package org.hibernate.rx;

import org.hibernate.SessionBuilder;
import org.hibernate.SessionFactory;
import org.hibernate.rx.engine.spi.RxHibernateSessionBuilderImplementor;

/**
 * Factory for {@link RxHibernateSession}
 */
public interface RxHibernateSessionFactory extends SessionFactory {

	RxHibernateSession openRxSession();

	@Override
	RxHibernateSessionBuilderImplementor withOptions();

	interface RxHibernateSessionBuilder<T extends RxHibernateSessionBuilder> extends SessionBuilder<T> {

		RxHibernateSession openRxSession();
	}
}
