package org.hibernate.rx.engine.spi;

import org.hibernate.engine.spi.SessionBuilderImplementor;
import org.hibernate.rx.RxHibernateSessionFactory;

public interface RxHibernateSessionBuilderImplementor
		extends RxHibernateSessionFactory.RxHibernateSessionBuilder<RxHibernateSessionBuilderImplementor>,
		SessionBuilderImplementor<RxHibernateSessionBuilderImplementor> {
}
