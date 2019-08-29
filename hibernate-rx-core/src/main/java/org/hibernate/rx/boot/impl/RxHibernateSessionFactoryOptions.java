package org.hibernate.rx.boot.impl;

import org.hibernate.boot.spi.AbstractDelegatingSessionFactoryOptions;
import org.hibernate.boot.spi.SessionFactoryOptions;

public class RxHibernateSessionFactoryOptions extends AbstractDelegatingSessionFactoryOptions {

	public RxHibernateSessionFactoryOptions(SessionFactoryOptions delegate) {
		super( delegate );
	}

}
