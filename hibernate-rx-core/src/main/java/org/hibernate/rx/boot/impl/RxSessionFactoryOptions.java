package org.hibernate.rx.boot.impl;

import org.hibernate.boot.spi.AbstractDelegatingSessionFactoryOptions;
import org.hibernate.boot.spi.SessionFactoryOptions;

public class RxSessionFactoryOptions extends AbstractDelegatingSessionFactoryOptions {

	public RxSessionFactoryOptions(SessionFactoryOptions delegate) {
		super( delegate );
	}

}
