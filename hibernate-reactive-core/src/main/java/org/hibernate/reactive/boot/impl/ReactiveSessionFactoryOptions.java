package org.hibernate.reactive.boot.impl;

import org.hibernate.boot.spi.AbstractDelegatingSessionFactoryOptions;
import org.hibernate.boot.spi.SessionFactoryOptions;

public class ReactiveSessionFactoryOptions extends AbstractDelegatingSessionFactoryOptions {

	public ReactiveSessionFactoryOptions(SessionFactoryOptions delegate) {
		super( delegate );
	}

}
