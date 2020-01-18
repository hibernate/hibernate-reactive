package org.hibernate.rx.boot.impl;

import org.hibernate.boot.SessionFactoryBuilder;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.boot.spi.SessionFactoryBuilderFactory;
import org.hibernate.boot.spi.SessionFactoryBuilderImplementor;

public class RxSessionFactoryBuilderFactory implements SessionFactoryBuilderFactory {

	@Override
	public SessionFactoryBuilder getSessionFactoryBuilder(MetadataImplementor metadata, SessionFactoryBuilderImplementor defaultBuilder) {
		return new RxSessionFactoryBuilder( metadata, defaultBuilder );
	}
}
