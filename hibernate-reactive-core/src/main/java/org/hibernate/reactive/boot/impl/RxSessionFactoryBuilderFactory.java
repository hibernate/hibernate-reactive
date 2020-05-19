package org.hibernate.reactive.boot.impl;

import org.hibernate.boot.SessionFactoryBuilder;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.boot.spi.SessionFactoryBuilderFactory;
import org.hibernate.boot.spi.SessionFactoryBuilderImplementor;

/**
 * A {@link java.util.ServiceLoader discoverable service} implementing
 * Hibernate's {@link SessionFactoryBuilderFactory extension point}
 * for integration with the process of building a
 * {@link org.hibernate.SessionFactory}.
 */
public class RxSessionFactoryBuilderFactory implements SessionFactoryBuilderFactory {

	@Override
	public SessionFactoryBuilder getSessionFactoryBuilder(MetadataImplementor metadata, SessionFactoryBuilderImplementor defaultBuilder) {
		return new RxSessionFactoryBuilderImpl( metadata, defaultBuilder );
	}
}
