/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.SessionFactory;
import org.hibernate.boot.SessionFactoryBuilder;
import org.hibernate.boot.spi.AbstractDelegatingSessionFactoryBuilderImplementor;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.boot.spi.SessionFactoryBuilderImplementor;
import org.hibernate.reactive.session.impl.ReactiveSessionFactoryImpl;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.stage.Stage;

/**
 * A {@link SessionFactoryBuilder} for {@link SessionFactory}
 * instances that can be {@link #unwrap(Class) unwrapped} to
 * produce a {@link Stage.SessionFactory} or
 * {@link Mutiny.SessionFactory}.
 *
 * @see ReactiveSessionFactoryBuilderFactory
 */
public class ReactiveSessionFactoryBuilder
		extends AbstractDelegatingSessionFactoryBuilderImplementor<ReactiveSessionFactoryBuilder> {

	private final SessionFactoryBuilderImplementor delegate;
	private final MetadataImplementor metadata;

	public ReactiveSessionFactoryBuilder(MetadataImplementor metadata, SessionFactoryBuilderImplementor delegate) {
		super( delegate );

		this.metadata = metadata;
		this.delegate = delegate;
	}

	@Override
	protected ReactiveSessionFactoryBuilder getThis() {
		return this;
	}

	@Override
	public <T extends SessionFactoryBuilder> T unwrap(Class<T> type) {
		if ( type.isAssignableFrom( getClass() ) ) {
			return type.cast( this );
		}
		else {
			return delegate.unwrap( type );
		}
	}

	@Override
	public SessionFactory build() {
		return new ReactiveSessionFactoryImpl( metadata, delegate.buildSessionFactoryOptions() );
	}

}
