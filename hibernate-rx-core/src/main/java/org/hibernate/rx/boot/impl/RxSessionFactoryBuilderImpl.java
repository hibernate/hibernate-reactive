package org.hibernate.rx.boot.impl;

import org.hibernate.boot.SessionFactoryBuilder;
import org.hibernate.boot.spi.AbstractDelegatingSessionFactoryBuilderImplementor;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.boot.spi.SessionFactoryBuilderImplementor;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.rx.RxSessionFactory;
import org.hibernate.rx.boot.RxSessionFactoryBuilder;
import org.hibernate.rx.boot.RxSessionFactoryBuilderImplementor;
import org.hibernate.rx.engine.impl.RxSessionFactoryImpl;

/**
 * Implementation of {@link RxSessionFactoryBuilder}.
 *
 * @see RxSessionFactoryBuilder
 * @see RxSessionFactoryBuilderFactory
 */
public class RxSessionFactoryBuilderImpl
		extends AbstractDelegatingSessionFactoryBuilderImplementor<RxSessionFactoryBuilderImplementor> implements
		RxSessionFactoryBuilderImplementor {

	private final SessionFactoryBuilderImplementor delegate;
	private final MetadataImplementor metadata;

	public RxSessionFactoryBuilderImpl(MetadataImplementor metadata, SessionFactoryBuilderImplementor delegate) {
		super( delegate );

		this.metadata = metadata;
		this.delegate = delegate;
	}

	@Override
	protected RxSessionFactoryBuilderImpl getThis() {
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
	public RxSessionFactory build() {
		RxSessionFactoryOptions options = new RxSessionFactoryOptions( delegate.buildSessionFactoryOptions() );
		return new RxSessionFactoryImpl( new SessionFactoryImpl( metadata, options ) );
	}
}
