package org.hibernate.rx.boot.impl;

import org.hibernate.boot.SessionFactoryBuilder;
import org.hibernate.boot.internal.BootstrapContextImpl;
import org.hibernate.boot.spi.AbstractDelegatingSessionFactoryBuilderImplementor;
import org.hibernate.boot.spi.BootstrapContext;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.boot.spi.SessionFactoryBuilderImplementor;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.rx.RxHibernateSessionFactory;
import org.hibernate.rx.boot.RxHibernateSessionFactoryBuilderImplementor;
import org.hibernate.rx.engine.impl.RxHibernateSessionFactoryImpl;

public class RxHibernateSessionFactoryBuilder
		extends AbstractDelegatingSessionFactoryBuilderImplementor<RxHibernateSessionFactoryBuilderImplementor> implements
		RxHibernateSessionFactoryBuilderImplementor {

	private final SessionFactoryBuilderImplementor delegate;
	private final MetadataImplementor metadata;

	public RxHibernateSessionFactoryBuilder(MetadataImplementor metadata, SessionFactoryBuilderImplementor delegate) {
		super( delegate );

		this.metadata = metadata;
		this.delegate = delegate;
	}

	@Override
	protected RxHibernateSessionFactoryBuilder getThis() {
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
	public RxHibernateSessionFactory build() {
		RxHibernateSessionFactoryOptions options = new RxHibernateSessionFactoryOptions( delegate.buildSessionFactoryOptions() );
		return new RxHibernateSessionFactoryImpl( new SessionFactoryImpl( metadata, options ) );
	}
}
