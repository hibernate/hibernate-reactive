package org.hibernate.reactive.boot.impl;

import org.hibernate.boot.SessionFactoryBuilder;
import org.hibernate.boot.spi.AbstractDelegatingSessionFactoryBuilderImplementor;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.boot.spi.SessionFactoryBuilderImplementor;
import org.hibernate.reactive.boot.ReactiveSessionFactoryBuilder;
import org.hibernate.reactive.boot.ReactiveSessionFactoryBuilderImplementor;
import org.hibernate.reactive.impl.StageSessionFactoryImpl;
import org.hibernate.reactive.stage.Stage;

/**
 * Implementation of {@link ReactiveSessionFactoryBuilder}.
 *
 * @see ReactiveSessionFactoryBuilder
 * @see ReactiveSessionFactoryBuilderFactory
 */
public class ReactiveSessionFactoryBuilderImpl
		extends AbstractDelegatingSessionFactoryBuilderImplementor<ReactiveSessionFactoryBuilderImplementor> implements
		ReactiveSessionFactoryBuilderImplementor {

	private final SessionFactoryBuilderImplementor delegate;
	private final MetadataImplementor metadata;

	public ReactiveSessionFactoryBuilderImpl(MetadataImplementor metadata, SessionFactoryBuilderImplementor delegate) {
		super( delegate );

		this.metadata = metadata;
		this.delegate = delegate;
	}

	@Override
	protected ReactiveSessionFactoryBuilderImpl getThis() {
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
	public Stage.SessionFactory build() {
		ReactiveSessionFactoryOptions options = new ReactiveSessionFactoryOptions( delegate.buildSessionFactoryOptions() );
		return new StageSessionFactoryImpl( metadata, options );
	}
}
