/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.SessionFactory;
import org.hibernate.boot.spi.AbstractDelegatingSessionFactoryBuilderImplementor;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.boot.spi.SessionFactoryBuilderImplementor;
import org.hibernate.reactive.session.impl.ReactiveSessionFactoryImpl;

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
	public SessionFactory build() {
		return new ReactiveSessionFactoryImpl(
				metadata,
				delegate.buildSessionFactoryOptions(),
				metadata.getTypeConfiguration()
						.getMetadataBuildingContext()
						.getBootstrapContext()
		);
	}
}
