/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.boot.internal.MetadataImpl;
import org.hibernate.boot.internal.SessionFactoryBuilderImpl;
import org.hibernate.boot.spi.BootstrapContext;
import org.hibernate.boot.spi.SessionFactoryBuilderImplementor;
import org.hibernate.boot.spi.SessionFactoryBuilderService;

final class ReactiveSessionFactoryBuilderService implements SessionFactoryBuilderService {

	@Override
	public SessionFactoryBuilderImplementor createSessionFactoryBuilder(final MetadataImpl metadata, final BootstrapContext bootstrapContext) {
		final SessionFactoryBuilderImpl defaultBuilder = new SessionFactoryBuilderImpl( metadata, bootstrapContext );
		final SessionFactoryBuilderImplementor reactiveSessionFactoryBuilder = new ReactiveSessionFactoryBuilder( metadata, defaultBuilder );
		return reactiveSessionFactoryBuilder;
	}

}
