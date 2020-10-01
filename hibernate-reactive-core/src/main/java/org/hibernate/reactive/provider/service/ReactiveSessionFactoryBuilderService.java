/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.boot.internal.MetadataImpl;
import org.hibernate.boot.internal.SessionFactoryBuilderImpl;
import org.hibernate.boot.internal.SessionFactoryOptionsBuilder;
import org.hibernate.boot.spi.BootstrapContext;
import org.hibernate.boot.spi.SessionFactoryBuilderImplementor;
import org.hibernate.boot.spi.SessionFactoryBuilderService;
import org.hibernate.reactive.bulk.impl.ReactiveBulkIdStrategy;

final class ReactiveSessionFactoryBuilderService implements SessionFactoryBuilderService {

	@Override
	public SessionFactoryBuilderImplementor createSessionFactoryBuilder(final MetadataImpl metadata, final BootstrapContext bootstrapContext) {
		SessionFactoryOptionsBuilder optionsBuilder = new SessionFactoryOptionsBuilder(
				metadata.getMetadataBuildingOptions().getServiceRegistry(),
				bootstrapContext
		);
		optionsBuilder.enableCollectionInDefaultFetchGroup(true);
		optionsBuilder.applyMultiTableBulkIdStrategy( new ReactiveBulkIdStrategy( metadata ) );
		return new ReactiveSessionFactoryBuilder( metadata, new SessionFactoryBuilderImpl( metadata, optionsBuilder ) );
	}

}
