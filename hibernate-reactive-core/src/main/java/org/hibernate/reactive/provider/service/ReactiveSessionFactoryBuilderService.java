/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.boot.internal.MetadataImpl;
import org.hibernate.boot.internal.SessionFactoryBuilderImpl;
import org.hibernate.boot.internal.SessionFactoryOptionsBuilder;
import org.hibernate.boot.spi.BootstrapContext;
import org.hibernate.boot.spi.SessionFactoryBuilderImplementor;
import org.hibernate.boot.spi.SessionFactoryBuilderService;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.service.spi.Configurable;

import java.util.Map;

/**
 * A {@link SessionFactoryBuilderService} that creates instances of {@link ReactiveSessionFactoryBuilder}.
 */
final class ReactiveSessionFactoryBuilderService implements SessionFactoryBuilderService, Configurable {

	private int batchSize;

	@Override
	public void configure(Map configurationValues) {
		batchSize = ConfigurationHelper.getInt( Settings.STATEMENT_BATCH_SIZE, configurationValues, 0 );
	}

	@Override
	public SessionFactoryBuilderImplementor createSessionFactoryBuilder(final MetadataImpl metadata, final BootstrapContext bootstrapContext) {
		SessionFactoryOptionsBuilder optionsBuilder = new SessionFactoryOptionsBuilder(
				metadata.getMetadataBuildingOptions().getServiceRegistry(),
				bootstrapContext
		);
		optionsBuilder.enableCollectionInDefaultFetchGroup( true );
		optionsBuilder.applyJdbcBatchSize( batchSize );
		return new ReactiveSessionFactoryBuilder(
				metadata,
				new SessionFactoryBuilderImpl( metadata,
											   optionsBuilder,
											   metadata.getTypeConfiguration()
													   .getMetadataBuildingContext()
													   .getBootstrapContext()
				)
		);
	}

}
