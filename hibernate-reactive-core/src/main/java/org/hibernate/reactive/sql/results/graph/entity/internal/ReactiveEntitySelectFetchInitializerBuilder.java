/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.entity.internal;

import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.metamodel.internal.StandardEmbeddableInstantiator;
import org.hibernate.metamodel.mapping.EmbeddableValuedModelPart;
import org.hibernate.metamodel.mapping.EntityValuedModelPart;
import org.hibernate.metamodel.mapping.internal.ToOneAttributeMapping;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.FetchParentAccess;
import org.hibernate.sql.results.graph.embeddable.EmbeddableInitializer;
import org.hibernate.sql.results.graph.entity.EntityInitializer;
import org.hibernate.sql.results.graph.entity.internal.BatchEntityInsideEmbeddableSelectFetchInitializer;
import org.hibernate.sql.results.graph.entity.internal.BatchEntitySelectFetchInitializer;
import org.hibernate.sql.results.graph.entity.internal.BatchInitializeEntitySelectFetchInitializer;

/**
 * @see org.hibernate.sql.results.graph.entity.internal.EntitySelectFetchInitializerBuilder
 */
public class ReactiveEntitySelectFetchInitializerBuilder {

	public static EntityInitializer createInitializer(
			FetchParentAccess parentAccess,
			ToOneAttributeMapping fetchedAttribute,
			EntityPersister entityPersister,
			DomainResult<?> keyResult,
			NavigablePath navigablePath,
			boolean selectByUniqueKey,
			AssemblerCreationState creationState) {
		if ( selectByUniqueKey ) {
			return new ReactiveEntitySelectFetchByUniqueKeyInitializer(
					parentAccess,
					fetchedAttribute,
					navigablePath,
					entityPersister,
					keyResult.createResultAssembler( parentAccess, creationState )
			);
		}
		final BatchMode batchMode = determineBatchMode( entityPersister, parentAccess, creationState );
		switch ( batchMode ) {
			case NONE:
				return new ReactiveEntitySelectFetchInitializer(
						parentAccess,
						fetchedAttribute,
						navigablePath,
						entityPersister,
						keyResult.createResultAssembler( parentAccess, creationState )
				);
			case BATCH_LOAD:
				if ( parentAccess.isEmbeddableInitializer() ) {
					return new BatchEntityInsideEmbeddableSelectFetchInitializer(
							parentAccess,
							fetchedAttribute,
							navigablePath,
							entityPersister,
							keyResult.createResultAssembler( parentAccess, creationState )
					);
				}
				else {
					return new BatchEntitySelectFetchInitializer(
							parentAccess,
							fetchedAttribute,
							navigablePath,
							entityPersister,
							keyResult.createResultAssembler( parentAccess, creationState )
					);
				}
			case BATCH_INITIALIZE:
				return new BatchInitializeEntitySelectFetchInitializer(
						parentAccess,
						fetchedAttribute,
						navigablePath,
						entityPersister,
						keyResult.createResultAssembler( parentAccess, creationState )
				);
		}
		throw new IllegalStateException( "Should be unreachable" );
	}

	// FIXME: Use the one in ORM
	private static BatchMode determineBatchMode(
			EntityPersister entityPersister,
			FetchParentAccess parentAccess,
			AssemblerCreationState creationState) {
		if ( !entityPersister.isBatchLoadable() || creationState.isScrollResult() ) {
			return BatchMode.NONE;
		}
		while ( parentAccess.isEmbeddableInitializer() ) {
			final EmbeddableInitializer embeddableInitializer = parentAccess.asEmbeddableInitializer();
			final EmbeddableValuedModelPart initializedPart = embeddableInitializer.getInitializedPart();
			// For entity identifier mappings we can't batch load,
			// because the entity identifier needs the instance in the resolveKey phase,
			// but batch loading is inherently executed out of order
			if ( initializedPart.isEntityIdentifierMapping()
					// todo: check if the virtual check is necessary
					|| initializedPart.isVirtual()
					// If the parent embeddable has a custom instantiator, we can't inject entities later through setValues()
					|| !( initializedPart.getMappedType().getRepresentationStrategy().getInstantiator() instanceof StandardEmbeddableInstantiator ) ) {
				return entityPersister.hasSubclasses() ? BatchMode.NONE : BatchMode.BATCH_INITIALIZE;
			}
			parentAccess = parentAccess.getFetchParentAccess();
			if ( parentAccess == null ) {
				break;
			}
		}
		if ( parentAccess != null ) {
			assert parentAccess.getInitializedPart() instanceof EntityValuedModelPart;
			final EntityPersister parentPersister = parentAccess.asEntityInitializer().getEntityDescriptor();
			final EntityDataAccess cacheAccess = parentPersister.getCacheAccessStrategy();
			if ( cacheAccess != null ) {
				// Do batch initialization instead of batch loading if the parent entity is cacheable
				// to avoid putting entity state into the cache at a point when the association is not yet set
				return BatchMode.BATCH_INITIALIZE;
			}
		}
		return BatchMode.BATCH_LOAD;
	}

	enum BatchMode {
		NONE,
		BATCH_LOAD,
		BATCH_INITIALIZE
	}

}
