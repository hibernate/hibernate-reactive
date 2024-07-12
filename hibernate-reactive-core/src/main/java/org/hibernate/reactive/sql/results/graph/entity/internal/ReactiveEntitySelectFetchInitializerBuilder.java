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
import org.hibernate.reactive.sql.results.graph.embeddable.internal.ReactiveEmbeddableForeignKeyResultImpl;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.InitializerParent;
import org.hibernate.sql.results.graph.embeddable.EmbeddableInitializer;
import org.hibernate.sql.results.graph.embeddable.internal.EmbeddableForeignKeyResultImpl;
import org.hibernate.sql.results.graph.entity.EntityInitializer;
import org.hibernate.sql.results.graph.entity.internal.BatchEntityInsideEmbeddableSelectFetchInitializer;
import org.hibernate.sql.results.graph.entity.internal.BatchEntitySelectFetchInitializer;
import org.hibernate.sql.results.graph.entity.internal.BatchInitializeEntitySelectFetchInitializer;

/**
 * @see org.hibernate.sql.results.graph.entity.internal.EntitySelectFetchInitializerBuilder
 */
public class ReactiveEntitySelectFetchInitializerBuilder {

	public static EntityInitializer<?> createInitializer(
			InitializerParent<?> parent,
			ToOneAttributeMapping fetchedAttribute,
			EntityPersister entityPersister,
			DomainResult<?> originalKeyResult,
			NavigablePath navigablePath,
			boolean selectByUniqueKey,
			boolean affectedByFilter,
			AssemblerCreationState creationState) {
		final DomainResult<?> keyResult = originalKeyResult instanceof EmbeddableForeignKeyResultImpl
				? new ReactiveEmbeddableForeignKeyResultImpl<>( (EmbeddableForeignKeyResultImpl<?>) originalKeyResult )
				: originalKeyResult;
		if ( selectByUniqueKey ) {
			return new ReactiveEntitySelectFetchByUniqueKeyInitializer(
					parent,
					fetchedAttribute,
					navigablePath,
					entityPersister,
					keyResult,
					affectedByFilter,
					creationState
			);
		}
		final BatchMode batchMode = determineBatchMode( entityPersister, parent, creationState );
		switch ( batchMode ) {
			case NONE:
				return new ReactiveEntitySelectFetchInitializer<>(
						parent,
						fetchedAttribute,
						navigablePath,
						entityPersister,
						keyResult,
						affectedByFilter,
						creationState
				);
			case BATCH_LOAD:
				if ( parent.isEmbeddableInitializer() ) {
					return new BatchEntityInsideEmbeddableSelectFetchInitializer(
							parent,
							fetchedAttribute,
							navigablePath,
							entityPersister,
							keyResult,
							affectedByFilter,
							creationState
					);
				}
				else {
					return new BatchEntitySelectFetchInitializer(
							parent,
							fetchedAttribute,
							navigablePath,
							entityPersister,
							keyResult,
							affectedByFilter,
							creationState
					);
				}
			case BATCH_INITIALIZE:
				return new BatchInitializeEntitySelectFetchInitializer(
						parent,
						fetchedAttribute,
						navigablePath,
						entityPersister,
						keyResult,
						affectedByFilter,
						creationState
				);
		}
		throw new IllegalStateException( "Should be unreachable" );
	}

	// FIXME: Use the one in ORM
	public static BatchMode determineBatchMode(
			EntityPersister entityPersister,
			InitializerParent<?> parent,
			AssemblerCreationState creationState) {
		if ( !entityPersister.isBatchLoadable() ) {
			return BatchMode.NONE;
		}
		if ( creationState.isDynamicInstantiation() ) {
			if ( canBatchInitializeBeUsed( entityPersister ) ) {
				return BatchMode.BATCH_INITIALIZE;
			}
			return BatchMode.NONE;
		}
		while ( parent.isEmbeddableInitializer() ) {
			final EmbeddableInitializer<?> embeddableInitializer = parent.asEmbeddableInitializer();
			final EmbeddableValuedModelPart initializedPart = embeddableInitializer.getInitializedPart();
			// For entity identifier mappings we can't batch load,
			// because the entity identifier needs the instance in the resolveKey phase,
			// but batch loading is inherently executed out of order
			if ( initializedPart.isEntityIdentifierMapping()
					// todo: check if the virtual check is necessary
					|| initializedPart.isVirtual()
					|| initializedPart.getMappedType().isPolymorphic()
					// If the parent embeddable has a custom instantiator,
					// we can't inject entities later through setValues()
					|| !( initializedPart.getMappedType().getRepresentationStrategy().getInstantiator()
					instanceof StandardEmbeddableInstantiator ) ) {
				return entityPersister.hasSubclasses() ? BatchMode.NONE : BatchMode.BATCH_INITIALIZE;
			}
			parent = parent.getParent();
			if ( parent == null ) {
				break;
			}
		}
		if ( parent != null ) {
			assert parent.getInitializedPart() instanceof EntityValuedModelPart;
			final EntityPersister parentPersister = parent.asEntityInitializer().getEntityDescriptor();
			final EntityDataAccess cacheAccess = parentPersister.getCacheAccessStrategy();
			if ( cacheAccess != null ) {
				// Do batch initialization instead of batch loading if the parent entity is cacheable
				// to avoid putting entity state into the cache at a point when the association is not yet set
				if ( canBatchInitializeBeUsed( entityPersister ) ) {
					return BatchMode.BATCH_INITIALIZE;
				}
				return BatchMode.NONE;
			}
		}
		return BatchMode.BATCH_LOAD;
	}

	private static boolean canBatchInitializeBeUsed(EntityPersister entityPersister) {
		//  we need to create a Proxy in order to use batch initialize
		return entityPersister.getRepresentationStrategy().getProxyFactory() != null;
	}

	private enum BatchMode {
		NONE,
		BATCH_LOAD,
		BATCH_INITIALIZE
	}

}
