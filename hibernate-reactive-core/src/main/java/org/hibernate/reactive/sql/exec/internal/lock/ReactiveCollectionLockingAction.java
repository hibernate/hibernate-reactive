/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal.lock;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.Locking;
import org.hibernate.engine.spi.EffectiveEntityGraph;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.query.sqm.mutation.internal.SqmMutationStrategyHelper;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.sql.exec.spi.ReactivePostAction;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.exec.internal.lock.CollectionLockingAction;
import org.hibernate.sql.exec.internal.lock.EntityDetails;
import org.hibernate.sql.exec.internal.lock.LockingHelper;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcSelectWithActionsBuilder;

import jakarta.persistence.Timeout;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.sql.exec.SqlExecLogger.SQL_EXEC_LOGGER;

/**
 * Reactive version of {@link CollectionLockingAction}
 */
public class ReactiveCollectionLockingAction extends CollectionLockingAction implements ReactivePostAction {

	protected ReactiveCollectionLockingAction(
			LoadedValuesCollectorImpl loadedValuesCollector,
			LockMode lockMode,
			Timeout lockTimeout) {
		super( loadedValuesCollector, lockMode, lockTimeout );
	}


	public static void apply(
			LockOptions lockOptions,
			QuerySpec lockingTarget,
			JdbcSelectWithActionsBuilder jdbcSelectBuilder) {
		assert lockOptions.getScope() == Locking.Scope.INCLUDE_COLLECTIONS;

		final var loadedValuesCollector = resolveLoadedValuesCollector( lockingTarget.getFromClause() );

		// NOTE: we need to set this separately so that it can get incorporated into
		// the JdbcValuesSourceProcessingState for proper callbacks
		jdbcSelectBuilder.setLoadedValuesCollector( loadedValuesCollector );

		// additionally, add a post-action which uses the collected values.
		jdbcSelectBuilder.appendPostAction( new ReactiveCollectionLockingAction(
				loadedValuesCollector,
				lockOptions.getLockMode(),
				lockOptions.getTimeout()
		) );
	}

	@Override
	public CompletionStage<Void> reactivePerformReactivePostAction(
			ReactiveConnection jdbcConnection,
			ExecutionContext executionContext) {
		LockingHelper.logLoadedValues( loadedValuesCollector );

		final var session = executionContext.getSession();

		// NOTE: we deal with effective graphs here to make sure embedded associations are treated as lazy
		final var effectiveEntityGraph = session.getLoadQueryInfluencers().getEffectiveEntityGraph();
		final var initialGraph = effectiveEntityGraph.getGraph();
		final var initialSemantic = effectiveEntityGraph.getSemantic();

		// collect registrations by entity type
		final var entitySegments = segmentLoadedValues( loadedValuesCollector );

		try {
			// for each entity-type, prepare a locking select statement per table.
			// this is based on the attributes for "state array" ordering purposes -
			// we match each attribute to the table it is mapped to and add it to
			// the select-list for that table-segment.
			CompletionStage<Void> loop = voidFuture();

			for ( Map.Entry<EntityMappingType, List<EntityKey>> entry : entitySegments.entrySet() ) {
				loop = loop.thenCompose( v -> execute( executionContext, entry, session, effectiveEntityGraph ) );
			}
			return loop;
		}
		finally {
			// reset the effective graph to whatever it was when we started
			effectiveEntityGraph.clear();
			session.getLoadQueryInfluencers().applyEntityGraph( initialGraph, initialSemantic );
		}
	}

	private CompletionStage<Void> execute(
			ExecutionContext executionContext,
			Map.Entry<EntityMappingType, List<EntityKey>> entry,
			SharedSessionContractImplementor session,
			EffectiveEntityGraph effectiveEntityGraph) {
		EntityMappingType entityMappingType = entry.getKey();
		List<EntityKey> entityKeys = entry.getValue();
		if ( SQL_EXEC_LOGGER.isDebugEnabled() ) {
			SQL_EXEC_LOGGER.startingIncludeCollectionsLockingProcess( entityMappingType.getEntityName() );
		}

		// apply an empty "fetch graph" to make sure any embedded associations reachable from
		// any of the DomainResults we will create are treated as lazy
		final var graph = entityMappingType.createRootGraph( session );
		effectiveEntityGraph.clear();
		effectiveEntityGraph.applyGraph( graph, GraphSemantic.FETCH );

		// create a cross-reference of information related to an entity based on its identifier.
		// we use this as the collection owners whose collections need to be locked
		final var entityDetailsMap = LockingHelper.resolveEntityKeys( entityKeys, executionContext );

		final List<Supplier<CompletionStage<Void>>> suppliers = new ArrayList<>();
		SqmMutationStrategyHelper.visitCollectionTables(
				entityMappingType, (attribute) -> {
					// we may need to lock the "collection table".
					// the conditions are a bit unclear as to directionality, etc., so for now lock each.
					suppliers.add( () -> reactiveLockCollectionTable(
							attribute,
							lockMode,
							lockTimeout,
							entityDetailsMap,
							executionContext
					) );
				}
		);
		return CompletionStages.loop( suppliers, Supplier::get );
	}

	public static CompletionStage<Void> reactiveLockCollectionTable(
			PluralAttributeMapping attributeMapping,
			LockMode lockMode,
			Timeout lockTimeout,
			Map<Object, EntityDetails> ownerDetailsMap,
			ExecutionContext executionContext) {
		///  execute query
		return voidFuture();
	}

}
