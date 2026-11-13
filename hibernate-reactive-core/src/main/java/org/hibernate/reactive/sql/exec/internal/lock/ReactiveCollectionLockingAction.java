/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal.lock;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.concurrent.CompletionStage;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.query.sqm.mutation.internal.SqmMutationStrategyHelper;
import org.hibernate.reactive.logging.internal.Log;
import org.hibernate.reactive.logging.internal.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.sql.exec.spi.ReactivePostAction;
import org.hibernate.sql.ast.spi.query.select.QuerySpec;
import org.hibernate.sql.exec.internal.lock.CollectionLockingAction;
import org.hibernate.sql.exec.internal.lock.LockingHelper;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcSelectWithActionsBuilder;
import org.hibernate.sql.exec.spi.LoadedValuesCollector;
import org.hibernate.sql.exec.spi.StatementAccess;

import jakarta.persistence.PessimisticLockScope;
import jakarta.persistence.Timeout;

import static org.hibernate.reactive.util.internal.CompletionStages.loop;
import static org.hibernate.sql.exec.SqlExecLogger.SQL_EXEC_LOGGER;

/**
 * Reactive version of {@link CollectionLockingAction}
 */
public class ReactiveCollectionLockingAction extends CollectionLockingAction implements ReactivePostAction {
	public static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	protected ReactiveCollectionLockingAction(LockMode lockMode, Timeout lockTimeout) {
		super( lockMode, lockTimeout );
	}

	public static void apply(
			LockOptions lockOptions,
			QuerySpec lockingTarget,
			JdbcSelectWithActionsBuilder jdbcSelectBuilder) {
		assert lockOptions.getScope() == PessimisticLockScope.EXTENDED;

		// NOTE: we need to set this separately so that it can get incorporated into
		// the JdbcValuesSourceProcessingState for proper callbacks
		jdbcSelectBuilder.setLoadedValuesCollectorFactory(
				lockOptions.getLockMode() == LockMode.PESSIMISTIC_FORCE_INCREMENT
						? new ReactiveLoadedValuesCollector.Factory( lockingTarget.getRootPathsForLocking() )
						: resolveLoadedValuesCollectorFactory( lockingTarget ) );

		// additionally, add a post-action which uses the collected values.
		jdbcSelectBuilder.appendPostAction( new ReactiveCollectionLockingAction(
				lockOptions.getLockMode(),
				lockOptions.getTimeout()
		) );
	}

	@Override
	public void performPostAction(
			StatementAccess jdbcStatementAccess,
			Connection jdbcConnection,
			ExecutionContext executionContext,
			LoadedValuesCollector loadedValuesCollector) {
		throw LOG.nonReactiveMethodCall( "reactivePerformReactivePostAction()" );
	}

	@Override
	public CompletionStage<Void> reactivePerformReactivePostAction(
			ReactiveConnection jdbcConnection,
			ExecutionContext executionContext,
			LoadedValuesCollector loadedValuesCollector) {
		final var completion = ReactiveDirectLockingCompletion.from( loadedValuesCollector );
		return completion == null
				? lockCollections( executionContext, loadedValuesCollector )
				: completion.complete( () -> lockCollections( executionContext, loadedValuesCollector ), executionContext.getSession() );
	}

	private CompletionStage<Void> lockCollections(ExecutionContext executionContext, LoadedValuesCollector loadedValuesCollector) {
		LockingHelper.logLoadedValues( loadedValuesCollector );

		final var session = executionContext.getSession();

		// NOTE: we deal with effective graphs here to make sure embedded associations are treated as lazy
		final var effectiveEntityGraph = session.getLoadQueryInfluencers().getEffectiveEntityGraph();
		final var initialGraph = effectiveEntityGraph.getGraph();
		final var initialSemantic = effectiveEntityGraph.getSemantic();

		// collect registrations by entity type
		final var entitySegments = segmentLoadedValues( loadedValuesCollector );

		// for each entity-type, lock the rows of its collection tables
		return loop( entitySegments.entrySet().iterator(), (entry, index) -> {
			final var entityMappingType = entry.getKey();
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
			final var entityDetailsMap = LockingHelper.resolveEntityKeys( entry.getValue(), executionContext );

			final var collectionTables = new ArrayList<PluralAttributeMapping>();
			SqmMutationStrategyHelper.visitCollectionTables( entityMappingType, collectionTables::add );
			// we may need to lock the "collection table".
			// the conditions are a bit unclear as to directionality, etc., so for now lock each.
			return loop( collectionTables, attribute -> ReactiveLockingHelper
					.lockCollectionTable( attribute, lockMode, lockTimeout, entityDetailsMap, executionContext ) );
		} ).whenComplete( (unused, throwable) -> {
			// reset the effective graph to whatever it was when we started
			effectiveEntityGraph.clear();
			session.getLoadQueryInfluencers().applyEntityGraph( initialGraph, initialSemantic );
		} );
	}
}
