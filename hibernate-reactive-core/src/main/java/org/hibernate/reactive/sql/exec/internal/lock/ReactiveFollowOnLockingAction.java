/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal.lock;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.Locking;
import org.hibernate.engine.spi.CollectionKey;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.metamodel.mapping.TableDetails;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.impl.ReactiveSessionImpl;
import org.hibernate.reactive.sql.exec.spi.ReactivePostAction;
import org.hibernate.sql.ast.spi.LockingClauseStrategy;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.exec.internal.lock.FollowOnLockingAction;
import org.hibernate.sql.exec.internal.lock.LockingHelper;
import org.hibernate.sql.exec.internal.lock.TableLock;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcSelectWithActionsBuilder;

import jakarta.persistence.Timeout;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import static java.util.Collections.emptyMap;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;

/**
 * Reactive version of {@link FollowOnLockingAction}
 */
public class ReactiveFollowOnLockingAction extends FollowOnLockingAction implements ReactivePostAction {
	protected ReactiveFollowOnLockingAction(
			LoadedValuesCollectorImpl loadedValuesCollector,
			LockMode lockMode,
			Timeout lockTimeout,
			Locking.Scope lockScope) {
		super( loadedValuesCollector, lockMode, lockTimeout, lockScope );
	}

	public static void apply(
			LockOptions lockOptions,
			QuerySpec lockingTarget,
			LockingClauseStrategy lockingClauseStrategy,
			JdbcSelectWithActionsBuilder jdbcSelectBuilder) {
		final var fromClause = lockingTarget.getFromClause();
		final var loadedValuesCollector = resolveLoadedValuesCollector( fromClause, lockingClauseStrategy );

		// NOTE: we need to set this separately so that it can get incorporated into
		// the JdbcValuesSourceProcessingState for proper callbacks
		jdbcSelectBuilder.setLoadedValuesCollector( loadedValuesCollector );

		// additionally, add a post-action which uses the collected values.
		jdbcSelectBuilder.appendPostAction( new ReactiveFollowOnLockingAction(
				loadedValuesCollector,
				lockOptions.getLockMode(),
				lockOptions.getTimeout(),
				lockOptions.getScope()
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
		final var entitySegments = segmentLoadedValues();
		final Map<EntityMappingType, Map<PluralAttributeMapping, List<CollectionKey>>> collectionSegments =
				lockScope == Locking.Scope.INCLUDE_FETCHES
						? segmentLoadedCollections()
						: emptyMap();

		// for each entity-type, prepare a locking select statement per table.
		// this is based on the attributes for "state array" ordering purposes -
		// we match each attribute to the table it is mapped to and add it to
		// the select-list for that table-segment.

		return loop(
				entitySegments.keySet().iterator(), (entityMappingType, index) -> {
					List<EntityKey> entityKeys = entitySegments.get( entityMappingType );
					final var tableLocks = prepareTableLocks( entityMappingType, entityKeys, session );

					// create a cross-reference of information related to an entity based on its identifier,
					// we'll use this later when we adjust the state array and inject state into the entity instance.
					final var entityDetailsMap = LockingHelper.resolveEntityKeys( entityKeys, executionContext );

					// at this point, we have all the individual locking selects ready to go - execute them
					final var lockingOptions = buildLockingOptions(
							tableLocks,
							entityDetailsMap,
							entityMappingType,
							effectiveEntityGraph,
							entityKeys,
							collectionSegments,
							session,
							executionContext
					);
					return loop( tableLocks.values().iterator(), (tableLock, i) ->
							( (ReactiveTableLock) tableLock ).reactivePerformActions(
									entityDetailsMap,
									lockingOptions,
									(ReactiveSessionImpl) session
							)
					 );

				}
		).whenComplete( (unused, throwable) -> {
			effectiveEntityGraph.clear();
			session.getLoadQueryInfluencers().applyEntityGraph( initialGraph, initialSemantic );
		} );

	}

	@Override
	protected TableLock createTableLock(
			TableDetails tableDetails,
			EntityMappingType entityMappingType,
			List<EntityKey> entityKeys,
			SharedSessionContractImplementor session) {
		return new ReactiveTableLock( tableDetails, entityMappingType, entityKeys, session );
	}
}
