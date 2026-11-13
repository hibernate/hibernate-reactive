/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal.lock;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.engine.spi.CollectionKey;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.metamodel.mapping.TableDetails;
import org.hibernate.query.sqm.mutation.internal.SqmMutationStrategyHelper;
import org.hibernate.reactive.logging.internal.Log;
import org.hibernate.reactive.logging.internal.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.internal.ReactiveSessionImpl;
import org.hibernate.reactive.sql.exec.spi.ReactivePostAction;
import org.hibernate.dialect.lock.spi.LockingClauseStrategy;
import org.hibernate.sql.ast.spi.query.select.QuerySpec;
import org.hibernate.sql.exec.internal.lock.FollowOnLockingAction;
import org.hibernate.sql.exec.internal.lock.EntityDetails;
import org.hibernate.sql.exec.internal.lock.LockingHelper;
import org.hibernate.sql.exec.internal.lock.TableLock;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcSelectWithActionsBuilder;
import org.hibernate.sql.exec.spi.LoadedValuesCollector;
import org.hibernate.sql.exec.spi.StatementAccess;

import jakarta.persistence.PessimisticLockScope;
import jakarta.persistence.Timeout;

import static java.util.Collections.emptyMap;
import static org.hibernate.reactive.util.internal.CompletionStages.loop;
import static org.hibernate.reactive.util.internal.CompletionStages.voidFuture;

/**
 * Reactive version of {@link FollowOnLockingAction}
 */
public class ReactiveFollowOnLockingAction extends FollowOnLockingAction implements ReactivePostAction {
	public static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public void performPostAction(
			StatementAccess jdbcStatementAccess,
			Connection jdbcConnection,
			ExecutionContext executionContext,
			LoadedValuesCollector loadedValuesCollector) {
		throw LOG.nonReactiveMethodCall( "reactivePerformPostAction()" );
	}

	private final LockMode lockMode;
	private final Timeout lockTimeout;
	private final PessimisticLockScope scope;

	protected ReactiveFollowOnLockingAction(
			LockMode lockMode,
			Timeout lockTimeout,
			PessimisticLockScope lockScope) {
		// The collection tables are locked by this class because the ORM implementation runs
		// blocking JDBC statements when the scope is not NORMAL: see lockCollectionTables
		super( lockMode, lockTimeout, PessimisticLockScope.NORMAL );
		this.lockMode = lockMode;
		this.lockTimeout = lockTimeout;
		this.scope = lockScope;
	}

	public static void apply(
			LockOptions lockOptions,
			QuerySpec lockingTarget,
			LockingClauseStrategy lockingClauseStrategy,
			JdbcSelectWithActionsBuilder jdbcSelectBuilder) {
		final var fromClause = lockingTarget.getFromClause();
		// NOTE: we need to set this separately so that it can get incorporated into
		// the JdbcValuesSourceProcessingState for proper callbacks
		jdbcSelectBuilder.setLoadedValuesCollectorFactory( resolveLoadedValuesCollectorFactory( fromClause, lockingClauseStrategy ) );

		// additionally, add a post-action which uses the collected values.
		jdbcSelectBuilder.appendPostAction( new ReactiveFollowOnLockingAction(
				lockOptions.getLockMode(),
				lockOptions.getTimeout(),
				lockOptions.getScope()
		) );
	}


	@Override
	public CompletionStage<Void> reactivePerformReactivePostAction(
			ReactiveConnection jdbcConnection,
			ExecutionContext executionContext,
			LoadedValuesCollector loadedValuesCollector) {
		LockingHelper.logLoadedValues( loadedValuesCollector );

		final var session = executionContext.getSession();

		// NOTE: we deal with effective graphs here to make sure embedded associations are treated as lazy
		final var effectiveEntityGraph = session.getLoadQueryInfluencers().getEffectiveEntityGraph();
		final var initialGraph = effectiveEntityGraph.getGraph();
		final var initialSemantic = effectiveEntityGraph.getSemantic();

		// collect registrations by entity type
		final var entitySegments = segmentLoadedValues( loadedValuesCollector );
		final var collectionSegments = segmentLoadedCollections( loadedValuesCollector );

		// for each entity-type, prepare a locking select statement per table.
		// this is based on the attributes for "state array" ordering purposes -
		// we match each attribute to the table it is mapped to and add it to
		// the select-list for that table-segment.

		return loop( entitySegments.entrySet().iterator(), (entry, index) -> {
					final var entityMappingType = entry.getKey();
					final var entityKeys = entry.getValue();
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
							executionContext,
							loadedValuesCollector
					);
					return lockCollectionTables( entityMappingType, entityDetailsMap, collectionSegments, loadedValuesCollector, executionContext )
							.thenCompose( v -> loop( tableLocks.values().iterator(), (tableLock, i) ->
									( (ReactiveTableLock) tableLock )
											.reactivePerformActions( entityDetailsMap, lockingOptions, (ReactiveSessionImpl) session )
							) )
							.thenCompose( v -> forceIncrementAfterLocking()
									? forceIncrement( entityDetailsMap, session )
									: voidFuture() );

				}
		).whenComplete( (unused, throwable) -> {
			effectiveEntityGraph.clear();
			session.getLoadQueryInfluencers().applyEntityGraph( initialGraph, initialSemantic );
		} );

	}

	/**
	 * With {@link LockMode#PESSIMISTIC_FORCE_INCREMENT} and {@link PessimisticLockScope#EXTENDED},
	 * increment the versions after the tables are locked, like {@link FollowOnLockingAction}.
	 * <p>
	 * With the other scopes, {@link org.hibernate.reactive.event.internal.DefaultReactiveLoadEventListener}
	 * increments the version after {@code find}.
	 * </p>
	 */
	private boolean forceIncrementAfterLocking() {
		return lockMode == LockMode.PESSIMISTIC_FORCE_INCREMENT && scope == PessimisticLockScope.EXTENDED;
	}

	private static CompletionStage<Void> forceIncrement(Map<Object, EntityDetails> entityDetailsMap, SharedSessionContractImplementor session) {
		return loop( entityDetailsMap.values(), details -> {
			// The reread updated the entity: complete the entry before the increment, like ORM
			final var entry = details.entry();
			entry.setVersion( entry.getPersister().getVersion( details.instance() ) );
			return ReactiveDirectLockingCompletion.forceVersionIncrement( details.instance(), entry, session );
		} );
	}

	/**
	 * The reactive version of the part of {@link FollowOnLockingAction#buildLockingOptions} that locks
	 * the collection tables, according to the lock scope.
	 */
	private CompletionStage<Void> lockCollectionTables(
			EntityMappingType entityMappingType,
			Map<Object, EntityDetails> entityDetailsMap,
			Map<EntityMappingType, Map<PluralAttributeMapping, List<CollectionKey>>> collectionSegments,
			LoadedValuesCollector loadedValuesCollector,
			ExecutionContext executionContext) {
		if ( scope == PessimisticLockScope.EXTENDED ) {
			final var collectionTables = new ArrayList<PluralAttributeMapping>();
			// we may need to lock the "collection table".
			// the conditions are a bit unclear as to directionality, etc., so for now lock each.
			SqmMutationStrategyHelper.visitCollectionTables( entityMappingType, collectionTables::add );
			return loop( collectionTables, attribute -> ReactiveLockingHelper
					.lockCollectionTable( attribute, lockMode, lockTimeout, entityDetailsMap, executionContext ) );
		}
		if ( scope == PessimisticLockScope.FETCHED
				&& loadedValuesCollector.getCollectedCollections() != null
				&& !loadedValuesCollector.getCollectedCollections().isEmpty() ) {
			final var attributeKeys = collectionSegments.get( entityMappingType );
			if ( attributeKeys != null ) {
				return loop( attributeKeys.entrySet().iterator(), (entry, index) -> ReactiveLockingHelper
						.lockCollectionTable( entry.getKey(), lockMode, lockTimeout, entry.getValue(), executionContext ) );
			}
		}
		return voidFuture();
	}

	@Override
	protected Map<EntityMappingType, Map<PluralAttributeMapping, List<CollectionKey>>> segmentLoadedCollections(
			LoadedValuesCollector loadedValuesCollector) {
		if ( scope == PessimisticLockScope.NORMAL ) {
			return emptyMap();
		}
		final Map<EntityMappingType, Map<PluralAttributeMapping, List<CollectionKey>>> map = new HashMap<>();
		LockingHelper.segmentLoadedCollections( loadedValuesCollector.getCollectedCollections(), scope, map );
		return map;
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
