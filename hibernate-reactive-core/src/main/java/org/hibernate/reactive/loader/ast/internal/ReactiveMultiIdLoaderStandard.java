/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.SubselectFetch;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.LoadEvent;
import org.hibernate.event.spi.LoadEventListener;
import org.hibernate.internal.util.collections.CollectionHelper;
import org.hibernate.loader.ast.internal.CacheEntityLoaderHelper;
import org.hibernate.loader.ast.internal.LoaderSelectBuilder;
import org.hibernate.loader.ast.spi.MultiIdLoadOptions;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.reactive.loader.ast.spi.ReactiveMultiIdEntityLoader;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.internal.JdbcParameterBindingsImpl;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.results.internal.RowTransformerStandardImpl;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.whileLoop;

/**
 * @see org.hibernate.loader.ast.internal.MultiIdLoaderStandard
 */
public class ReactiveMultiIdLoaderStandard<T> implements ReactiveMultiIdEntityLoader<T> {
	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final EntityPersister entityDescriptor;
	private final SessionFactoryImplementor sessionFactory;

	private final int idJdbcTypeCount;

	public ReactiveMultiIdLoaderStandard(
			EntityPersister entityDescriptor,
			PersistentClass bootDescriptor,
			SessionFactoryImplementor sessionFactory) {
		this.entityDescriptor = entityDescriptor;
		this.idJdbcTypeCount = bootDescriptor.getIdentifier().getColumnSpan();
		this.sessionFactory = sessionFactory;
		assert idJdbcTypeCount > 0;
	}

	@Override
	public EntityMappingType getLoadable() {
		return entityDescriptor;
	}

	@Override
	public <K> CompletionStage<List<T>> load(K[] ids, MultiIdLoadOptions loadOptions, EventSource session) {
		assert ids != null;
		return loadOptions.isOrderReturnEnabled()
				? performOrderedMultiLoad( ids, session, loadOptions )
				: performUnorderedMultiLoad( ids, session, loadOptions );
	}

	private CompletionStage<List<T>>  performOrderedMultiLoad(Object[] ids, EventSource session, MultiIdLoadOptions loadOptions) {
		if ( LOG.isTraceEnabled() ) {
			LOG.tracef( "#performOrderedMultiLoad(`%s`, ..)", entityDescriptor.getEntityName() );
		}

		assert loadOptions.isOrderReturnEnabled();

		final JdbcEnvironment jdbcEnvironment = sessionFactory.getJdbcServices().getJdbcEnvironment();
		final Dialect dialect = jdbcEnvironment.getDialect();

		final List<Object> result = CollectionHelper.arrayList( ids.length );

		final LockOptions lockOptions = (loadOptions.getLockOptions() == null)
				? new LockOptions( LockMode.NONE )
				: loadOptions.getLockOptions();

		final int maxBatchSize = loadOptions.getBatchSize() != null && loadOptions.getBatchSize() > 0
				? loadOptions.getBatchSize()
				: dialect
				.getDefaultBatchLoadSizingStrategy()
				.determineOptimalBatchLoadSize(
						idJdbcTypeCount,
						ids.length,
						sessionFactory.getSessionFactoryOptions().inClauseParameterPaddingEnabled()
				);

		final List<Object> idsInBatch = new ArrayList<>();
		final List<Integer> elementPositionsLoadedByBatch = new ArrayList<>();

		final boolean coerce = !sessionFactory.getJpaMetamodel().getJpaCompliance().isLoadByIdComplianceEnabled();
		return loop( 0, ids.length, i -> {
			final Object id = coerce ?
					entityDescriptor.getIdentifierMapping().getJavaType().coerce( ids[i], session ) :
					ids[i];

			final EntityKey entityKey = new EntityKey( id, entityDescriptor );
			if ( loadOptions.isSessionCheckingEnabled() || loadOptions.isSecondLevelCacheCheckingEnabled() ) {
				LoadEvent loadEvent = new LoadEvent( id,
													 entityDescriptor.getMappedClass().getName(),
													 lockOptions,
													 session,
													 getReadOnlyFromLoadQueryInfluencers( session )
				);

				Object managedEntity = null;

				if ( loadOptions.isSessionCheckingEnabled() ) {
					// look for it in the Session first
					CacheEntityLoaderHelper.PersistenceContextEntry persistenceContextEntry = CacheEntityLoaderHelper.INSTANCE.loadFromSessionCache(
							loadEvent,
							entityKey,
							LoadEventListener.GET
					);
					managedEntity = persistenceContextEntry.getEntity();

					if ( managedEntity != null && !loadOptions.isReturnOfDeletedEntitiesEnabled() && !persistenceContextEntry.isManaged() ) {
						// put a null in the result
						result.add( i, null );
						return voidFuture();
					}
				}

				if ( managedEntity == null && loadOptions.isSecondLevelCacheCheckingEnabled() ) {
					// look for it in the SessionFactory
					managedEntity = CacheEntityLoaderHelper.INSTANCE.loadFromSecondLevelCache(
							loadEvent,
							entityDescriptor,
							entityKey
					);
				}

				if ( managedEntity != null ) {
					result.add( i, managedEntity );
					return voidFuture();
				}
			}

			// if we did not hit any of the continues above, then we need to batch
			// load the entity state.
			idsInBatch.add( id );

			CompletionStage<Void> loopResult = voidFuture();
			if ( idsInBatch.size() >= maxBatchSize ) {
				// we've hit the allotted max-batch-size, perform an "intermediate load"
				loopResult = loadEntitiesById( idsInBatch, lockOptions, session ).thenAccept( v -> idsInBatch.clear() );

			}

			return loopResult.thenAccept( v -> {
				// Save the EntityKey instance for use later!
				// todo (6.0) : see below wrt why `elementPositionsLoadedByBatch` probably isn't needed
				result.add( i, entityKey );
				elementPositionsLoadedByBatch.add( i );
			} );
		} ).thenCompose( v -> {
			if ( !idsInBatch.isEmpty() ) {
				// we still have ids to load from the processing above since the last max-batch-size trigger,
				// perform a load for them
				return loadEntitiesById( idsInBatch, lockOptions, session ).thenCompose( CompletionStages::voidFuture );
			}
			return voidFuture();
		} ).thenApply( v -> {
			// todo (6.0) : can't we just walk all elements of the results looking for EntityKey and replacing here?
			//		can't imagine
			final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
			for ( Integer position : elementPositionsLoadedByBatch ) {
				// the element value at this position in the result List should be
				// the EntityKey for that entity; reuse it!
				final EntityKey entityKey = (EntityKey) result.get( position );
				Object entity = persistenceContext.getEntity( entityKey );
				if ( entity != null && !loadOptions.isReturnOfDeletedEntitiesEnabled() ) {
					// make sure it is not DELETED
					final EntityEntry entry = persistenceContext.getEntry( entity );
					if ( entry.getStatus().isDeletedOrGone() ) {
						// the entity is locally deleted, and the options ask that we not return such entities...
						entity = null;
					}
				}
				result.set( position, entity );
			}

			//noinspection unchecked
			return (List<T>) result;
		} );
	}

	private CompletionStage<List<T>> loadEntitiesById(List<Object> idsInBatch, LockOptions lockOptions, SharedSessionContractImplementor session) {
		assert idsInBatch != null;
		assert !idsInBatch.isEmpty();

		final int numberOfIdsInBatch = idsInBatch.size();
		if ( numberOfIdsInBatch == 1 ) {
			return performSingleMultiLoad( idsInBatch.get( 0 ), lockOptions, session );
		}

		if ( LOG.isTraceEnabled() ) {
			LOG.tracef( "#loadEntitiesById(`%s`, `%s`, ..)", entityDescriptor.getEntityName(), numberOfIdsInBatch );
		}

		final List<JdbcParameter> jdbcParameters = new ArrayList<>( numberOfIdsInBatch * idJdbcTypeCount);

		final SelectStatement sqlAst = LoaderSelectBuilder.createSelect(
				getLoadable(),
				// null here means to select everything
				null,
				getLoadable().getIdentifierMapping(),
				null,
				numberOfIdsInBatch,
				session.getLoadQueryInfluencers(),
				lockOptions,
				jdbcParameters::add,
				sessionFactory
		);

		final JdbcServices jdbcServices = sessionFactory.getJdbcServices();
		final JdbcEnvironment jdbcEnvironment = jdbcServices.getJdbcEnvironment();
		final SqlAstTranslatorFactory sqlAstTranslatorFactory = jdbcEnvironment.getSqlAstTranslatorFactory();

		final JdbcParameterBindings jdbcParameterBindings = new JdbcParameterBindingsImpl( jdbcParameters.size() );
		int offset = 0;

		for ( int i = 0; i < numberOfIdsInBatch; i++ ) {
			final Object id = idsInBatch.get( i );

			offset += jdbcParameterBindings.registerParametersForEachJdbcValue(
					id,
					offset,
					entityDescriptor.getIdentifierMapping(),
					jdbcParameters,
					session
			);
		}

		// we should have used all the JdbcParameter references (created bindings for all)
		assert offset == jdbcParameters.size();
		final JdbcOperationQuerySelect jdbcSelect = sqlAstTranslatorFactory.buildSelectTranslator( sessionFactory, sqlAst )
				.translate( jdbcParameterBindings, QueryOptions.NONE );

		final SubselectFetch.RegistrationHandler subSelectFetchableKeysHandler;
		if ( entityDescriptor.hasSubselectLoadableCollections() ) {
			subSelectFetchableKeysHandler = SubselectFetch.createRegistrationHandler(
					session.getPersistenceContext().getBatchFetchQueue(),
					sqlAst,
					jdbcParameters,
					jdbcParameterBindings
			);
		}
		else {
			subSelectFetchableKeysHandler = null;
		}

		return StandardReactiveSelectExecutor.INSTANCE.list(
				jdbcSelect,
				jdbcParameterBindings,
				new ExecutionContextWithSubselectFetchHandler( session, subSelectFetchableKeysHandler ),
				RowTransformerStandardImpl.instance(),
				ReactiveListResultsConsumer.UniqueSemantic.FILTER
		);
	}

	private CompletionStage<List<T>> performSingleMultiLoad(Object id, LockOptions lockOptions, SharedSessionContractImplementor session) {
		return ( (ReactiveEntityPersister) entityDescriptor )
				.reactiveLoad( id, null, lockOptions, session )
				.thenApply( ReactiveMultiIdLoaderStandard::singletonList );
	}

	private static <T> List<T> singletonList(Object loaded) {
		return Collections.singletonList( (T) loaded );
	}

	private CompletionStage<List<T>> performUnorderedMultiLoad(Object[] ids, EventSource session, MultiIdLoadOptions loadOptions) {
		assert !loadOptions.isOrderReturnEnabled();
		assert ids != null;

		if ( LOG.isTraceEnabled() ) {
			LOG.tracef( "#performUnorderedMultiLoad(`%s`, ..)", entityDescriptor.getEntityName() );
		}

		final List<T> result = CollectionHelper.arrayList( ids.length );

		final LockOptions lockOptions = loadOptions.getLockOptions() == null
				? new LockOptions( LockMode.NONE )
				: loadOptions.getLockOptions();

		if ( loadOptions.isSessionCheckingEnabled() || loadOptions.isSecondLevelCacheCheckingEnabled() ) {
			// the user requested that we exclude ids corresponding to already managed
			// entities from the generated load SQL.  So here we will iterate all
			// incoming id values and see whether it corresponds to an existing
			// entity associated with the PC - if it does we add it to the result
			// list immediately and remove its id from the group of ids to load.
			boolean foundAnyManagedEntities = false;
			final List<Object> nonManagedIds = new ArrayList<>();

			final boolean coerce = !sessionFactory.getJpaMetamodel().getJpaCompliance().isLoadByIdComplianceEnabled();
			for ( int i = 0; i < ids.length; i++ ) {
				final Object id = coerce
						? entityDescriptor.getIdentifierMapping().getJavaType().coerce( ids[i], session )
						: ids[i];

				final EntityKey entityKey = new EntityKey( id, entityDescriptor );

				LoadEvent loadEvent = new LoadEvent(
						id,
						entityDescriptor.getMappedClass().getName(),
						lockOptions,
						session,
						getReadOnlyFromLoadQueryInfluencers( session )
				);

				Object managedEntity = null;

				// look for it in the Session first
				CacheEntityLoaderHelper.PersistenceContextEntry persistenceContextEntry = CacheEntityLoaderHelper.INSTANCE
						.loadFromSessionCache(
								loadEvent,
								entityKey,
								LoadEventListener.GET
						);
				if ( loadOptions.isSessionCheckingEnabled() ) {
					managedEntity = persistenceContextEntry.getEntity();

					if ( managedEntity != null
							&& !loadOptions.isReturnOfDeletedEntitiesEnabled()
							&& !persistenceContextEntry.isManaged() ) {
						foundAnyManagedEntities = true;
						result.add( null );
						continue;
					}
				}

				if ( managedEntity == null && loadOptions.isSecondLevelCacheCheckingEnabled() ) {
					managedEntity = CacheEntityLoaderHelper.INSTANCE.loadFromSecondLevelCache(
							loadEvent,
							entityDescriptor,
							entityKey
					);
				}

				if ( managedEntity != null ) {
					foundAnyManagedEntities = true;
					//noinspection unchecked
					result.add( (T) managedEntity );
				}
				else {
					nonManagedIds.add( id );
				}
			}

			if ( foundAnyManagedEntities ) {
				if ( nonManagedIds.isEmpty() ) {
					// all of the given ids were already associated with the Session
					return completedFuture( result );
				}
				else {
					// over-write the ids to be loaded with the collection of
					// just non-managed ones
					ids = nonManagedIds.toArray(
							(Object[]) Array.newInstance(
									ids.getClass().getComponentType(),
									nonManagedIds.size()
							)
					);
				}
			}
		}

		final Object[] identifiers = ids;
		final int[] numberOfIdsLeft = { ids.length };
		final int maxBatchSize = loadOptions.getBatchSize() != null && loadOptions.getBatchSize() > 0
				? loadOptions.getBatchSize()
				: session.getJdbcServices()
				.getJdbcEnvironment()
				.getDialect()
				.getDefaultBatchLoadSizingStrategy()
				.determineOptimalBatchLoadSize(
						entityDescriptor.getIdentifierType().getColumnSpan( session.getFactory() ),
						numberOfIdsLeft[0],
						sessionFactory.getSessionFactoryOptions().inClauseParameterPaddingEnabled()
				);

		int[] idPosition = { 0 };

		return whileLoop( () -> numberOfIdsLeft[0] > 0, () -> {
			final int batchSize = Math.min( numberOfIdsLeft[0], maxBatchSize );

			final Object[] idsInBatch = new Object[batchSize];
			System.arraycopy( identifiers, idPosition[0], idsInBatch, 0, batchSize );

			return loadEntitiesById( Arrays.asList( idsInBatch ), lockOptions, session )
					.thenAccept( result::addAll )
					.thenAccept( v -> {
						numberOfIdsLeft[0] = numberOfIdsLeft[0] - batchSize;
						idPosition[0] += batchSize;
					} );
		} )
				.thenApply( v -> result );
	}

	private Boolean getReadOnlyFromLoadQueryInfluencers(SharedSessionContractImplementor session) {
		Boolean readOnly = null;
		final LoadQueryInfluencers loadQueryInfluencers = session.getLoadQueryInfluencers();
		if ( loadQueryInfluencers != null ) {
			readOnly = loadQueryInfluencers.getReadOnly();
		}
		return readOnly;
	}

}
