package org.hibernate.rx.loader.entity.impl;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.pagination.LimitHelper;
import org.hibernate.engine.internal.BatchFetchQueueHelper;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.*;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.LoadEvent;
import org.hibernate.event.spi.LoadEventListener;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.internal.util.collections.CollectionHelper;
import org.hibernate.loader.entity.BatchingEntityLoader;
import org.hibernate.loader.entity.CacheEntityLoaderHelper;
import org.hibernate.loader.entity.EntityJoinWalker;
import org.hibernate.loader.entity.UniqueEntityLoader;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.persister.entity.MultiLoadOptions;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.rx.engine.impl.RxPersistenceContextAdapter;
import org.hibernate.rx.sql.impl.Parameters;
import org.hibernate.rx.util.impl.RxUtil;
import org.hibernate.type.Type;
import org.jboss.logging.Logger;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * @see org.hibernate.loader.entity.DynamicBatchingEntityLoaderBuilder
 */
public class RxDynamicBatchingEntityLoaderBuilder extends RxBatchingEntityLoaderBuilder {
	private static final Logger log = Logger.getLogger( RxDynamicBatchingEntityLoaderBuilder.class );

	public static final RxDynamicBatchingEntityLoaderBuilder INSTANCE = new RxDynamicBatchingEntityLoaderBuilder();

	public CompletionStage<List<Object>> multiLoad(
			OuterJoinLoadable persister,
			Serializable[] ids,
			SessionImplementor session,
			MultiLoadOptions loadOptions) {
		return loadOptions.isOrderReturnEnabled() ?
				performOrderedMultiLoad(persister, ids, session, loadOptions) :
				performUnorderedMultiLoad(persister, ids, session, loadOptions);
	}

	public static StringBuilder buildBatchFetchRestrictionFragment(
			String alias,
			String[] columnNames,
			Dialect dialect,
			Supplier<String> nextParameter) {
		// the general idea here is to just insert a placeholder that we can easily find later...
		if ( columnNames.length == 1 ) {
			// non-composite key
			return new StringBuilder( StringHelper.qualify( alias, columnNames[0] ) )
					.append( " in (" ).append( StringHelper.BATCH_ID_PLACEHOLDER ).append( ')' );
		}
		else {
			// composite key - the form to use here depends on what the dialect supports.
			if ( dialect.supportsRowValueConstructorSyntaxInInList() ) {
				// use : (col1, col2) in ( (?,?), (?,?), ... )
				StringBuilder builder = new StringBuilder();
				builder.append( '(' );
				boolean firstPass = true;
				String deliminator = "";
				for ( String columnName : columnNames ) {
					builder.append( deliminator ).append( StringHelper.qualify( alias, columnName ) );
					if ( firstPass ) {
						firstPass = false;
						deliminator = ",";
					}
				}
				builder.append( ") in (" );
				builder.append( StringHelper.BATCH_ID_PLACEHOLDER );
				builder.append( ')' );
				return builder;
			}
			else {
				// use : ( (col1 = ? and col2 = ?) or (col1 = ? and col2 = ?) or ... )
				//		unfortunately most of this building needs to be held off until we know
				//		the exact number of ids :(
				final StringBuilder stringBuilder = new StringBuilder();
				stringBuilder.append( '(' )
						.append( StringHelper.BATCH_ID_PLACEHOLDER )
						.append( ')' );
				return stringBuilder;
			}
		}
	}

	private CompletionStage<List<Object>> performOrderedBatchLoad(
			List<Serializable> idsInBatch,
			LockOptions lockOptions,
			OuterJoinLoadable persister,
			SessionImplementor session) {
		final int batchSize =  idsInBatch.size();
		final RxDynamicEntityLoader batchingLoader = new RxDynamicEntityLoader(
				persister,
				batchSize,
				lockOptions,
				session.getFactory(),
				session.getLoadQueryInfluencers()
		);

		final Serializable[] idsInBatchArray = idsInBatch.toArray(new Serializable[0]);
		QueryParameters qp = buildMultiLoadQueryParameters( persister, idsInBatchArray, lockOptions );
		CompletionStage<List<Object>> result = batchingLoader.doEntityBatchFetch(session, qp, idsInBatchArray);
		idsInBatch.clear();
		return result;
	}

	protected CompletionStage<List<Object>> performUnorderedMultiLoad(
			OuterJoinLoadable persister,
			Serializable[] ids,
			SessionImplementor session,
			MultiLoadOptions loadOptions) {
		assert !loadOptions.isOrderReturnEnabled();

		final List<Object> result = CollectionHelper.arrayList( ids.length );

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
			final List<Serializable> nonManagedIds = new ArrayList<>();
			for ( Serializable id : ids ) {
				final EntityKey entityKey = new EntityKey( id, persister );

				LoadEvent loadEvent = new LoadEvent(
						id,
						persister.getMappedClass().getName(),
						lockOptions,
						(EventSource) session,
						null
				);

				Object managedEntity = null;

				// look for it in the Session first
				CacheEntityLoaderHelper.PersistenceContextEntry persistenceContextEntry =
						CacheEntityLoaderHelper.INSTANCE.loadFromSessionCache(
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
							persister,
							entityKey
					);
				}

				if ( managedEntity != null ) {
					foundAnyManagedEntities = true;
					result.add( managedEntity );
				}
				else {
					nonManagedIds.add( id );
				}
			}

			if ( foundAnyManagedEntities ) {
				if ( nonManagedIds.isEmpty() ) {
					// all of the given ids were already associated with the Session
					return RxUtil.completedFuture(result);
				}
				else {
					// over-write the ids to be loaded with the collection of
					// just non-managed ones
					ids = nonManagedIds.toArray(
							(Serializable[]) Array.newInstance(
									ids.getClass().getComponentType(),
									nonManagedIds.size()
							)
					);
				}
			}
		}

		int numberOfIdsLeft = ids.length;
		final int maxBatchSize;
		if ( loadOptions.getBatchSize() != null && loadOptions.getBatchSize() > 0 ) {
			maxBatchSize = loadOptions.getBatchSize();
		}
		else {
			maxBatchSize = session.getJdbcServices().getJdbcEnvironment().getDialect()
					.getDefaultBatchLoadSizingStrategy()
					.determineOptimalBatchLoadSize(
							persister.getIdentifierType().getColumnSpan( session.getFactory() ),
							numberOfIdsLeft
					);
		}

		CompletionStage<Void> stage = RxUtil.nullFuture();

		int idPosition = 0;
		while ( numberOfIdsLeft > 0 ) {
			int batchSize =  Math.min( numberOfIdsLeft, maxBatchSize );
			final RxDynamicEntityLoader batchingLoader = new RxDynamicEntityLoader(
					persister,
					batchSize,
					lockOptions,
					session.getFactory(),
					session.getLoadQueryInfluencers()
			);

			Serializable[] idsInBatch = new Serializable[batchSize];
			System.arraycopy( ids, idPosition, idsInBatch, 0, batchSize );

			QueryParameters qp = buildMultiLoadQueryParameters( persister, idsInBatch, lockOptions );
			CompletionStage<Void> fetch =
					batchingLoader.doEntityBatchFetch(session, qp, idsInBatch)
							.thenAccept(result::addAll);
			stage = stage.thenCompose( v -> fetch );

			numberOfIdsLeft = numberOfIdsLeft - batchSize;
			idPosition += batchSize;
		}

		return stage.thenApply( v -> result );
	}

	public static QueryParameters buildMultiLoadQueryParameters(
			OuterJoinLoadable persister,
			Serializable[] ids,
			LockOptions lockOptions) {
		Type[] types = new Type[ids.length];
		Arrays.fill( types, persister.getIdentifierType() );

		QueryParameters qp = new QueryParameters();
		qp.setOptionalEntityName( persister.getEntityName() );
		qp.setPositionalParameterTypes( types );
		qp.setPositionalParameterValues( ids );
		qp.setLockOptions( lockOptions );
		qp.setOptionalObject( null );
		qp.setOptionalId( null );
		return qp;
	}

	@Override
	protected UniqueEntityLoader buildBatchingLoader(
			OuterJoinLoadable persister,
			int batchSize,
			LockMode lockMode,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		return new DynamicBatchingEntityLoader( persister, batchSize, lockMode, factory, influencers );
	}

	@Override
	protected UniqueEntityLoader buildBatchingLoader(
			OuterJoinLoadable persister,
			int batchSize,
			LockOptions lockOptions,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		return new DynamicBatchingEntityLoader( persister, batchSize, lockOptions, factory, influencers );
	}

	public static class DynamicBatchingEntityLoader extends BatchingEntityLoader {
		private final int maxBatchSize;
		private final UniqueEntityLoader singleKeyLoader;
		private final RxDynamicEntityLoader dynamicLoader;

		public DynamicBatchingEntityLoader(
				OuterJoinLoadable persister,
				int maxBatchSize,
				LockMode lockMode,
				SessionFactoryImplementor factory,
				LoadQueryInfluencers loadQueryInfluencers) {
			super( persister );
			this.maxBatchSize = maxBatchSize;
			this.singleKeyLoader = new RxEntityLoader( persister, 1, lockMode, factory, loadQueryInfluencers );
			this.dynamicLoader = new RxDynamicEntityLoader( persister, maxBatchSize, lockMode, factory, loadQueryInfluencers );
		}

		public DynamicBatchingEntityLoader(
				OuterJoinLoadable persister,
				int maxBatchSize,
				LockOptions lockOptions,
				SessionFactoryImplementor factory,
				LoadQueryInfluencers loadQueryInfluencers) {
			super( persister );
			this.maxBatchSize = maxBatchSize;
			this.singleKeyLoader = new RxEntityLoader( persister, 1, lockOptions, factory, loadQueryInfluencers );
			this.dynamicLoader = new RxDynamicEntityLoader( persister, maxBatchSize, lockOptions, factory, loadQueryInfluencers );
		}

		@Override
		public CompletionStage<Object> load(
				Serializable id,
				Object optionalObject,
				SharedSessionContractImplementor session,
				LockOptions lockOptions) {
			final Serializable[] batch = session.getPersistenceContextInternal()
					.getBatchFetchQueue()
					.getEntityBatch( persister(), id, maxBatchSize, persister().getEntityMode() );

			final int numberOfIds = ArrayHelper.countNonNull( batch );
			if ( numberOfIds <= 1 ) {
				final Object result =  singleKeyLoader.load( id, optionalObject, session );
				if ( result == null ) {
					// There was no entity with the specified ID. Make sure the EntityKey does not remain
					// in the batch to avoid including it in future batches that get executed.
					BatchFetchQueueHelper.removeBatchLoadableEntityKey( id, persister(), session );
				}
				return RxUtil.completedFuture(result);
			}

			final Serializable[] idsToLoad = new Serializable[numberOfIds];
			System.arraycopy( batch, 0, idsToLoad, 0, numberOfIds );

			if ( log.isDebugEnabled() ) {
				log.debugf( "Batch loading entity: %s", MessageHelper.infoString( persister(), idsToLoad, session.getFactory() ) );
			}

			QueryParameters qp = buildQueryParameters( id, idsToLoad, optionalObject, lockOptions, false );

			return dynamicLoader.doEntityBatchFetch( (SessionImplementor) session, qp, idsToLoad )
					.thenApply( results -> {
						// The EntityKey for any entity that is not found will remain in the batch.
						// Explicitly remove the EntityKeys for entities that were not found to
						// avoid including them in future batches that get executed.
						BatchFetchQueueHelper.removeNotFoundBatchLoadableEntityKeys( idsToLoad, results, persister(), session );

						return getObjectFromList(results, id, session);
					} );
		}
	}

	private static String repeat(Supplier<String> string, int times, String deliminator) {
		StringBuilder buf = new StringBuilder().append( string.get() );
		for ( int i = 1; i < times; i++ ) {
			buf.append( deliminator ).append( string.get() );
		}
		return buf.toString();
	}

	public static String expandBatchIdPlaceholder(
			String sql,
			Serializable[] ids,
			String alias,
			String[] keyColumnNames,
			Dialect dialect,
			Supplier<String> nextParameter) {
		if ( keyColumnNames.length == 1 ) {
			// non-composite
			return StringHelper.replace( sql, StringHelper.BATCH_ID_PLACEHOLDER, repeat( nextParameter, ids.length, "," ) );
		}
		else {
			// composite
			if ( dialect.supportsRowValueConstructorSyntaxInInList() ) {
				final String tuple = '(' + repeat( nextParameter, keyColumnNames.length, "," ) + ')';
				return StringHelper.replace( sql, StringHelper.BATCH_ID_PLACEHOLDER, StringHelper.repeat( tuple, ids.length, "," ) );
			}
			else {
				final String keyCheck = '(' + StringHelper.joinWithQualifierAndSuffix(
						keyColumnNames,
						alias,
						" = " + nextParameter.get(),
						" and "
				) + ')';
				return StringHelper.replace( sql, StringHelper.BATCH_ID_PLACEHOLDER, StringHelper.repeat( keyCheck, ids.length, " or " ) );
			}
		}
	}

	private CompletionStage<List<Object>> performOrderedMultiLoad(
			OuterJoinLoadable persister,
			Serializable[] ids,
			SessionImplementor session,
			MultiLoadOptions loadOptions) {
		assert loadOptions.isOrderReturnEnabled();

		final List<Object> result = CollectionHelper.arrayList( ids.length );

		final LockOptions lockOptions = loadOptions.getLockOptions() == null
				? new LockOptions( LockMode.NONE )
				: loadOptions.getLockOptions();

		final int maxBatchSize;
		if ( loadOptions.getBatchSize() != null && loadOptions.getBatchSize() > 0 ) {
			maxBatchSize = loadOptions.getBatchSize();
		}
		else {
			maxBatchSize = session.getJdbcServices().getJdbcEnvironment().getDialect().getDefaultBatchLoadSizingStrategy().determineOptimalBatchLoadSize(
					persister.getIdentifierType().getColumnSpan( session.getFactory() ),
					ids.length
			);
		}

		final List<Serializable> idsInBatch = new ArrayList<>();
		final List<Integer> elementPositionsLoadedByBatch = new ArrayList<>();

		CompletionStage<?> stage = RxUtil.nullFuture();

		for ( int i = 0; i < ids.length; i++ ) {
			final Serializable id = ids[i];
			final EntityKey entityKey = new EntityKey( id, persister );

			if ( loadOptions.isSessionCheckingEnabled() || loadOptions.isSecondLevelCacheCheckingEnabled() ) {
				LoadEvent loadEvent = new LoadEvent(
						id,
						persister.getMappedClass().getName(),
						lockOptions,
						(EventSource) session,
						null
				);

				Object managedEntity = null;

				if ( loadOptions.isSessionCheckingEnabled() ) {
					// look for it in the Session first
					CacheEntityLoaderHelper.PersistenceContextEntry persistenceContextEntry =
							CacheEntityLoaderHelper.INSTANCE
									.loadFromSessionCache(
											loadEvent,
											entityKey,
											LoadEventListener.GET
									);
					managedEntity = persistenceContextEntry.getEntity();

					if ( managedEntity != null
							&& !loadOptions.isReturnOfDeletedEntitiesEnabled()
							&& !persistenceContextEntry.isManaged() ) {
						// put a null in the result
						result.add( i, null );
						continue;
					}
				}

				if ( managedEntity == null && loadOptions.isSecondLevelCacheCheckingEnabled() ) {
					// look for it in the SessionFactory
					managedEntity = CacheEntityLoaderHelper.INSTANCE
							.loadFromSecondLevelCache(
									loadEvent,
									persister,
									entityKey
							);
				}

				if ( managedEntity != null ) {
					result.add( i, managedEntity );
					continue;
				}
			}

			// if we did not hit any of the continues above, then we need to batch
			// load the entity state.
			idsInBatch.add( ids[i] );

			if ( idsInBatch.size() >= maxBatchSize ) {
				CompletionStage<List<Object>> load = performOrderedBatchLoad(idsInBatch, lockOptions, persister, session);
				stage = stage.thenCompose( v -> load );
			}

			// Save the EntityKey instance for use later!
			result.add( i, entityKey );
			elementPositionsLoadedByBatch.add( i );
		}

		if ( !idsInBatch.isEmpty() ) {
			CompletionStage<List<Object>> load = performOrderedBatchLoad(idsInBatch, lockOptions, persister, session);
			stage = stage.thenCompose( v -> load );
		}

		return stage.thenApply( v -> {
			final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
			for ( Integer position : elementPositionsLoadedByBatch ) {
				// the element value at this position in the result List should be
				// the EntityKey for that entity; reuse it!
				final EntityKey entityKey = (EntityKey) result.get( position );
				Object entity = persistenceContext.getEntity( entityKey );
				if ( entity != null && !loadOptions.isReturnOfDeletedEntitiesEnabled() ) {
					// make sure it is not DELETED
					final EntityEntry entry = persistenceContext.getEntry( entity );
					if ( entry.getStatus() == Status.DELETED || entry.getStatus() == Status.GONE ) {
						// the entity is locally deleted, and the options ask that we not return such entities...
						entity = null;
					}
				}
				result.set( position, entity );
			}
			return result;
		});
	}

	private static class RxDynamicEntityLoader extends RxEntityLoader {
		// todo : see the discussion on org.hibernate.loader.collection.DynamicBatchingCollectionInitializerBuilder.DynamicBatchingCollectionLoader

		private final String sqlTemplate;
		private final String alias;

		public RxDynamicEntityLoader(
				OuterJoinLoadable persister,
				int maxBatchSize,
				LockOptions lockOptions,
				SessionFactoryImplementor factory,
				LoadQueryInfluencers loadQueryInfluencers) {
			this( persister, maxBatchSize, lockOptions.getLockMode(), factory, loadQueryInfluencers );
		}

		public RxDynamicEntityLoader(
				OuterJoinLoadable persister,
				int maxBatchSize,
				LockMode lockMode,
				SessionFactoryImplementor factory,
				LoadQueryInfluencers loadQueryInfluencers) {
			super( persister, -1, lockMode, factory, loadQueryInfluencers );

			EntityJoinWalker walker = new RxEntityJoinWalker(
					persister,
					persister.getIdentifierColumnNames(),
					-1,
					lockMode,
					factory,
					loadQueryInfluencers) {
				@Override
				protected StringBuilder whereString(String alias, String[] columnNames, int batchSize) {
					return buildBatchFetchRestrictionFragment(
							alias,
							columnNames,
							getDialect(),
							Parameters.createDialectParameterGenerator(getFactory())
					);
				}
			};

			initFromWalker( walker );
			this.sqlTemplate = walker.getSQLString();
			this.alias = walker.getAlias();
			postInstantiate();

			if ( LOG.isDebugEnabled() ) {
				LOG.debugf(
						"SQL-template for dynamic entity [%s] batch-fetching [%s] : %s",
						entityName,
						lockMode,
						sqlTemplate
				);
			}
		}

		@Override
		protected boolean isSingleRowLoader() {
			return false;
		}

		@Override
		protected boolean isSubselectLoadingEnabled() {
			return persister.hasSubselectLoadableCollections();
		}

		public CompletionStage<List<Object>> doEntityBatchFetch(
				SessionImplementor session,
				QueryParameters queryParameters,
				Serializable[] ids) {
			final JdbcServices jdbcServices = session.getJdbcServices();
			final String sql = expandBatchIdPlaceholder(
					sqlTemplate,
					ids,
					alias,
					persister.getKeyColumnNames(),
					jdbcServices.getJdbcEnvironment().getDialect(),
					Parameters.createDialectParameterGenerator(getFactory())
			);

//			try {
				final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
				boolean defaultReadOnlyOrig = persistenceContext.isDefaultReadOnly();
				if ( queryParameters.isReadOnlyInitialized() ) {
					// The read-only/modifiable mode for the query was explicitly set.
					// Temporarily set the default read-only/modifiable setting to the query's setting.
					persistenceContext.setDefaultReadOnly( queryParameters.isReadOnly() );
				}
				else {
					// The read-only/modifiable setting for the query was not initialized.
					// Use the default read-only/modifiable from the persistence context instead.
					queryParameters.setReadOnly( persistenceContext.isDefaultReadOnly() );
				}
				persistenceContext.beforeLoad();
				return doTheLoad( sql, queryParameters, session, ids)
						.handle( (results, e) -> {
							persistenceContext.afterLoad();
							if (e != null) {
								persistenceContext.setDefaultReadOnly(defaultReadOnlyOrig);
								if (e instanceof SQLException) {
									throw jdbcServices.getSqlExceptionHelper().convert(
											(SQLException) e,
											"could not load an entity batch: " + MessageHelper.infoString(
													getEntityPersisters()[0],
													ids,
													session.getFactory()
											),
											sql
									);
								}
							}
							return results;
						})
						.thenCompose(list ->
								// only initialize non-lazy collections after everything else has been refreshed
								((RxPersistenceContextAdapter) persistenceContext ).rxInitializeNonLazyCollections()
										.thenApply(v -> list)
						)
						.handle( (results, e) -> {
							persistenceContext.setDefaultReadOnly(defaultReadOnlyOrig);
							if (e != null) {
								if (e instanceof SQLException) {
									throw jdbcServices.getSqlExceptionHelper().convert(
											(SQLException) e,
											"could not load an entity batch: " + MessageHelper.infoString(
													getEntityPersisters()[0],
													ids,
													session.getFactory()
											),
											sql
									);
								}
							}
							return results;
						});
		}

		private CompletionStage<List<Object>> doTheLoad(
				String sql,
				QueryParameters queryParameters,
				SessionImplementor session,
				Object ids) {
			final RowSelection selection = queryParameters.getRowSelection();
			final int maxRows = LimitHelper.hasMaxRows( selection ) ?
					selection.getMaxRows() :
					Integer.MAX_VALUE;

			final List<AfterLoadAction> afterLoadActions = new ArrayList<>();
			return executeRxQueryStatement( sql,
					queryParameters,
					false,
					afterLoadActions,
					session,
					resultSet -> {
						try {
							return processResultSet(
									resultSet,
									queryParameters,
									session,
									false,
									null,
									maxRows,
									afterLoadActions
							);
						}
						catch (SQLException sqle) {
							throw getFactory().getJdbcServices().getSqlExceptionHelper().convert(
									sqle,
									"could not load an entity batch: " + MessageHelper.infoString(
											getEntityPersisters()[0],
											ids,
											session.getFactory()
									),
									sql
							);
						}
					}
			);
		}
	}
}
