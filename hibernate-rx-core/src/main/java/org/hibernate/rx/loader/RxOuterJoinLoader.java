package org.hibernate.rx.loader;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.pagination.LimitHelper;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.RowSelection;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.loader.EntityAliases;
import org.hibernate.loader.OuterJoinLoader;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.Loadable;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.rx.engine.impl.RxPersistenceContextAdapter;
import org.hibernate.rx.impl.RxQueryExecutor;
import org.hibernate.rx.util.impl.RxUtil;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.EntityType;

/**
 * Reactive version of {@link OuterJoinLoader}
 * <p>
 *     This class follows the same structure of {@link OuterJoinLoader} with the methods signature change so that
 *     it's possible to return a {@link CompletionStage}
 * </p>
 */
public class RxOuterJoinLoader extends OuterJoinLoader {
	public RxOuterJoinLoader(SessionFactoryImplementor factory, LoadQueryInfluencers loadQueryInfluencers) {
		super(factory, loadQueryInfluencers);
	}

	public CompletionStage<List<Object>> doRxQueryAndInitializeNonLazyCollections(
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies) {
		return doRxQueryAndInitializeNonLazyCollections( session, queryParameters, returnProxies, null );
	}

	protected CompletionStage<List<Object>> doRxQueryAndInitializeNonLazyCollections(
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies,
			final ResultTransformer forcedResultTransformer) {
		final PersistenceContext persistenceContext = session.getPersistenceContext();
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
		return doRxQuery( session, queryParameters, returnProxies, forcedResultTransformer )
				.handle( (list, e) -> {
					persistenceContext.afterLoad();
					if (e != null) {
						persistenceContext.setDefaultReadOnly(defaultReadOnlyOrig);
						RxUtil.rethrow(e);
					}
					return list;
				})
				.thenCompose(list -> {
					// only initialize non-lazy collections after everything else has been refreshed
					return ((RxPersistenceContextAdapter) persistenceContext ).rxInitializeNonLazyCollections()
							.thenApply(v -> list);
				})
				.handle( (list, e) -> {
					persistenceContext.setDefaultReadOnly(defaultReadOnlyOrig);
					if (e != null) {
						RxUtil.rethrow(e);
					}
					return list;
				});
	}

	private CompletionStage<List<Object>> doRxQuery(
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies,
			final ResultTransformer forcedResultTransformer) throws HibernateException {

		final RowSelection selection = queryParameters.getRowSelection();
		final int maxRows = LimitHelper.hasMaxRows( selection ) ?
				selection.getMaxRows() :
				Integer.MAX_VALUE;

		final List<AfterLoadAction> afterLoadActions = new ArrayList<>();

		return executeRxQueryStatement(
				getSQLString(), queryParameters, false, afterLoadActions, session,
				resultSet -> {
					try {
						return processResultSet(
								resultSet,
								queryParameters,
								session,
								returnProxies,
								forcedResultTransformer,
								maxRows,
								afterLoadActions
						);
					}
					catch (SQLException sqle) {
						throw getFactory().getJdbcServices().getSqlExceptionHelper().convert(
								sqle,
								"could not load an entity batch: " + MessageHelper.infoString(
										getEntityPersisters()[0],
										queryParameters.getOptionalId(),
										session.getFactory()
								),
								sql
						);
					}
				}
		);
	}

	private CollectionPersister[] collectionPersisters(Object resultSetId, SharedSessionContractImplementor session) {
		final CollectionPersister[] collectionPersisters = getCollectionPersisters();
		if ( collectionPersisters != null ) {
			for ( CollectionPersister collectionPersister : collectionPersisters ) {
				if ( collectionPersister.isArray() ) {
					//for arrays, we should end the collection load before resolving
					//the entities, since the actual array instances are not instantiated
					//during loading
					//TODO: or we could do this polymorphically, and have two
					//      different operations implemented differently for arrays
					endCollectionLoad( resultSetId, session, collectionPersister );
				}
			}
		}
		return collectionPersisters;
	}

	// Added this just to catch the exception and make the code more readable
	private CompletionStage<Object> rxRows(ResultSet rs, QueryParameters queryParameters, SharedSessionContractImplementor session, boolean returnProxies, ResultTransformer forcedResultTransformer, EntityKey optionalObjectKey, LockMode[] lockModesArray, boolean createSubselects, List subselectResultKeys, List hydratedObjects, EntityKey[] keys) {
		try {
			return getRxRowFromResultSet( rs, session, queryParameters, lockModesArray, optionalObjectKey, hydratedObjects,
					keys, returnProxies, forcedResultTransformer
			);
		} catch (SQLException sqle) {
			throw getFactory().getJdbcServices().getSqlExceptionHelper().convert(
					sqle,
					"could not load an entity batch: " + MessageHelper.infoString(
							getEntityPersisters()[0],
							queryParameters.getOptionalId(),
							session.getFactory()
					),
					sql
			);
		}
	}

	private CompletionStage<Object> getRxRowFromResultSet(
			final ResultSet resultSet,
			final SharedSessionContractImplementor session,
			final QueryParameters queryParameters,
			final LockMode[] lockModesArray,
			final EntityKey optionalObjectKey,
			final List hydratedObjects,
			final EntityKey[] keys,
			boolean returnProxies,
			ResultTransformer forcedResultTransformer) throws SQLException, HibernateException {
		final Loadable[] persisters = getEntityPersisters();
		final int entitySpan = persisters.length;
		extractKeysFromResultSet( persisters, queryParameters, resultSet, session, keys, lockModesArray, hydratedObjects );

//		FIXME: for later
		registerNonExists( keys, persisters, session );

		// this call is side-effecty
		return getRxRow( resultSet, persisters, keys, queryParameters, optionalObjectKey, lockModesArray, hydratedObjects, session )
				.thenApply( row -> {

			//		TODO: We don't do collection of elements for now
			//		readCollectionElements( row, resultSet, session );

					if ( returnProxies ) {
						// now get an existing proxy for each row element (if there is one)
						final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
						for ( int i = 0; i < entitySpan; i++ ) {
							Object entity = row[i];
							Object proxy = persistenceContext.proxyFor( persisters[i], keys[i], entity );
							if ( entity != proxy ) {
								// force the proxy to resolve itself
								( (HibernateProxy) proxy ).getHibernateLazyInitializer().setImplementation( entity );
								row[i] = proxy;
							}
						}
					}

					applyPostLoadLocks( row, lockModesArray, session );

					try {
						return forcedResultTransformer == null
								? getResultColumnOrRow( row, queryParameters.getResultTransformer(), resultSet, session )
								: forcedResultTransformer.transformTuple( getResultRow( row, resultSet, session ),  getResultRowAliases());

					}
					catch (SQLException sqle) {
						throw getFactory().getJdbcServices().getSqlExceptionHelper().convert(
								sqle,
								"could not load an entity batch: " + MessageHelper.infoString(
										getEntityPersisters()[0],
										queryParameters.getOptionalId(),
										session.getFactory()
								),
								sql
						);
					}
				});
	}

	/**
	 * Resolve any IDs for currently loaded objects, duplications within the
	 * <tt>ResultSet</tt>, etc. Instantiate empty objects to be initialized from the
	 * <tt>ResultSet</tt>. Return an array of objects (a row of results) and an
	 * array of booleans (by side-effect) that determine whether the corresponding
	 * object should be initialized.
	 */
	private CompletionStage<Object[]> getRxRow(
			final ResultSet rs,
			final Loadable[] persisters,
			final EntityKey[] keys,
			final QueryParameters queryParameters,
			final EntityKey optionalObjectKey,
			final LockMode[] lockModes,
			final List hydratedObjects,
			final SharedSessionContractImplementor session) throws HibernateException, SQLException {
		final int cols = persisters.length;
		final EntityAliases[] entityAliases = getEntityAliases();

		if ( LOG.isDebugEnabled() ) {
			LOG.debugf( "Result row: %s", StringHelper.toString( keys ) );
		}

		CompletionStage<Object[]> rowResultsStage = RxUtil.completedFuture( new Object[cols] );

		for ( int i = 0; i < cols; i++ ) {
			final int current = i;
			rowResultsStage = rowResultsStage.thenCompose( rowResults -> {
				try {
					return getObject(rs, persisters, keys, queryParameters.getOptionalObject(), optionalObjectKey, lockModes, hydratedObjects, session, entityAliases, current)
							.thenApply( obj -> {
								rowResults[current] = obj;
								return rowResults;
							} );
				}
				catch (SQLException sqle) {
					throw getFactory().getJdbcServices().getSqlExceptionHelper().convert(
							sqle,
							"could not load an entity batch: " + MessageHelper.infoString(
									getEntityPersisters()[0],
									queryParameters.getOptionalId(),
									session.getFactory()
							),
							sql
					);
				}
			});
		}

		return rowResultsStage;
	}

	private CompletionStage<Object> getObject(ResultSet rs, Loadable[] persisters, EntityKey[] keys, Object optionalObject, EntityKey optionalObjectKey, LockMode[] lockModes, List hydratedObjects, SharedSessionContractImplementor session, EntityAliases[] entityAliases, int i) throws SQLException {
		Object object = null;
		EntityKey key = keys[i];
		if ( keys[i] != null ) {
			//If the object is already loaded, return the loaded one
			object = session.getEntityUsingInterceptor( key );
			if ( object != null ) {
				instanceAlreadyLoaded( rs, i, persisters[i], key, object, lockModes[i], hydratedObjects, session);
			}
			else {
				object = instanceNotYetLoaded( rs, i, persisters[i], entityAliases[i].getRowIdAlias(), key, lockModes[i],
						optionalObjectKey, optionalObject, hydratedObjects, session );
			}
		}
		return RxUtil.completedFuture( object );
	}

	protected CompletionStage<List<Object>> executeRxQueryStatement(
			String sqlStatement,
			QueryParameters queryParameters,
			boolean scroll,
			List<AfterLoadAction> afterLoadActions,
			SessionImplementor session,
			Function<ResultSet, List<Object>> transformer) {

		// Processing query filters.
		queryParameters.processFilters( sqlStatement, session );

		// Applying LIMIT clause.
		final LimitHandler limitHandler = getLimitHandler( queryParameters.getRowSelection() );
		String sql = limitHandler.processSql( queryParameters.getFilteredSQL(), queryParameters.getRowSelection() );

		// Adding locks and comments.
		sql = preprocessSQL( sql, queryParameters, getFactory(), afterLoadActions );

		return new RxQueryExecutor().execute( sql, queryParameters, session, transformer );
	}

}
