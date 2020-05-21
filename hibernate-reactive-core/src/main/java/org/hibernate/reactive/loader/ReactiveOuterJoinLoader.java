package org.hibernate.reactive.loader;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.HibernateException;
import org.hibernate.JDBCException;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.pagination.LimitHelper;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.RowSelection;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.loader.OuterJoinLoader;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.reactive.engine.impl.ReactivePersistenceContextAdapter;
import org.hibernate.reactive.impl.ReactiveSessionInternal;
import org.hibernate.transform.ResultTransformer;

import static org.hibernate.reactive.adaptor.impl.QueryParametersAdaptor.toParameterArray;

/**
 * Reactive version of {@link OuterJoinLoader}
 * <p>
 *     This class follows the same structure of {@link OuterJoinLoader} with the methods signature change so that
 *     it's possible to return a {@link CompletionStage}
 * </p>
 */
public class ReactiveOuterJoinLoader extends OuterJoinLoader {

	public ReactiveOuterJoinLoader(SessionFactoryImplementor factory, LoadQueryInfluencers loadQueryInfluencers) {
		super(factory, loadQueryInfluencers);
	}

	public CompletionStage<List<Object>> doReactiveQueryAndInitializeNonLazyCollections(
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies) {
		return doReactiveQueryAndInitializeNonLazyCollections( getSQLString(), session, queryParameters, returnProxies );
	}

	protected CompletionStage<List<Object>> doReactiveQueryAndInitializeNonLazyCollections(
			final String sql,
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies) {
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
		return doReactiveQuery( sql, session, queryParameters, returnProxies )
				.whenComplete( (list, e) -> persistenceContext.afterLoad() )
				.thenCompose( list ->
					// only initialize non-lazy collections after everything else has been refreshed
					 ((ReactivePersistenceContextAdapter) persistenceContext ).reactiveInitializeNonLazyCollections()
							.thenApply(v -> list)
				)
				.whenComplete( (list, e) -> persistenceContext.setDefaultReadOnly(defaultReadOnlyOrig) );
	}

	private CompletionStage<List<Object>> doReactiveQuery(
			String sql,
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies) throws HibernateException {

		final RowSelection selection = queryParameters.getRowSelection();
		final int maxRows = LimitHelper.hasMaxRows( selection ) ?
				selection.getMaxRows() :
				Integer.MAX_VALUE;

		final List<AfterLoadAction> afterLoadActions = new ArrayList<>();

		return executeReactiveQueryStatement(
				sql,
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
								returnProxies,
								null,
								maxRows,
								afterLoadActions
						);
					}
					catch (SQLException sqle) {
						//don't log or convert it - just pass it on to the caller
						throw new JDBCException( "could not load batch", sqle );
					}
				}
		);
	}

	// Added this just to catch the exception and make the code more readable
//	private CompletionStage<Object> reactiveRows(ResultSet rs, QueryParameters queryParameters, SharedSessionContractImplementor session, boolean returnProxies, ResultTransformer forcedResultTransformer, EntityKey optionalObjectKey, LockMode[] lockModesArray, boolean createSubselects, List subselectResultKeys, List hydratedObjects, EntityKey[] keys) {
//		try {
//			return getReactiveRowFromResultSet( rs, session, queryParameters, lockModesArray, optionalObjectKey, hydratedObjects,
//					keys, returnProxies, forcedResultTransformer
//			);
//		} catch (SQLException sqle) {
//			throw getFactory().getJdbcServices().getSqlExceptionHelper().convert(
//					sqle,
//					"could not load an entity batch: " + MessageHelper.infoString(
//							getEntityPersisters()[0],
//							queryParameters.getOptionalId(),
//							session.getFactory()
//					),
//					sql
//			);
//		}
//	}

//	private CompletionStage<Object> getReactiveRowFromResultSet(
//			final ResultSet resultSet,
//			final SharedSessionContractImplementor session,
//			final QueryParameters queryParameters,
//			final LockMode[] lockModesArray,
//			final EntityKey optionalObjectKey,
//			final List hydratedObjects,
//			final EntityKey[] keys,
//			boolean returnProxies,
//			ResultTransformer forcedResultTransformer) throws SQLException, HibernateException {
//		final Loadable[] persisters = getEntityPersisters();
//		final int entitySpan = persisters.length;
//		extractKeysFromResultSet( persisters, queryParameters, resultSet, session, keys, lockModesArray, hydratedObjects );
//
////		FIXME: for later
//		registerNonExists( keys, persisters, session );
//
//		// this call is side-effecty
//		return getReactiveRow( resultSet, persisters, keys, queryParameters, optionalObjectKey, lockModesArray, hydratedObjects, session )
//				.thenApply( row -> {
//
//			//		TODO: We don't do collection of elements for now
//			//		readCollectionElements( row, resultSet, session );
//
//					if ( returnProxies ) {
//						// now get an existing proxy for each row element (if there is one)
//						final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
//						for ( int i = 0; i < entitySpan; i++ ) {
//							Object entity = row[i];
//							Object proxy = persistenceContext.proxyFor( persisters[i], keys[i], entity );
//							if ( entity != proxy ) {
//								// force the proxy to resolve itself
//								( (HibernateProxy) proxy ).getHibernateLazyInitializer().setImplementation( entity );
//								row[i] = proxy;
//							}
//						}
//					}
//
//					applyPostLoadLocks( row, lockModesArray, session );
//
//					try {
//						return forcedResultTransformer == null
//								? getResultColumnOrRow( row, queryParameters.getResultTransformer(), resultSet, session )
//								: forcedResultTransformer.transformTuple( getResultRow( row, resultSet, session ),  getResultRowAliases());
//
//					}
//					catch (SQLException sqle) {
//						throw getFactory().getJdbcServices().getSqlExceptionHelper().convert(
//								sqle,
//								"could not load an entity batch: " + MessageHelper.infoString(
//										getEntityPersisters()[0],
//										queryParameters.getOptionalId(),
//										session.getFactory()
//								),
//								sql
//						);
//					}
//				});
//	}

//	/**
//	 * Resolve any IDs for currently loaded objects, duplications within the
//	 * <tt>ResultSet</tt>, etc. Instantiate empty objects to be initialized from the
//	 * <tt>ResultSet</tt>. Return an array of objects (a row of results) and an
//	 * array of booleans (by side-effect) that determine whether the corresponding
//	 * object should be initialized.
//	 */
//	private CompletionStage<Object[]> getReactiveRow(
//			final ResultSet rs,
//			final Loadable[] persisters,
//			final EntityKey[] keys,
//			final QueryParameters queryParameters,
//			final EntityKey optionalObjectKey,
//			final LockMode[] lockModes,
//			final List hydratedObjects,
//			final SharedSessionContractImplementor session) throws HibernateException, SQLException {
//		final int cols = persisters.length;
//		final EntityAliases[] entityAliases = getEntityAliases();
//
//		if ( LOG.isDebugEnabled() ) {
//			LOG.debugf( "Result row: %s", StringHelper.toString( keys ) );
//		}
//
//		CompletionStage<Object[]> rowResultsStage = CompletionStages.completedFuture( new Object[cols] );
//
//		for ( int i = 0; i < cols; i++ ) {
//			final int current = i;
//			rowResultsStage = rowResultsStage.thenCompose( rowResults -> {
//				try {
//					return getObject(rs, persisters, keys, queryParameters.getOptionalObject(), optionalObjectKey, lockModes, hydratedObjects, session, entityAliases, current)
//							.thenApply( obj -> {
//								rowResults[current] = obj;
//								return rowResults;
//							} );
//				}
//				catch (SQLException sqle) {
//					throw getFactory().getJdbcServices().getSqlExceptionHelper().convert(
//							sqle,
//							"could not load an entity batch: " + MessageHelper.infoString(
//									getEntityPersisters()[0],
//									queryParameters.getOptionalId(),
//									session.getFactory()
//							),
//							sql
//					);
//				}
//			});
//		}
//
//		return rowResultsStage;
//	}

//	private CompletionStage<Object> getObject(ResultSet rs, Loadable[] persisters, EntityKey[] keys, Object optionalObject, EntityKey optionalObjectKey, LockMode[] lockModes, List hydratedObjects, SharedSessionContractImplementor session, EntityAliases[] entityAliases, int i) throws SQLException {
//		Object object = null;
//		EntityKey key = keys[i];
//		if ( keys[i] != null ) {
//			//If the object is already loaded, return the loaded one
//			object = session.getEntityUsingInterceptor( key );
//			if ( object != null ) {
//				instanceAlreadyLoaded( rs, i, persisters[i], key, object, lockModes[i], hydratedObjects, session);
//			}
//			else {
//				object = instanceNotYetLoaded( rs, i, persisters[i], entityAliases[i].getRowIdAlias(), key, lockModes[i],
//						optionalObjectKey, optionalObject, hydratedObjects, session );
//			}
//		}
//		return CompletionStages.completedFuture( object );
//	}

	protected CompletionStage<List<Object>> executeReactiveQueryStatement(
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

		return session.unwrap(ReactiveSessionInternal.class)
				.getReactiveConnection()
				.selectJdbc( sql, toParameterArray(queryParameters, session) )
				.thenApply( transformer );
	}

}
