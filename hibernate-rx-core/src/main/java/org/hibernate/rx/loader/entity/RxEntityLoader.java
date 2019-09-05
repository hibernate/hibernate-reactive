package org.hibernate.rx.loader.entity;

import java.io.Serializable;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.pagination.LimitHelper;
import org.hibernate.engine.jdbc.spi.JdbcCoordinator;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.RowSelection;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.entity.AbstractEntityLoader;
import org.hibernate.loader.entity.EntityJoinWalker;
import org.hibernate.loader.entity.UniqueEntityLoader;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.Loadable;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.rx.impl.RxQueryExecutor;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

/**
 * @see org.hibernate.loader.entity.EntityLoader
 */
public class RxEntityLoader extends AbstractEntityLoader implements UniqueEntityLoader {

	private final boolean batchLoader;
	private final int[][] compositeKeyManyToOneTargetIndices;

	public RxEntityLoader(
			OuterJoinLoadable persister,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this( persister, 1, LockMode.NONE, factory, loadQueryInfluencers );
	}

	// We don't use all the parameters but I kept them for simmetry with EntityLoader
	public RxEntityLoader(
			OuterJoinLoadable persister,
			LockMode lockMode,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this( persister, 1, lockMode, factory, loadQueryInfluencers );
	}

	public RxEntityLoader(
			OuterJoinLoadable persister,
			LockOptions lockOptions,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this( persister, 1, lockOptions, factory, loadQueryInfluencers );
	}

	public RxEntityLoader(
			OuterJoinLoadable persister,
			int batchSize,
			LockMode lockMode,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this(
				persister,
				persister.getIdentifierColumnNames(),
				persister.getIdentifierType(),
				batchSize,
				lockMode,
				factory,
				loadQueryInfluencers
		);
	}

	public RxEntityLoader(
			OuterJoinLoadable persister,
			int batchSize,
			LockOptions lockOptions,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this(
				persister,
				persister.getIdentifierColumnNames(),
				persister.getIdentifierType(),
				batchSize,
				lockOptions,
				factory,
				loadQueryInfluencers
		);
	}

	public RxEntityLoader(
			OuterJoinLoadable persister,
			String[] uniqueKey,
			Type uniqueKeyType,
			int batchSize,
			LockMode lockMode,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this( persister, uniqueKeyType, batchSize, factory, loadQueryInfluencers, new EntityJoinWalker(
				persister,
				uniqueKey,
				batchSize,
				lockMode,
				factory,
				loadQueryInfluencers
		) );
	}

	private RxEntityLoader(
			OuterJoinLoadable persister,
			Type uniqueKeyType,
			int batchSize,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers,
			EntityJoinWalker walker) throws MappingException {
		super( persister, uniqueKeyType, factory, loadQueryInfluencers );
		if ( persister == null ) {
			throw new AssertionFailure( "EntityPersister must not be null or empty" );
		}
		initFromWalker( walker );
		this.compositeKeyManyToOneTargetIndices = walker.getCompositeKeyManyToOneTargetIndices();
		postInstantiate();
		batchLoader = batchSize > 1;
	}


	public RxEntityLoader(
			OuterJoinLoadable persister,
			String[] uniqueKey,
			Type uniqueKeyType,
			int batchSize,
			LockOptions lockOptions,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		super( persister, uniqueKeyType, factory, loadQueryInfluencers );

		EntityJoinWalker walker = new EntityJoinWalker(
				persister,
				uniqueKey,
				batchSize,
				lockOptions,
				factory,
				loadQueryInfluencers
		);
		initFromWalker( walker );
		this.compositeKeyManyToOneTargetIndices = walker.getCompositeKeyManyToOneTargetIndices();
		postInstantiate();
		batchLoader = batchSize > 1;
	}

	@Override
	public Object load(
			Serializable id, Object optionalObject, SharedSessionContractImplementor session)
			throws HibernateException {
		return load( id, optionalObject, session, LockOptions.NONE );
	}

	@Override
	public Object load(
			Serializable id,
			Object optionalObject,
			SharedSessionContractImplementor session,
			LockOptions lockOptions) {
		return load( session, id, optionalObject, id, lockOptions );
	}

	@Override
	protected Object load(
			SharedSessionContractImplementor session,
			Object id,
			Object optionalObject,
			Serializable optionalId,
			LockOptions lockOptions) {

		Object list = loadRxEntity(
				session,
				id,
				uniqueKeyType,
				optionalObject,
				entityName,
				optionalId,
				persister,
				lockOptions
		);

// TODO: We need to validate the result
//		if ( list.size()==1 ) {
//			return list.get(0);
//		}
//		else if ( list.size()==0 ) {
//			return null;
//		}
//		else {
//			if ( getCollectionOwners()!=null ) {
//				return list.get(0);
//			}
//			else {
//				throw new HibernateException(
//						"More than one row with the given identifier was found: " +
//								id +
//								", for class: " +
//								persister.getEntityName()
//				);
//			}
//		}

		return list;
	}

	protected Object loadRxEntity(
			final SharedSessionContractImplementor session,
			final Object id,
			final Type identifierType,
			final Object optionalObject,
			final String optionalEntityName,
			final Serializable optionalIdentifier,
			final EntityPersister persister,
			LockOptions lockOptions) throws HibernateException {

		Object result;
		try {
			QueryParameters qp = new QueryParameters();
			qp.setPositionalParameterTypes( new Type[] { identifierType } );
			qp.setPositionalParameterValues( new Object[] { id } );
			qp.setOptionalObject( optionalObject );
			qp.setOptionalEntityName( optionalEntityName );
			qp.setOptionalId( optionalIdentifier );
			qp.setLockOptions( lockOptions );

			result = doRxQueryAndInitializeNonLazyCollections( session, qp, false );
		}
		catch (SQLException sqle) {
			final Loadable[] persisters = getEntityPersisters();
			throw this.getFactory().getJdbcServices().getSqlExceptionHelper().convert(
					sqle,
					"could not load an entity: " +
							MessageHelper.infoString(
									persisters[persisters.length - 1],
									id,
									identifierType,
									getFactory()
							),
					getSQLString()
			);
		}

		LOG.debug( "Done entity load" );
		return result;
	}

	public Object doRxQueryAndInitializeNonLazyCollections(
			final SharedSessionContractImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies)
			throws HibernateException, SQLException {
		return doRxQueryAndInitializeNonLazyCollections( session, queryParameters, returnProxies, null );
	}

	public Object doRxQueryAndInitializeNonLazyCollections(
			final SharedSessionContractImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies,
			final ResultTransformer forcedResultTransformer)
			throws HibernateException, SQLException {
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
		Object result;
		try {
			try {
				result = doRxQuery( session, queryParameters, returnProxies, forcedResultTransformer );
			}
			finally {
				persistenceContext.afterLoad();
			}
			persistenceContext.initializeNonLazyCollections();
		}
		finally {
			// Restore the original default
			persistenceContext.setDefaultReadOnly( defaultReadOnlyOrig );
		}
		return result;
	}

	private Object doRxQuery(
			final SharedSessionContractImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies,
			final ResultTransformer forcedResultTransformer) throws SQLException, HibernateException {

		final RowSelection selection = queryParameters.getRowSelection();
		final int maxRows = LimitHelper.hasMaxRows( selection ) ?
				selection.getMaxRows() :
				Integer.MAX_VALUE;

		final List<AfterLoadAction> afterLoadActions = new ArrayList<AfterLoadAction>();

		final CompletionStage<Object> result = executeRxQueryStatement(
				queryParameters,
				false,
				afterLoadActions,
				session
		);

		return processResult(
				result,
				queryParameters,
				session,
				returnProxies,
				forcedResultTransformer,
				maxRows,
				afterLoadActions
		);
	}

	private static EntityKey getOptionalObjectKey(QueryParameters queryParameters, SharedSessionContractImplementor session) {
		final Object optionalObject = queryParameters.getOptionalObject();
		final Serializable optionalId = queryParameters.getOptionalId();
		final String optionalEntityName = queryParameters.getOptionalEntityName();

		if ( optionalObject != null && optionalEntityName != null ) {
			return session.generateEntityKey(
					optionalId, session.getEntityPersister(
							optionalEntityName,
							optionalObject
					)
			);
		}
		else {
			return null;
		}
	}

	protected CompletionStage<Object> processResult(
			CompletionStage<Object> result,
			QueryParameters queryParameters,
			SharedSessionContractImplementor session,
			boolean returnProxies,
			ResultTransformer forcedResultTransformer,
			int maxRows,
			List<AfterLoadAction> afterLoadActions) throws SQLException {
		final int entitySpan = getEntityPersisters().length;
		final EntityKey optionalObjectKey = getOptionalObjectKey( queryParameters, session );
		final LockMode[] lockModesArray = getLockModes( queryParameters.getLockOptions() );
		final boolean createSubselects = isSubselectLoadingEnabled();
		final List subselectResultKeys = createSubselects ? new ArrayList() : null;
		final ArrayList hydratedObjects = entitySpan == 0 ? null : new ArrayList( entitySpan * 10 );

//		handleEmptyCollections( queryParameters.getCollectionKeys(), rs, session );
		EntityKey[] keys = new EntityKey[entitySpan]; //we can reuse it for each row
		LOG.trace( "Processing result set" );
		int count;

//		final boolean debugEnabled = LOG.isDebugEnabled();
//		for ( count = 0; count < maxRows && rs.next(); count++ ) {
//			if ( debugEnabled ) {
//				LOG.debugf( "Result set row: %s", count );
//			}
//			Object row = getRowFromResultSet(
//					result,
//					session,
//					queryParameters,
//					lockModesArray,
//					optionalObjectKey,
//					hydratedObjects,
//					keys,
//					returnProxies,
//					forcedResultTransformer
//			);
//			results.add( result );
//			if ( createSubselects ) {
//				subselectResultKeys.add( keys );
//				keys = new EntityKey[entitySpan]; //can't reuse in this case
//			}
//		}

//		LOG.tracev( "Done processing result set ({0} rows)", count );
//
//		initializeEntitiesAndCollections(
//				hydratedObjects,
//				rs,
//				session,
//				queryParameters.isReadOnly( session ),
//				afterLoadActions
//		);
//		if ( createSubselects ) {
//			createSubselects( subselectResultKeys, queryParameters, session );
//		}
		return result;
	}

	protected void extractKeysFromResult(
			Loadable[] persisters,
			QueryParameters queryParameters,
			CompletionStage<Object> resultSet,
			SharedSessionContractImplementor session,
			EntityKey[] keys,
			LockMode[] lockModes,
			List hydratedObjects) throws SQLException {
		final int entitySpan = persisters.length;

		final int numberOfPersistersToProcess;
		final Serializable optionalId = queryParameters.getOptionalId();
		if ( isSingleRowLoader() && optionalId != null ) {
			keys[entitySpan - 1] = session.generateEntityKey( optionalId, persisters[entitySpan - 1] );
			// skip the last persister below...
			numberOfPersistersToProcess = entitySpan - 1;
		}
		else {
			numberOfPersistersToProcess = entitySpan;
		}

		final Object[] hydratedKeyState = new Object[numberOfPersistersToProcess];
//
//		for ( int i = 0; i < numberOfPersistersToProcess; i++ ) {
//			final Type idType = persisters[i].getIdentifierType();
//			hydratedKeyState[i] = idType.hydrate(
//					resultSet,
//					getEntityAliases()[i].getSuffixedKeyAliases(),
//					session,
//					null
//			);
//		}

		for ( int i = 0; i < numberOfPersistersToProcess; i++ ) {
			final Type idType = persisters[i].getIdentifierType();
			if ( idType.isComponentType() && getCompositeKeyManyToOneTargetIndices() != null ) {
				// we may need to force resolve any key-many-to-one(s)
				int[] keyManyToOneTargetIndices = getCompositeKeyManyToOneTargetIndices()[i];
				// todo : better solution is to order the index processing based on target indices
				//		that would account for multiple levels whereas this scheme does not
				if ( keyManyToOneTargetIndices != null ) {
					for ( int targetIndex : keyManyToOneTargetIndices ) {
						if ( targetIndex < numberOfPersistersToProcess ) {
							final Type targetIdType = persisters[targetIndex].getIdentifierType();
							final Serializable targetId = (Serializable) targetIdType.resolve(
									hydratedKeyState[targetIndex],
									session,
									null
							);
							// todo : need a way to signal that this key is resolved and its data resolved
							keys[targetIndex] = session.generateEntityKey( targetId, persisters[targetIndex] );
						}

						// this part copied from #getRow, this section could be refactored out
						Object object = session.getEntityUsingInterceptor( keys[targetIndex] );
//						if ( object != null ) {
//							//its already loaded so don't need to hydrate it
//							instanceAlreadyLoaded(
//									resultSet,
//									targetIndex,
//									persisters[targetIndex],
//									keys[targetIndex],
//									object,
//									lockModes[targetIndex],
//									hydratedObjects,
//									session
//							);
//						}
//						else {
//							instanceNotYetLoaded(
//									resultSet,
//									targetIndex,
//									persisters[targetIndex],
//									getEntityAliases()[targetIndex].getRowIdAlias(),
//									keys[targetIndex],
//									lockModes[targetIndex],
//									getOptionalObjectKey( queryParameters, session ),
//									queryParameters.getOptionalObject(),
//									hydratedObjects,
//									session
//							);
//						}
					}
				}
			}
			// If hydratedKeyState[i] is null, then we know the association should be null.
			// Don't bother resolving the ID if hydratedKeyState[i] is null.

			// Implementation note: if the ID is a composite ID, then resolving a null value will
			// result in instantiating an empty composite if AvailableSettings#CREATE_EMPTY_COMPOSITES_ENABLED
			// is true. By not resolving a null value for a composite ID, we avoid the overhead of instantiating
			// an empty composite, checking if it is equivalent to null (it should be), then ultimately throwing
			// out the empty value.
			final Serializable resolvedId;
			if ( hydratedKeyState[i] != null ) {
				resolvedId = (Serializable) idType.resolve( hydratedKeyState[i], session, null );
			}
			else {
				resolvedId = null;
			}
			keys[i] = resolvedId == null ? null : session.generateEntityKey( resolvedId, persisters[i] );
		}
	}

//
//	private Object getRowFromResultSet(
//			final ResultSet resultSet,
//			final SharedSessionContractImplementor session,
//			final QueryParameters queryParameters,
//			final LockMode[] lockModesArray,
//			final EntityKey optionalObjectKey,
//			final List hydratedObjects,
//			final EntityKey[] keys,
//			boolean returnProxies) throws SQLException, HibernateException {
//		return getRowFromResultSet(
//				resultSet,
//				session,
//				queryParameters,
//				lockModesArray,
//				optionalObjectKey,
//				hydratedObjects,
//				keys,
//				returnProxies,
//				null
//		);
//	}
//
//	private Object getRowFromResultSet(
//			final CompletionStage<Object> resultSet,
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
//		extractKeysFromResultSet(
//				persisters,
//				queryParameters,
//				resultSet,
//				session,
//				keys,
//				lockModesArray,
//				hydratedObjects
//		);
//
////		registerNonExists( keys, persisters, session );
//
//		// this call is side-effecty
//		Object[] row = getRow(
//				resultSet,
//				persisters,
//				keys,
//				queryParameters.getOptionalObject(),
//				optionalObjectKey,
//				lockModesArray,
//				hydratedObjects,
//				session
//		);
//
////		readCollectionElements( row, resultSet, session );
//
//		if ( returnProxies ) {
//			// now get an existing proxy for each row element (if there is one)
//			final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
//			for ( int i = 0; i < entitySpan; i++ ) {
//				Object entity = row[i];
//				Object proxy = persistenceContext.proxyFor( persisters[i], keys[i], entity );
//				if ( entity != proxy ) {
//					// force the proxy to resolve itself
//					( (HibernateProxy) proxy ).getHibernateLazyInitializer().setImplementation( entity );
//					row[i] = proxy;
//				}
//			}
//		}
//
//		applyPostLoadLocks( row, lockModesArray, session );
//
//		return forcedResultTransformer == null
//				? getResultColumnOrRow( row, queryParameters.getResultTransformer(), resultSet, session )
//				: forcedResultTransformer.transformTuple(
//				getResultRow( row, resultSet, session ),
//				getResultRowAliases()
//		)
//				;
//	}

	protected CompletionStage<Object> executeRxQueryStatement(
			final QueryParameters queryParameters,
			final boolean scroll,
			List<AfterLoadAction> afterLoadActions,
			final SharedSessionContractImplementor session) throws SQLException {
		String sql = getSQLString();
		return executeRxQueryStatement( sql, queryParameters, scroll, afterLoadActions, session );
	}

	protected CompletionStage<Object> executeRxQueryStatement(
			String sqlStatement,
			QueryParameters queryParameters,
			boolean scroll	,
			List<AfterLoadAction> afterLoadActions,
			SharedSessionContractImplementor session) throws SQLException {

		// Processing query filters.
		queryParameters.processFilters( sqlStatement, session );

		// Applying LIMIT clause.
		final LimitHandler limitHandler = getLimitHandler(
				queryParameters.getRowSelection()
		);
		String sql = limitHandler.processSql( queryParameters.getFilteredSQL(), queryParameters.getRowSelection() );

		// Adding locks and comments.
		sql = preprocessSQL( sql, queryParameters, getFactory(), afterLoadActions );

//		final PreparedStatement st = prepareQueryStatement( sql, queryParameters, limitHandler, scroll, session );

		RxQueryExecutor executor = new RxQueryExecutor();
		CompletionStage<Object> result = executor.execute( sql, queryParameters, getFactory() );
//		final ResultSet rs = getResultSet(
//					st,
//					queryParameters.getRowSelection(),
//					limitHandler,
//					queryParameters.hasAutoDiscoverScalarTypes(),
//					session
//			);

		return result;
	}
}
