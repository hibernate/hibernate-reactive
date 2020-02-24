package org.hibernate.rx.loader.entity.impl;

import org.hibernate.*;
import org.hibernate.bytecode.enhance.spi.interceptor.EnhancementAsProxyLazinessInterceptor;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.entry.CacheEntry;
import org.hibernate.cache.spi.entry.ReferenceCacheEntryImpl;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.pagination.LimitHelper;
import org.hibernate.engine.internal.CacheHelper;
import org.hibernate.engine.internal.TwoPhaseLoad;
import org.hibernate.engine.spi.*;
import org.hibernate.event.service.spi.EventListenerGroup;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.*;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.loader.CollectionAliases;
import org.hibernate.loader.EntityAliases;
import org.hibernate.loader.entity.AbstractEntityLoader;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.Loadable;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.persister.entity.UniqueKeyLoadable;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.rx.engine.impl.RxTwoPhaseLoad;
import org.hibernate.rx.impl.RxQueryExecutor;
import org.hibernate.rx.util.impl.RxUtil;
import org.hibernate.stat.spi.StatisticsImplementor;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.AssociationType;
import org.hibernate.type.EntityType;
import org.hibernate.type.Type;
import org.hibernate.type.VersionType;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class RxAbstractEntityLoader extends AbstractEntityLoader {

	private final boolean enhancementAsProxyEnabled;

	public RxAbstractEntityLoader(OuterJoinLoadable persister, Type uniqueKeyType, SessionFactoryImplementor factory,
								  LoadQueryInfluencers loadQueryInfluencers) {
		super(persister, uniqueKeyType, factory, loadQueryInfluencers);
		this.enhancementAsProxyEnabled = factory.getSessionFactoryOptions().isEnhancementAsProxyEnabled();
	}

	@Override
	protected CompletionStage<Optional<Object>> load(
			SharedSessionContractImplementor session,
			Object id,
			Object optionalObject,
			Serializable optionalId,
			LockOptions lockOptions,
			Boolean readOnly) {

		return loadRxEntity(
				(SessionImplementor) session,
				id,
				uniqueKeyType,
				optionalObject,
				entityName,
				optionalId,
				persister,
				lockOptions
		).thenApply( list -> {
			switch ( list.size() ) {
				case 1:
					return Optional.ofNullable( list.get( 0 ) );
				case 0:
					return Optional.empty();
				default:
					if ( getCollectionOwners() != null ) {
						return Optional.ofNullable( list.get( 0 ) );
					}
			}
			throw new HibernateException(
					"More than one row with the given identifier was found: " +
							id +
							", for class: " +
							persister.getEntityName()
			);
		} );
	}

	protected CompletionStage<List<?>> loadRxEntity(
			final SessionImplementor session,
			final Object id,
			final Type identifierType,
			final Object optionalObject,
			final String optionalEntityName,
			final Serializable optionalIdentifier,
			final EntityPersister persister,
			LockOptions lockOptions) throws HibernateException {

		QueryParameters qp = new QueryParameters();
		qp.setPositionalParameterTypes( new Type[] { identifierType } );
		qp.setPositionalParameterValues( new Object[] { id } );
		qp.setOptionalObject( optionalObject );
		qp.setOptionalEntityName( optionalEntityName );
		qp.setOptionalId( optionalIdentifier );
		qp.setLockOptions( lockOptions );

		return doRxQueryAndInitializeNonLazyCollections( session, qp, false )
			.handle( (list, e) -> {
				LOG.debug( "Done entity load" );
				if (e instanceof SQLException) {
					final Loadable[] persisters = getEntityPersisters();
					throw this.getFactory().getJdbcServices().getSqlExceptionHelper().convert(
							(SQLException) e,
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
				else if (e !=null ) {
					RxUtil.rethrow(e);
				}
				return list;
			});
	}

	protected CompletionStage<List<?>> doRxQueryAndInitializeNonLazyCollections(
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies) {
		return doRxQueryAndInitializeNonLazyCollections( session, queryParameters, returnProxies, null );
	}

	protected CompletionStage<List<?>> doRxQueryAndInitializeNonLazyCollections(
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
					if (e == null) {
						persistenceContext.initializeNonLazyCollections();
					}
					persistenceContext.setDefaultReadOnly(defaultReadOnlyOrig);
					if (e != null) {
						RxUtil.rethrow(e);
					}
					return list;
				});
	}

	private CompletionStage<List<?>> doRxQuery(
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies,
			final ResultTransformer forcedResultTransformer) throws HibernateException {

		final RowSelection selection = queryParameters.getRowSelection();
		final int maxRows = LimitHelper.hasMaxRows( selection ) ?
				selection.getMaxRows() :
				Integer.MAX_VALUE;

		final List<AfterLoadAction> afterLoadActions = new ArrayList<AfterLoadAction>();

		return executeRxQueryStatement(
				getSQLString(), queryParameters, false, afterLoadActions, session,
				resultSet -> {
					try {
						return rxProcessResultSet(
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

	private CompletionStage<Void> initializeEntitiesAndCollections(
			final List hydratedObjects,
			final Object resultSetId,
			final SharedSessionContractImplementor session,
			final boolean readOnly,
			List<AfterLoadAction> afterLoadActions) throws HibernateException {

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

		//important: reuse the same event instances for performance!
		final PreLoadEvent pre;
		final PostLoadEvent post;
		if ( session.isEventSource() ) {
			pre = new PreLoadEvent( (EventSource) session );
			post = new PostLoadEvent( (EventSource) session );
		}
		else {
			pre = null;
			post = null;
		}

		CompletionStage<Void> stage = RxUtil.nullFuture();
		if ( hydratedObjects != null ) {
			int hydratedObjectsSize = hydratedObjects.size();
			LOG.tracev( "Total objects hydrated: {0}", hydratedObjectsSize );

			if ( hydratedObjectsSize != 0 ) {
				final Iterable<PreLoadEventListener> listeners = session
						.getFactory()
						.getServiceRegistry()
						.getService( EventListenerRegistry.class )
						.getEventListenerGroup( EventType.PRE_LOAD )
						.listeners();

				for ( Object hydratedObject : hydratedObjects ) {
					stage = stage.thenCompose(v -> {
						CompletionStage<Void> completionStage = RxTwoPhaseLoad.initializeEntity(hydratedObject, readOnly, session, pre, listeners);
						return completionStage;
					});
				}

			}
		}

		return stage.thenAccept( v -> {
			if ( collectionPersisters != null ) {
				for ( CollectionPersister collectionPersister : collectionPersisters ) {
					if ( !collectionPersister.isArray() ) {
						//for sets, we should end the collection load after resolving
						//the entities, since we might call hashCode() on the elements
						//TODO: or we could do this polymorphically, and have two
						//      different operations implemented differently for arrays
						endCollectionLoad( resultSetId, session, collectionPersister );
					}
				}
			}

			if ( hydratedObjects != null ) {
				for ( Object hydratedObject : hydratedObjects ) {
					RxTwoPhaseLoad.afterInitialize( hydratedObject, session );
				}
			}

			// Until this entire method is refactored w/ polymorphism, postLoad was
			// split off from initializeEntity.  It *must* occur after
			// endCollectionLoad to ensure the collection is in the
			// persistence context.
			final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
			if ( hydratedObjects != null && hydratedObjects.size() > 0 ) {

				final Iterable<PostLoadEventListener> postLoadEventListeners;
				if ( session.isEventSource() ) {
					final EventListenerGroup<PostLoadEventListener> listenerGroup = session.getFactory()
							.getServiceRegistry()
							.getService( EventListenerRegistry.class )
							.getEventListenerGroup( EventType.POST_LOAD );
					postLoadEventListeners = listenerGroup.listeners();
				}
				else {
					postLoadEventListeners = Collections.emptyList();
				}

				for ( Object hydratedObject : hydratedObjects ) {
					RxTwoPhaseLoad.postLoad( hydratedObject, session, post, postLoadEventListeners );
					if ( afterLoadActions != null ) {
						for ( AfterLoadAction afterLoadAction : afterLoadActions ) {
							final EntityEntry entityEntry = persistenceContext.getEntry( hydratedObject );
							if ( entityEntry == null ) {
								// big problem
								throw new HibernateException(
										"Could not locate EntityEntry immediately after two-phase load"
								);
							}
							afterLoadAction.afterLoad( session, hydratedObject, (Loadable) entityEntry.getPersister() );
						}
					}
				}
			}
		});
	}

	protected CompletionStage<List<?>> rxProcessResultSet(
			ResultSet rs,
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
		final List hydratedObjects = entitySpan == 0 ? null : new ArrayList( entitySpan * 10 );
		final List results = new ArrayList();

		handleEmptyCollections( queryParameters.getCollectionKeys(), rs, session );
		EntityKey[] keys = new EntityKey[entitySpan]; //we can reuse it for each row
		LOG.trace( "Processing result set" );
		int count;

		final boolean debugEnabled = LOG.isDebugEnabled();
		for ( count = 0; count < maxRows && rs.next(); count++ ) {
			if ( debugEnabled ) {
				LOG.debugf( "Result set row: %s", count );
			}
			Object result = getRowFromResultSet(
					rs,
					session,
					queryParameters,
					lockModesArray,
					optionalObjectKey,
					hydratedObjects,
					keys,
					returnProxies,
					forcedResultTransformer
			);
			results.add( result );
			if ( createSubselects ) {
				subselectResultKeys.add( keys );
				keys = new EntityKey[entitySpan]; //can't reuse in this case
			}
		}

		LOG.tracev( "Done processing result set ({0} rows)", count );

		return initializeEntitiesAndCollections(
				hydratedObjects,
				rs,
				session,
				queryParameters.isReadOnly( session ),
				afterLoadActions
		).thenApply( v -> {
			if ( createSubselects ) {
				createSubselects( subselectResultKeys, queryParameters, session );
			}
			return results;
		});
	}

	private Object getRowFromResultSet(
			final ResultSet resultSet,
			final SharedSessionContractImplementor session,
			final QueryParameters queryParameters,
			final LockMode[] lockModesArray,
			final EntityKey optionalObjectKey,
			final List hydratedObjects,
			final EntityKey[] keys,
			boolean returnProxies,
			ResultTransformer forcedResultTransformer) throws SQLException, HibernateException {
		final int entitySpan = persisters.length;
		extractKeysFromResultSet(
				persisters,
				queryParameters,
				resultSet,
				session,
				keys,
				lockModesArray,
				hydratedObjects
		);

		registerNonExists( keys, persisters, session );

		// this call is side-effecty
		Object[] row = getRow(
				resultSet,
				persisters,
				keys,
				queryParameters.getOptionalObject(),
				optionalObjectKey,
				lockModesArray,
				hydratedObjects,
				session
		);

		readCollectionElements( row, resultSet, session );

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

		return forcedResultTransformer == null
				? getResultColumnOrRow( row, queryParameters.getResultTransformer(), resultSet, session )
				: forcedResultTransformer.transformTuple(
				getResultRow( row, resultSet, session ),
				getResultRowAliases()
		);
	}


	/**
	 * For missing objects associated by one-to-one with another object in the
	 * result set, register the fact that the the object is missing with the
	 * session.
	 */
	private void registerNonExists(
			final EntityKey[] keys,
			final Loadable[] persisters,
			final SharedSessionContractImplementor session) {

		final int[] owners = getOwners();
		if ( owners != null ) {

			EntityType[] ownerAssociationTypes = getOwnerAssociationTypes();
			final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
			for ( int i = 0; i < keys.length; i++ ) {

				int owner = owners[i];
				if ( owner > -1 ) {
					EntityKey ownerKey = keys[owner];
					if ( keys[i] == null && ownerKey != null ) {


						/*final boolean isPrimaryKey;
						final boolean isSpecialOneToOne;
						if ( ownerAssociationTypes == null || ownerAssociationTypes[i] == null ) {
							isPrimaryKey = true;
							isSpecialOneToOne = false;
						}
						else {
							isPrimaryKey = ownerAssociationTypes[i].getRHSUniqueKeyPropertyName()==null;
							isSpecialOneToOne = ownerAssociationTypes[i].getLHSPropertyName()!=null;
						}*/

						//TODO: can we *always* use the "null property" approach for everything?
						/*if ( isPrimaryKey && !isSpecialOneToOne ) {
							persistenceContext.addNonExistantEntityKey(
									new EntityKey( ownerKey.getIdentifier(), persisters[i], session.getEntityMode() )
							);
						}
						else if ( isSpecialOneToOne ) {*/
						boolean isOneToOneAssociation = ownerAssociationTypes != null &&
								ownerAssociationTypes[i] != null &&
								ownerAssociationTypes[i].isOneToOne();
						if ( isOneToOneAssociation ) {
							persistenceContext.addNullProperty(
									ownerKey,
									ownerAssociationTypes[i].getPropertyName()
							);
						}
						/*}
						else {
							persistenceContext.addNonExistantEntityUniqueKey( new EntityUniqueKey(
									persisters[i].getEntityName(),
									ownerAssociationTypes[i].getRHSUniqueKeyPropertyName(),
									ownerKey.getIdentifier(),
									persisters[owner].getIdentifierType(),
									session.getEntityMode()
							) );
						}*/
					}
				}
			}
		}
	}

	private void createSubselects(List keys, QueryParameters queryParameters, SharedSessionContractImplementor session) {
		if ( keys.size() > 1 ) { //if we only returned one entity, query by key is more efficient

			Set[] keySets = transpose( keys );

			Map namedParameterLocMap = buildNamedParameterLocMap( queryParameters );

			final Loadable[] loadables = getEntityPersisters();
			final String[] aliases = getAliases();
			final String subselectQueryString = SubselectFetch.createSubselectFetchQueryFragment( queryParameters );
			final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
			final BatchFetchQueue batchFetchQueue = persistenceContext.getBatchFetchQueue();
			for ( Object key : keys ) {
				final EntityKey[] rowKeys = (EntityKey[]) key;
				for ( int i = 0; i < rowKeys.length; i++ ) {

					if ( rowKeys[i] != null && loadables[i].hasSubselectLoadableCollections() ) {

						SubselectFetch subselectFetch = new SubselectFetch(
								subselectQueryString,
								aliases[i],
								loadables[i],
								queryParameters,
								keySets[i],
								namedParameterLocMap
						);

						batchFetchQueue
								.addSubselect( rowKeys[i], subselectFetch );
					}

				}

			}
		}
	}

	private Map buildNamedParameterLocMap(QueryParameters queryParameters) {
		if ( queryParameters.getNamedParameters() != null ) {
			final Map namedParameterLocMap = new HashMap();
			for ( String name : queryParameters.getNamedParameters().keySet() ) {
				namedParameterLocMap.put(
						name,
						getNamedParameterLocs( name )
				);
			}
			return namedParameterLocMap;
		}
		else {
			return null;
		}
	}

	private static Set[] transpose(List keys) {
		Set[] result = new Set[( (EntityKey[]) keys.get( 0 ) ).length];
		for ( int j = 0; j < result.length; j++ ) {
			result[j] = new HashSet( keys.size() );
			for ( Object key : keys ) {
				result[j].add( ( (EntityKey[]) key )[j] );
			}
		}
		return result;
	}

	/**
	 * Read any collection elements contained in a single row of the result set
	 */
	private CompletionStage<Void> readCollectionElements(Object[] row, ResultSet resultSet, SharedSessionContractImplementor session)
			throws SQLException, HibernateException {

		//TODO: make this handle multiple collection roles!

		CompletionStage<Void> stage = RxUtil.nullFuture();
		final CollectionPersister[] collectionPersisters = getCollectionPersisters();
		if ( collectionPersisters != null ) {

			final CollectionAliases[] descriptors = getCollectionAliases();
			final int[] collectionOwners = getCollectionOwners();

			for ( int i = 0; i < collectionPersisters.length; i++ ) {

				final boolean hasCollectionOwners = collectionOwners != null &&
						collectionOwners[i] > -1;
				//true if this is a query and we are loading multiple instances of the same collection role
				//otherwise this is a CollectionInitializer and we are loading up a single collection or batch

				final Object owner = hasCollectionOwners ?
						row[collectionOwners[i]] :
						null; //if null, owner will be retrieved from session

				final CollectionPersister collectionPersister = collectionPersisters[i];
				final Serializable key;
				if ( owner == null ) {
					key = null;
				}
				else {
					key = collectionPersister.getCollectionType().getKeyOfOwner( owner, session );
					//TODO: old version did not require hashmap lookup:
					//keys[collectionOwner].getIdentifier()
				}

				final int index = i;
				stage = stage.thenCompose( v -> readCollectionElement(
						owner,
						key,
						collectionPersister,
						descriptors[index],
						resultSet,
						session
				) );

			}

		}
		return stage;
	}


	/**
	 * Read one collection element from the current row of the JDBC result set
	 */
	private CompletionStage<Void> readCollectionElement(
			final Object optionalOwner,
			final Serializable optionalKey,
			final CollectionPersister persister,
			final CollectionAliases descriptor,
			final ResultSet rs,
			final SharedSessionContractImplementor session)
			throws HibernateException {

		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();

		final Serializable collectionRowKey;
		try {
			collectionRowKey = (Serializable) persister.readKey(
					rs,
					descriptor.getSuffixedKeyAliases(),
					session
			);
		} catch (SQLException e) {
			throw new HibernateException( e );
		}

		CompletionStage<Void> stage = RxUtil.nullFuture();
		if ( collectionRowKey != null ) {
			// we found a collection element in the result set

			if ( LOG.isDebugEnabled() ) {
				LOG.debugf(
						"Found row of collection: %s",
						MessageHelper.collectionInfoString( persister, collectionRowKey, getFactory() )
				);
			}

			Object owner = optionalOwner;
			if ( owner == null ) {
				owner = persistenceContext.getCollectionOwner( collectionRowKey, persister );
				if ( owner == null ) {
					//TODO: This is assertion is disabled because there is a bug that means the
					//	  original owner of a transient, uninitialized collection is not known
					//	  if the collection is re-referenced by a different object associated
					//	  with the current Session
					//throw new AssertionFailure("bug loading unowned collection");
				}
			}

			PersistentCollection rowCollection = persistenceContext.getLoadContexts()
					.getCollectionLoadContext( rs )
					.getLoadingCollection( persister, collectionRowKey );

			try {
				if ( rowCollection != null ) {
					CompletionStage<?> readFromStage = (CompletionStage<?>) rowCollection.readFrom( rs, persister, descriptor, owner );
					stage = stage.thenCompose( v -> readFromStage.thenAccept( obj -> {} ) );

				}
			} catch (SQLException e) {
				throw new HibernateException( e );
			}

		}
		else if ( optionalKey != null ) {
			// we did not find a collection element in the result set, so we
			// ensure that a collection is created with the owner's identifier,
			// since what we have is an empty collection

			if ( LOG.isDebugEnabled() ) {
				LOG.debugf(
						"Result set contains (possibly empty) collection: %s",
						MessageHelper.collectionInfoString( persister, optionalKey, getFactory() )
				);
			}

			persistenceContext.getLoadContexts()
					.getCollectionLoadContext( rs )
					.getLoadingCollection( persister, optionalKey ); // handle empty collection

		}

		// else no collection element, but also no owner

		return stage;
	}

	/**
	 * Resolve any IDs for currently loaded objects, duplications within the
	 * <tt>ResultSet</tt>, etc. Instantiate empty objects to be initialized from the
	 * <tt>ResultSet</tt>. Return an array of objects (a row of results) and an
	 * array of booleans (by side-effect) that determine whether the corresponding
	 * object should be initialized.
	 */
	private Object[] getRow(
			final ResultSet rs,
			final Loadable[] persisters,
			final EntityKey[] keys,
			final Object optionalObject,
			final EntityKey optionalObjectKey,
			final LockMode[] lockModes,
			final List hydratedObjects,
			final SharedSessionContractImplementor session) throws HibernateException, SQLException {
		final int cols = persisters.length;
		final EntityAliases[] entityAliases = getEntityAliases();

		if ( LOG.isDebugEnabled() ) {
			LOG.debugf( "Result row: %s", StringHelper.toString( keys ) );
		}

		final Object[] rowResults = new Object[cols];

		for ( int i = 0; i < cols; i++ ) {

			Object object = null;
			EntityKey key = keys[i];

			if ( keys[i] == null ) {
				//do nothing
			}
			else {
				//If the object is already loaded, return the loaded one
				object = session.getEntityUsingInterceptor( key );
				if ( object != null ) {
					instanceAlreadyLoaded(
							rs,
							i,
							persisters[i],
							key,
							object,
							lockModes[i],
							hydratedObjects,
							session
					);
				}
				else {
					object = instanceNotYetLoaded(
							rs,
							i,
							persisters[i],
							entityAliases[i].getRowIdAlias(),
							key,
							lockModes[i],
							optionalObjectKey,
							optionalObject,
							hydratedObjects,
							session
					);
				}
			}

			rowResults[i] = object;

		}

		return rowResults;
	}


	/**
	 * The entity instance is already in the session cache
	 */
	protected void instanceAlreadyLoaded(
			final ResultSet rs,
			final int i,
			final Loadable persister,
			final EntityKey key,
			final Object object,
			final LockMode requestedLockMode,
			List hydratedObjects,
			final SharedSessionContractImplementor session)
			throws HibernateException, SQLException {
		if ( !persister.isInstance( object ) ) {
			throw new WrongClassException(
					"loaded object was of wrong class " + object.getClass(),
					key.getIdentifier(),
					persister.getEntityName()
			);
		}

		if ( persister.getBytecodeEnhancementMetadata().isEnhancedForLazyLoading() && enhancementAsProxyEnabled ) {
			if ( "merge".equals( session.getLoadQueryInfluencers().getInternalFetchProfile() ) ) {
				// we are processing a merge and have found an existing "managed copy" in the
				// session - we need to check if this copy is an enhanced-proxy and, if so,
				// perform the hydration just as if it were "not yet loaded"
				final PersistentAttributeInterceptable interceptable = (PersistentAttributeInterceptable) object;
				final PersistentAttributeInterceptor interceptor = interceptable.$$_hibernate_getInterceptor();
				if ( interceptor instanceof EnhancementAsProxyLazinessInterceptor) {
					hydrateEntityState(
							rs,
							i,
							persister,
							getEntityAliases()[i].getRowIdAlias(),
							key,
							hydratedObjects,
							session,
							getInstanceClass(
									rs,
									i,
									persister,
									key.getIdentifier(),
									session
							),
							object,
							requestedLockMode
					);

					// EARLY EXIT!!!
					//		- to skip the version check
					return;
				}
			}
		}

		if ( LockMode.NONE != requestedLockMode && upgradeLocks() ) {
			final EntityEntry entry = session.getPersistenceContextInternal().getEntry( object );
			if ( entry.getLockMode().lessThan( requestedLockMode ) ) {
				//we only check the version when _upgrading_ lock modes
				if ( persister.isVersioned() ) {
					checkVersion( i, persister, key.getIdentifier(), object, rs, session );
				}
				//we need to upgrade the lock mode to the mode requested
				entry.setLockMode( requestedLockMode );
			}
		}
	}


	/**
	 * The entity instance is not in the session cache
	 */
	protected Object instanceNotYetLoaded(
			final ResultSet rs,
			final int i,
			final Loadable persister,
			final String rowIdAlias,
			final EntityKey key,
			final LockMode lockMode,
			final EntityKey optionalObjectKey,
			final Object optionalObject,
			final List hydratedObjects,
			final SharedSessionContractImplementor session)
			throws HibernateException, SQLException {
		final String instanceClass = getInstanceClass(
				rs,
				i,
				persister,
				key.getIdentifier(),
				session
		);

		// see if the entity defines reference caching, and if so use the cached reference (if one).
		if ( session.getCacheMode().isGetEnabled() && persister.canUseReferenceCacheEntries() ) {
			final EntityDataAccess cache = persister.getCacheAccessStrategy();
			if ( cache != null ) {
				final Object ck = cache.generateCacheKey(
						key.getIdentifier(),
						persister,
						session.getFactory(),
						session.getTenantIdentifier()
				);
				final Object cachedEntry = CacheHelper.fromSharedCache( session, ck, cache );
				if ( cachedEntry != null ) {
					CacheEntry entry = (CacheEntry) persister.getCacheEntryStructure().destructure(
							cachedEntry,
							getFactory()
					);
					return ( (ReferenceCacheEntryImpl) entry ).getReference();
				}
			}
		}

		final Object object;
		if ( optionalObjectKey != null && key.equals( optionalObjectKey ) ) {
			//its the given optional object
			object = optionalObject;
		}
		else {
			// instantiate a new instance
			object = session.instantiate( instanceClass, key.getIdentifier() );
		}

		//need to hydrate it.

		// grab its state from the ResultSet and keep it in the Session
		// (but don't yet initialize the object itself)
		// note that we acquire LockMode.READ even if it was not requested
		LockMode acquiredLockMode = lockMode == LockMode.NONE ? LockMode.READ : lockMode;
		hydrateEntityState(
				rs,
				i,
				persister,
				rowIdAlias,
				key,
				hydratedObjects,
				session,
				instanceClass,
				object,
				acquiredLockMode
		);

		return object;
	}

	private void hydrateEntityState(
			ResultSet rs,
			int i,
			Loadable persister,
			String rowIdAlias,
			EntityKey key,
			List hydratedObjects,
			SharedSessionContractImplementor session,
			String instanceClass,
			Object object,
			LockMode acquiredLockMode) throws SQLException {
		loadFromResultSet(
				rs,
				i,
				object,
				instanceClass,
				key,
				rowIdAlias,
				acquiredLockMode,
				persister,
				session
		);

		//materialize associations (and initialize the object) later
		hydratedObjects.add( object );
	}

	private void loadFromResultSet(
			final ResultSet rs,
			final int i,
			final Object object,
			final String instanceEntityName,
			final EntityKey key,
			final String rowIdAlias,
			final LockMode lockMode,
			final Loadable rootPersister,
			final SharedSessionContractImplementor session) throws SQLException, HibernateException {

		final Serializable id = key.getIdentifier();

		// Get the persister for the _subclass_
		final Loadable persister = (Loadable) getFactory().getEntityPersister( instanceEntityName );

		if ( LOG.isTraceEnabled() ) {
			LOG.tracef(
					"Initializing object from ResultSet: %s",
					MessageHelper.infoString(
							persister,
							id,
							getFactory()
					)
			);
		}

		boolean fetchAllPropertiesRequested = isEagerPropertyFetchEnabled( i );

		// add temp entry so that the next step is circular-reference
		// safe - only needed because some types don't take proper
		// advantage of two-phase-load (esp. components)
		TwoPhaseLoad.addUninitializedEntity(
				key,
				object,
				persister,
				lockMode,
				session
		);

		//This is not very nice (and quite slow):
		final String[][] cols = persister == rootPersister ?
				getEntityAliases()[i].getSuffixedPropertyAliases() :
				getEntityAliases()[i].getSuffixedPropertyAliases( persister );

		final Object[] values = persister.hydrate(
				rs,
				id,
				object,
				rootPersister,
				cols,
				fetchAllPropertiesRequested,
				session
		);

		final Object rowId = persister.hasRowId() ? rs.getObject( rowIdAlias ) : null;

		final AssociationType[] ownerAssociationTypes = getOwnerAssociationTypes();
		if ( ownerAssociationTypes != null && ownerAssociationTypes[i] != null ) {
			String ukName = ownerAssociationTypes[i].getRHSUniqueKeyPropertyName();
			if ( ukName != null ) {
				final int index = ( (UniqueKeyLoadable) persister ).getPropertyIndex( ukName );
				final Type type = persister.getPropertyTypes()[index];

				// polymorphism not really handled completely correctly,
				// perhaps...well, actually its ok, assuming that the
				// entity name used in the lookup is the same as the
				// the one used here, which it will be

				EntityUniqueKey euk = new EntityUniqueKey(
						rootPersister.getEntityName(), //polymorphism comment above
						ukName,
						type.semiResolve( values[index], session, object ),
						type,
						persister.getEntityMode(),
						session.getFactory()
				);
				session.getPersistenceContextInternal().addEntity( euk, object );
			}
		}

		TwoPhaseLoad.postHydrate(
				persister,
				id,
				values,
				rowId,
				object,
				lockMode,
				session
		);

	}

	private boolean isEagerPropertyFetchEnabled(int i) {
		boolean[] array = getEntityEagerPropertyFetches();
		return array != null && array[i];
	}

	private void checkVersion(
			final int i,
			final Loadable persister,
			final Serializable id,
			final Object entity,
			final ResultSet rs,
			final SharedSessionContractImplementor session) throws HibernateException, SQLException {

		Object version = session.getPersistenceContextInternal().getEntry( entity ).getVersion();

		if ( version != null ) {
			// null version means the object is in the process of being loaded somewhere else in the ResultSet
			final VersionType versionType = persister.getVersionType();
			final Object currentVersion = versionType.nullSafeGet(
					rs,
					getEntityAliases()[i].getSuffixedVersionAliases(),
					session,
					null
			);
			if ( !versionType.isEqual( version, currentVersion ) ) {
				final StatisticsImplementor statistics = session.getFactory().getStatistics();
				if ( statistics.isStatisticsEnabled() ) {
					statistics.optimisticFailure( persister.getEntityName() );
				}
				throw new StaleObjectStateException( persister.getEntityName(), id );
			}
		}

	}


	/**
	 * Determine the concrete class of an instance in the <tt>ResultSet</tt>
	 */
	private String getInstanceClass(
			final ResultSet rs,
			final int i,
			final Loadable persister,
			final Serializable id,
			final SharedSessionContractImplementor session) throws HibernateException, SQLException {

		if ( persister.hasSubclasses() ) {

			// Code to handle subclasses of topClass
			final Object discriminatorValue = persister.getDiscriminatorType().nullSafeGet(
					rs,
					getEntityAliases()[i].getSuffixedDiscriminatorAlias(),
					session,
					null
			);

			final String result = persister.getSubclassForDiscriminatorValue( discriminatorValue );

			if ( result == null ) {
				//woops we got an instance of another class hierarchy branch
				throw new WrongClassException(
						"Discriminator: " + discriminatorValue,
						id,
						persister.getEntityName()
				);
			}

			return result;

		}
		else {
			return persister.getEntityName();
		}
	}

	protected CompletionStage<List<?>> executeRxQueryStatement(
			String sqlStatement,
			QueryParameters queryParameters,
			boolean scroll,
			List<AfterLoadAction> afterLoadActions,
			SessionImplementor session,
			Function<ResultSet, CompletionStage<List<?>>> transformer) {

		// Processing query filters.
		queryParameters.processFilters( sqlStatement, session );

		// Applying LIMIT clause.
		final LimitHandler limitHandler = getLimitHandler( queryParameters.getRowSelection() );
		String sql = limitHandler.processSql( queryParameters.getFilteredSQL(), queryParameters.getRowSelection() );

		// Adding locks and comments.
		sql = preprocessSQL( sql, queryParameters, getFactory(), afterLoadActions );

		return new RxQueryExecutor().execute( sql, queryParameters, session, transformer );
	}


	@Override
	public CompletionStage<Optional<Object>> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session) {
		// this form is deprecated!
		return load( id, optionalObject, session, LockOptions.NONE, null );
	}

	@Override
	public CompletionStage<Optional<Object>> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session, Boolean readOnly) {
		// this form is deprecated!
		return load( id, optionalObject, session, LockOptions.NONE, readOnly );
	}

	@Override
	public CompletionStage<Optional<Object>> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session, LockOptions lockOptions) {
		return load( id, optionalObject, session, lockOptions, null );
	}

	@Override
	public CompletionStage<Optional<Object>> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session, LockOptions lockOptions, Boolean readOnly) {
		return load( session, id, optionalObject, id, lockOptions, readOnly );
	}
}
