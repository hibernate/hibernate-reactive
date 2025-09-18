/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hibernate.HibernateException;
import org.hibernate.Internal;
import org.hibernate.LockMode;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.BatchFetchQueue;
import org.hibernate.engine.spi.CollectionEntry;
import org.hibernate.engine.spi.CollectionKey;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityHolder;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.EntityUniqueKey;
import org.hibernate.engine.spi.NaturalIdResolutions;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.PersistentAttributeInterceptable;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.service.spi.EventListenerGroup;
import org.hibernate.event.spi.PostLoadEvent;
import org.hibernate.event.spi.PostLoadEventListener;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.engine.spi.ReactiveSharedSessionContractImplementor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.sql.exec.spi.Callback;
import org.hibernate.sql.results.graph.entity.EntityInitializer;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingState;
import org.hibernate.sql.results.spi.LoadContexts;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * Add reactive methods to a {@link PersistenceContext}.
 */
public class ReactivePersistenceContextAdapter implements PersistenceContext {

	private static final Log LOG = make( Log.class, lookup() );

	private final PersistenceContext delegate;

	/**
	 * Constructs a PersistentContext, bound to the given session.
	 */
	public ReactivePersistenceContextAdapter(PersistenceContext persistenceContext) {
		this.delegate = persistenceContext;
	}

	public CompletionStage<Void> reactiveInitializeNonLazyCollections() throws HibernateException {
		final NonLazyCollectionInitializer initializer = new NonLazyCollectionInitializer();
		delegate.initializeNonLazyCollections( initializer );
		return initializer.stage;
	}

	private class NonLazyCollectionInitializer implements Consumer<PersistentCollection<?>> {
		CompletionStage<Void> stage = voidFuture();

		@Override
		public void accept(PersistentCollection<?> nonLazyCollection) {
			if ( !nonLazyCollection.wasInitialized() ) {
				stage = stage.thenCompose( v ->
						( (ReactiveSharedSessionContractImplementor) getSession() )
								.reactiveInitializeCollection( nonLazyCollection, false ) );
			}
		}
	}

	/**
	 * @deprecated use {@link #reactiveInitializeNonLazyCollections} instead.
	 */
	@Deprecated
	@Override
	public void initializeNonLazyCollections() {
		// still called by ResultSetProcessorImpl, so can't throw UnsupportedOperationException
	}

	@Override
	public void initializeNonLazyCollections(Consumer<PersistentCollection<?>> initializeAction) {
		throw LOG.nonReactiveMethodCall( "reactiveInitializeNonLazyCollection" );
	}

	@Deprecated
	@Override
	public Object[] getDatabaseSnapshot(Object id, EntityPersister persister) throws HibernateException {
		throw LOG.nonReactiveMethodCall( "reactiveGetDatabaseSnapshot" );
	}

	private static final Object[] NO_ROW = new Object[] {PersistenceContext.NO_ROW};

	public CompletionStage<Object[]> reactiveGetDatabaseSnapshot(Object id, EntityPersister persister) throws HibernateException {
		final SessionImplementor session = (SessionImplementor) getSession();
		final EntityKey key = session.generateEntityKey( id, persister );
		final Object[] cached = getEntitySnapshotsByKey() == null
				? null
				: (Object[]) getEntitySnapshotsByKey().get( key );
		if ( cached != null ) {
			return completedFuture( cached == NO_ROW ? null : cached );
		}
		else {
			return ( (ReactiveEntityPersister) persister )
					.reactiveGetDatabaseSnapshot( id, session )
					.thenApply( snapshot -> {
						getOrInitializeEntitySnapshotsByKey().put( key, snapshot );
						return snapshot;
					} );
		}
	}

	@Override
	public boolean isStateless() {
		return delegate.isStateless();
	}

	@Override
	public SharedSessionContractImplementor getSession() {
		return delegate.getSession();
	}

	@Override
	public LoadContexts getLoadContexts() {
		return delegate.getLoadContexts();
	}

	@Override
	public boolean hasLoadContext() {
		return delegate.hasLoadContext();
	}

	@Override
	public PersistentCollection<?> useUnownedCollection(CollectionKey key) {
		return delegate.useUnownedCollection( key );
	}

	@Override
	public BatchFetchQueue getBatchFetchQueue() {
		return delegate.getBatchFetchQueue();
	}

	@Override
	public void clear() {
		delegate.clear();
	}

	@Override
	public void setEntryStatus(EntityEntry entry, Status status) {
		delegate.setEntryStatus( entry, status );
	}

	@Override
	public void afterTransactionCompletion() {
		delegate.afterTransactionCompletion();
	}

	@Override
	public Object[] getCachedDatabaseSnapshot(EntityKey key) {
		return delegate.getCachedDatabaseSnapshot( key );
	}

	@Override
	public Object getNaturalIdSnapshot(Object id, EntityPersister persister) {
		return delegate.getNaturalIdSnapshot( id, persister );
	}

	@Override
	public void addEntity(EntityKey key, Object entity) {
		delegate.addEntity( key, entity );
	}

	@Override
	public Object getEntity(EntityKey key) {
		return delegate.getEntity( key );
	}

	@Override
	public boolean containsEntity(EntityKey key) {
		return delegate.containsEntity( key );
	}

	@Override
	public Object removeEntity(EntityKey key) {
		return delegate.removeEntity( key );
	}

	@Override
	public void addEntity(EntityUniqueKey euk, Object entity) {
		delegate.addEntity( euk, entity );
	}

	@Override
	public Object getEntity(EntityUniqueKey euk) {
		return delegate.getEntity( euk );
	}

	@Override
	public EntityEntry getEntry(Object entity) {
		return delegate.getEntry( entity );
	}

	@Override
	public EntityEntry removeEntry(Object entity) {
		return delegate.removeEntry( entity );
	}

	@Override
	public boolean isEntryFor(Object entity) {
		return delegate.isEntryFor( entity );
	}

	@Override
	public CollectionEntry getCollectionEntry(PersistentCollection<?> coll) {
		return delegate.getCollectionEntry( coll );
	}

	@Override
	public EntityEntry addEntity(
			Object entity,
			Status status,
			Object[] loadedState,
			EntityKey entityKey,
			Object version,
			LockMode lockMode,
			boolean existsInDatabase,
			EntityPersister persister,
			boolean disableVersionIncrement) {
		return delegate.addEntity(
				entity,
				status,
				loadedState,
				entityKey,
				version,
				lockMode,
				existsInDatabase,
				persister,
				disableVersionIncrement
		);
	}

	@Override
	public EntityEntry addEntry(
			Object entity,
			Status status,
			Object[] loadedState,
			Object rowId,
			Object id,
			Object version,
			LockMode lockMode,
			boolean existsInDatabase,
			EntityPersister persister,
			boolean disableVersionIncrement) {
		return delegate.addEntry(
				entity,
				status,
				loadedState,
				rowId,
				id,
				version,
				lockMode,
				existsInDatabase,
				persister,
				disableVersionIncrement
		);
	}

	@Override
	public EntityEntry addReferenceEntry(Object entity, Status status) {
		return delegate.addReferenceEntry( entity, status );
	}

	@Override
	public boolean containsCollection(PersistentCollection<?> collection) {
		return delegate.containsCollection( collection );
	}

	@Override
	public boolean containsProxy(Object proxy) {
		return delegate.containsProxy( proxy );
	}

	@Override
	public boolean reassociateIfUninitializedProxy(Object value) {
		return delegate.reassociateIfUninitializedProxy( value );
	}

	@Override
	public void reassociateProxy(Object value, Object id) {
		delegate.reassociateProxy( value, id );
	}

	@Override
	public Object unproxy(Object maybeProxy) {
		return delegate.unproxy( maybeProxy );
	}

	@Override
	public Object unproxyAndReassociate(Object maybeProxy) {
		return delegate.unproxyAndReassociate( maybeProxy );
	}

	@Override
	public void checkUniqueness(EntityKey key, Object object) {
		delegate.checkUniqueness( key, object );
	}

	@Override
	public Object narrowProxy(Object proxy, EntityPersister persister, EntityKey key, Object object) {
		return delegate.narrowProxy( proxy, persister, key, object );
	}

	@Override
	public Object proxyFor(EntityPersister persister, EntityKey key, Object impl) {
		return delegate.proxyFor( persister, key, impl );
	}

	@Override
	public Object proxyFor(Object impl) {
		return delegate.proxyFor( impl );
	}

	@Override
	public Object proxyFor(EntityHolder holder, EntityPersister persister) {
		return delegate.proxyFor( holder, persister );
	}

	@Override
	public void addEnhancedProxy(EntityKey key, PersistentAttributeInterceptable entity) {
		delegate.addEnhancedProxy( key, entity );
	}

	@Override
	public Object getCollectionOwner(Object key, CollectionPersister collectionPersister) {
		return delegate.getCollectionOwner( key, collectionPersister );
	}

	@Override
	public Object getLoadedCollectionOwnerOrNull(PersistentCollection<?> collection) {
		return delegate.getLoadedCollectionOwnerOrNull( collection );
	}

	@Override
	public Object getLoadedCollectionOwnerIdOrNull(PersistentCollection<?> collection) {
		return delegate.getLoadedCollectionOwnerIdOrNull( collection );
	}

	@Override
	public void addUninitializedCollection(CollectionPersister persister, PersistentCollection<?> collection, Object id) {
		delegate.addUninitializedCollection( persister, collection, id );
	}

	@Override
	public void addUninitializedDetachedCollection(CollectionPersister persister, PersistentCollection<?> collection) {
		delegate.addUninitializedDetachedCollection( persister, collection );
	}

	@Override
	public void addNewCollection(CollectionPersister persister, PersistentCollection<?> collection) {
		delegate.addNewCollection( persister, collection );
	}

	@Override
	public void addInitializedDetachedCollection(CollectionPersister collectionPersister, PersistentCollection<?> collection) {
		delegate.addInitializedDetachedCollection( collectionPersister, collection );
	}

	@Override
	public void replaceCollection(CollectionPersister persister, PersistentCollection<?> oldCollection, PersistentCollection<?> collection) {
		delegate.replaceCollection( persister, oldCollection, collection );
	}

	@Override
	public CollectionEntry addInitializedCollection(CollectionPersister persister, PersistentCollection<?> collection, Object id) {
		return delegate.addInitializedCollection( persister, collection, id );
	}

	@Override
	public PersistentCollection<?> getCollection(CollectionKey collectionKey) {
		return delegate.getCollection( collectionKey );
	}

	@Override
	public void addNonLazyCollection(PersistentCollection<?> collection) {
		delegate.addNonLazyCollection( collection );
	}

	@Override
	public PersistentCollection<?> getCollectionHolder(Object array) {
		return delegate.getCollectionHolder( array );
	}

	@Override
	public void addCollectionHolder(PersistentCollection<?> holder) {
		delegate.addCollectionHolder( holder );
	}

	@Override
	public PersistentCollection<?> removeCollectionHolder(Object array) {
		return delegate.removeCollectionHolder( array );
	}

	@Override
	public Serializable getSnapshot(PersistentCollection<?> coll) {
		return delegate.getSnapshot( coll );
	}

	@Override
	public Object getProxy(EntityKey key) {
		return delegate.getProxy( key );
	}

	@Override
	public void addProxy(EntityKey key, Object proxy) {
		delegate.addProxy( key, proxy );
	}

	@Override
	public Object removeProxy(EntityKey key) {
		return delegate.removeProxy( key );
	}

	@Override
	public EntityHolder claimEntityHolderIfPossible(EntityKey key, Object entity, JdbcValuesSourceProcessingState processingState, EntityInitializer<?> initializer) {
		return delegate.claimEntityHolderIfPossible( key, entity, processingState, initializer );
	}

	@Override
	public EntityHolder addEntityHolder(EntityKey key, Object entity) {
		return delegate.addEntityHolder( key, entity );
	}

	@Override
	public EntityHolder getEntityHolder(EntityKey key) {
		return delegate.getEntityHolder( key );
	}

	@Override
	public boolean containsEntityHolder(EntityKey key) {
		return delegate.containsEntityHolder( key );
	}

	@Override
	public EntityHolder removeEntityHolder(EntityKey key) {
		return delegate.removeEntityHolder( key );
	}

	@Override
	public void postLoad(JdbcValuesSourceProcessingState processingState, Consumer<EntityHolder> loadedConsumer) {
		throw LOG.nonReactiveMethodCall( "reactivePostLoad(JdbcValuesSourceProcessingState, Consumer<EntityHolder>) )" );
	}

	@Internal
	@Override
	public Map<EntityKey, Object> getEntitiesByKey() {
		return delegate.getEntitiesByKey();
	}

	@Internal
	@Override
	public Map<EntityKey, EntityHolder> getEntityHoldersByKey() {
		return delegate.getEntityHoldersByKey();
	}

	@Override
	public Map.Entry<Object, EntityEntry>[] reentrantSafeEntityEntries() {
		return delegate.reentrantSafeEntityEntries();
	}

	@Override
	public int getNumberOfManagedEntities() {
		return delegate.getNumberOfManagedEntities();
	}

	@Internal
	@Override
	public Map<PersistentCollection<?>, CollectionEntry> getCollectionEntries() {
		return delegate.getCollectionEntries();
	}

	@Override
	public void forEachCollectionEntry(BiConsumer<PersistentCollection<?>, CollectionEntry> action, boolean concurrent) {
		delegate.forEachCollectionEntry( action, concurrent );
	}

	@Deprecated
	@Override
	public Map<CollectionKey, PersistentCollection<?>> getCollectionsByKey() {
		return delegate.getCollectionsByKey();
	}

	@Override
	public int getCascadeLevel() {
		return delegate.getCascadeLevel();
	}

	@Override
	public int incrementCascadeLevel() {
		return delegate.incrementCascadeLevel();
	}

	@Override
	public int decrementCascadeLevel() {
		return delegate.decrementCascadeLevel();
	}

	@Override
	public boolean isFlushing() {
		return delegate.isFlushing();
	}

	@Override
	public void setFlushing(boolean flushing) {
		delegate.setFlushing( flushing );
	}

	@Override
	public void beforeLoad() {
		delegate.beforeLoad();
	}

	@Override
	public void afterLoad() {
		delegate.afterLoad();
	}

	@Override
	public boolean isLoadFinished() {
		return delegate.isLoadFinished();
	}

	@Override
	public String toString() {
		return delegate.toString();
	}

	@Override
	public Object getOwnerId(String entityName, String propertyName, Object childEntity, Map mergeMap) {
		return delegate.getOwnerId( entityName, propertyName, childEntity, mergeMap );
	}

	@Override
	public Object getIndexInOwner(String entity, String property, Object childObject, Map mergeMap) {
		return delegate.getIndexInOwner( entity, property, childObject, mergeMap );
	}

	@Override
	public void addNullProperty(EntityKey ownerKey, String propertyName) {
		delegate.addNullProperty( ownerKey, propertyName );
	}

	@Override
	public boolean isPropertyNull(EntityKey ownerKey, String propertyName) {
		return delegate.isPropertyNull( ownerKey, propertyName );
	}

	@Override
	public boolean isDefaultReadOnly() {
		return delegate.isDefaultReadOnly();
	}

	@Override
	public void setDefaultReadOnly(boolean readOnly) {
		delegate.setDefaultReadOnly( readOnly );
	}

	@Override
	public boolean isReadOnly(Object entityOrProxy) {
		return delegate.isReadOnly( entityOrProxy );
	}

	@Override
	public void setReadOnly(Object entityOrProxy, boolean readOnly) {
		delegate.setReadOnly( entityOrProxy, readOnly );
	}

	@Override
	public boolean isRemovingOrphanBeforeUpdates() {
		return delegate.isRemovingOrphanBeforeUpdates();
	}

	@Override
	public void beginRemoveOrphanBeforeUpdates() {
		delegate.beginRemoveOrphanBeforeUpdates();
	}

	@Override
	public void endRemoveOrphanBeforeUpdates() {
		delegate.endRemoveOrphanBeforeUpdates();
	}

	@Override
	public void replaceDelayedEntityIdentityInsertKeys(EntityKey oldKey, Object generatedId) {
		delegate.replaceDelayedEntityIdentityInsertKeys( oldKey, generatedId );
	}

	@Internal
	@Override
	public void replaceEntityEntryRowId(Object entity, Object rowId) {
		delegate.replaceEntityEntryRowId( entity, rowId );
	}

	@Override
	public void addChildParent(Object child, Object parent) {
		delegate.addChildParent( child, parent );
	}

	@Override
	public void removeChildParent(Object child) {
		delegate.removeChildParent( child );
	}

	@Override
	public void registerInsertedKey(EntityPersister persister, Object id) {
		delegate.registerInsertedKey( persister, id );
	}

	@Override
	public boolean wasInsertedDuringTransaction(EntityPersister persister, Object id) {
		return delegate.wasInsertedDuringTransaction( persister, id );
	}

	@Override
	public boolean containsNullifiableEntityKey(Supplier<EntityKey> sek) {
		return delegate.containsNullifiableEntityKey( sek );
	}

	@Override
	public void registerNullifiableEntityKey(EntityKey key) {
		delegate.registerNullifiableEntityKey( key );
	}

	@Override
	public boolean isNullifiableEntityKeysEmpty() {
		return delegate.isNullifiableEntityKeysEmpty();
	}

	@Override
	public boolean containsDeletedUnloadedEntityKey(EntityKey ek) {
		return delegate.containsDeletedUnloadedEntityKey( ek );
	}

	@Override
	public void registerDeletedUnloadedEntityKey(EntityKey key) {
		delegate.registerDeletedUnloadedEntityKey( key );
	}

	@Override
	public void removeDeletedUnloadedEntityKey(EntityKey key) {
		delegate.removeDeletedUnloadedEntityKey( key );
	}

	@Override
	public boolean containsDeletedUnloadedEntityKeys() {
		return delegate.containsDeletedUnloadedEntityKeys();
	}

	@Override
	public int getCollectionEntriesSize() {
		return delegate.getCollectionEntriesSize();
	}

	@Override
	public CollectionEntry removeCollectionEntry(PersistentCollection<?> collection) {
		return delegate.removeCollectionEntry( collection );
	}

	@Override
	public void clearCollectionsByKey() {
		delegate.clearCollectionsByKey();
	}

	@Override
	public PersistentCollection<?> addCollectionByKey(CollectionKey collectionKey, PersistentCollection<?> persistentCollection) {
		return delegate.addCollectionByKey( collectionKey, persistentCollection );
	}

	@Override
	public void removeCollectionByKey(CollectionKey collectionKey) {
		delegate.removeCollectionByKey( collectionKey );
	}

	@Internal
	@Override
	public Map<EntityKey,Object> getEntitySnapshotsByKey() {
		return delegate.getEntitySnapshotsByKey();
	}

	@Override
	@Internal
	public Map<EntityKey,Object> getOrInitializeEntitySnapshotsByKey() {
		return delegate.getOrInitializeEntitySnapshotsByKey();
	}

	@Override
	public Iterator<Object> managedEntitiesIterator() {
		return delegate.managedEntitiesIterator();
	}

	@Override
	public NaturalIdResolutions getNaturalIdResolutions() {
		return delegate.getNaturalIdResolutions();
	}

	/**
	 * Reactive version of {@link StatefulPersistenceContext#postLoad(JdbcValuesSourceProcessingState, Consumer)}
	 */
	public CompletionStage<Void> reactivePostLoad(
			JdbcValuesSourceProcessingState processingState,
			Consumer<EntityHolder> holderConsumer) {
		final ReactiveCallbackImpl callback = (ReactiveCallbackImpl) processingState
				.getExecutionContext().getCallback();
		return processHolders(
				holderConsumer,
				processingState.getLoadingEntityHolders(),
				getSession().getFactory().getEventListenerGroups().eventListenerGroup_POST_LOAD,
				processingState.getPostLoadEvent(),
				callback
		).thenCompose( v -> processHolders(
				holderConsumer,
				processingState.getReloadedEntityHolders(),
				null,
				null,
				callback
		) );
	}

	private CompletionStage<Void> processHolders(
			Consumer<EntityHolder> holderConsumer,
			List<EntityHolder> loadingEntityHolders,
			EventListenerGroup<PostLoadEventListener> listenerGroup,
			PostLoadEvent postLoadEvent,
			ReactiveCallbackImpl callback) {
		if ( loadingEntityHolders != null ) {
			return loop( loadingEntityHolders,
						 holder -> processLoadedEntityHolder(
								 holder,
								 listenerGroup,
								 postLoadEvent,
								 callback,
								 holderConsumer
						 )
			).thenAccept( v -> loadingEntityHolders.clear() );
		}
		return voidFuture();
	}

	/**
	 * Reactive version of {@link StatefulPersistenceContext#processLoadedEntityHolder(EntityHolder, EventListenerGroup, PostLoadEvent, Callback, Consumer)}
	 */
	private CompletionStage<Void> processLoadedEntityHolder(
			EntityHolder holder,
			EventListenerGroup<PostLoadEventListener> listenerGroup,
			PostLoadEvent postLoadEvent,
			ReactiveCallbackImpl callback,
			Consumer<EntityHolder> holderConsumer) {
		if ( holderConsumer != null ) {
			holderConsumer.accept( holder );
		}
		if ( holder.getEntity() == null ) {
			// It's possible that we tried to load an entity and found out it doesn't exist,
			// in which case we added an entry with a null proxy and entity.
			// Remove that empty entry on post load to avoid unwanted side effects
			getEntitiesByKey().remove( holder.getEntityKey() );
		}
		else {
			if ( postLoadEvent != null ) {
				postLoadEvent.reset();
				postLoadEvent.setEntity( holder.getEntity() )
						.setId( holder.getEntityKey().getIdentifier() )
						.setPersister( holder.getDescriptor() );
				listenerGroup.fireEventOnEachListener( postLoadEvent, PostLoadEventListener::onPostLoad	);
				if ( callback != null ) {
					return callback
							.invokeReactiveLoadActions( holder.getEntity(), holder.getDescriptor(), getSession() )
							.thenAccept( v -> holder.resetEntityInitialier() );
				}
			}
		}
		return voidFuture();
	}
}
