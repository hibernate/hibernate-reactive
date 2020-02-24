package org.hibernate.rx.engine.impl;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.MappingException;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.loading.internal.LoadContexts;
import org.hibernate.engine.spi.*;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.rx.engine.loading.impl.RxLoadContexts;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class RxPersistenceContext implements PersistenceContext {

	private final PersistenceContext delegate;

	public RxPersistenceContext(PersistenceContext context) {
		this.delegate = context;
	}

	@Override
	public LoadContexts getLoadContexts() {
		return new RxLoadContexts( delegate.getLoadContexts() );
	}

	//////////////////
	// DELEGATES
	//////////////////

	@Override
	public boolean isStateless() {
		return delegate.isStateless();
	}

	@Override
	public SharedSessionContractImplementor getSession() {
		return delegate.getSession();
	}

	@Override
	public void addUnownedCollection(CollectionKey key, PersistentCollection collection) {
		delegate.addUnownedCollection(key, collection);
	}

	@Override
	public PersistentCollection useUnownedCollection(CollectionKey key) {
		return delegate.useUnownedCollection(key);
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
	public boolean hasNonReadOnlyEntities() {
		return delegate.hasNonReadOnlyEntities();
	}

	@Override
	public void setEntryStatus(EntityEntry entry, Status status) {
		delegate.setEntryStatus(entry, status);
	}

	@Override
	public void afterTransactionCompletion() {
		delegate.afterTransactionCompletion();
	}

	@Override
	public Object[] getDatabaseSnapshot(Serializable id, EntityPersister persister) {
		return delegate.getDatabaseSnapshot(id, persister);
	}

	@Override
	public Object[] getCachedDatabaseSnapshot(EntityKey key) {
		return delegate.getCachedDatabaseSnapshot(key);
	}

	@Override
	public Object[] getNaturalIdSnapshot(Serializable id, EntityPersister persister) {
		return delegate.getNaturalIdSnapshot(id, persister);
	}

	@Override
	public void addEntity(EntityKey key, Object entity) {
		delegate.addEntity(key, entity);
	}

	@Override
	public Object getEntity(EntityKey key) {
		return delegate.getEntity(key);
	}

	@Override
	public boolean containsEntity(EntityKey key) {
		return delegate.containsEntity(key);
	}

	@Override
	public Object removeEntity(EntityKey key) {
		return delegate.removeEntity(key);
	}

	@Override
	public void addEntity(EntityUniqueKey euk, Object entity) {
		delegate.addEntity(euk, entity);
	}

	@Override
	public Object getEntity(EntityUniqueKey euk) {
		return delegate.getEntity(euk);
	}

	@Override
	public EntityEntry getEntry(Object entity) {
		return delegate.getEntry(entity);
	}

	@Override
	public EntityEntry removeEntry(Object entity) {
		return delegate.removeEntry(entity);
	}

	@Override
	public boolean isEntryFor(Object entity) {
		return delegate.isEntryFor(entity);
	}

	@Override
	public CollectionEntry getCollectionEntry(PersistentCollection coll) {
		return delegate.getCollectionEntry(coll);
	}

	@Override
	public EntityEntry addEntity(Object entity, Status status, Object[] loadedState, EntityKey entityKey, Object version, LockMode lockMode, boolean existsInDatabase, EntityPersister persister, boolean disableVersionIncrement) {
		return delegate.addEntity(entity, status, loadedState, entityKey, version, lockMode, existsInDatabase, persister, disableVersionIncrement);
	}

	@Override
	public EntityEntry addEntry(Object entity, Status status, Object[] loadedState, Object rowId, Serializable id, Object version, LockMode lockMode, boolean existsInDatabase, EntityPersister persister, boolean disableVersionIncrement) {
		return delegate.addEntry(entity, status, loadedState, rowId, id, version, lockMode, existsInDatabase, persister, disableVersionIncrement);
	}

	@Override
	public boolean containsCollection(PersistentCollection collection) {
		return delegate.containsCollection(collection);
	}

	@Override
	public boolean containsProxy(Object proxy) {
		return delegate.containsProxy(proxy);
	}

	@Override
	public boolean reassociateIfUninitializedProxy(Object value) throws MappingException {
		return delegate.reassociateIfUninitializedProxy(value);
	}

	@Override
	public void reassociateProxy(Object value, Serializable id) throws MappingException {
		delegate.reassociateProxy(value, id);
	}

	@Override
	public Object unproxy(Object maybeProxy) throws HibernateException {
		return delegate.unproxy(maybeProxy);
	}

	@Override
	public Object unproxyAndReassociate(Object maybeProxy) throws HibernateException {
		return delegate.unproxyAndReassociate(maybeProxy);
	}

	@Override
	public void checkUniqueness(EntityKey key, Object object) throws HibernateException {
		delegate.checkUniqueness(key, object);
	}

	@Override
	public Object narrowProxy(Object proxy, EntityPersister persister, EntityKey key, Object object) throws HibernateException {
		return delegate.narrowProxy(proxy, persister, key, object);
	}

	@Override
	public Object proxyFor(EntityPersister persister, EntityKey key, Object impl) throws HibernateException {
		return delegate.proxyFor(persister, key, impl);
	}

	@Override
	public Object proxyFor(Object impl) throws HibernateException {
		return delegate.proxyFor(impl);
	}

	@Override
	public void addEnhancedProxy(EntityKey key, PersistentAttributeInterceptable entity) {
		delegate.addEnhancedProxy(key, entity);
	}

	@Override
	public Object getCollectionOwner(Serializable key, CollectionPersister collectionPersister) throws MappingException {
		return delegate.getCollectionOwner(key, collectionPersister);
	}

	@Override
	public Object getLoadedCollectionOwnerOrNull(PersistentCollection collection) {
		return delegate.getLoadedCollectionOwnerOrNull(collection);
	}

	@Override
	public Serializable getLoadedCollectionOwnerIdOrNull(PersistentCollection collection) {
		return delegate.getLoadedCollectionOwnerIdOrNull(collection);
	}

	@Override
	public void addUninitializedCollection(CollectionPersister persister, PersistentCollection collection, Serializable id) {
		delegate.addUninitializedCollection(persister, collection, id);
	}

	@Override
	public void addUninitializedDetachedCollection(CollectionPersister persister, PersistentCollection collection) {
		delegate.addUninitializedDetachedCollection(persister, collection);
	}

	@Override
	public void addNewCollection(CollectionPersister persister, PersistentCollection collection) throws HibernateException {
		delegate.addNewCollection(persister, collection);
	}

	@Override
	public void addInitializedDetachedCollection(CollectionPersister collectionPersister, PersistentCollection collection) throws HibernateException {
		delegate.addInitializedDetachedCollection(collectionPersister, collection);
	}

	@Override
	public CollectionEntry addInitializedCollection(CollectionPersister persister, PersistentCollection collection, Serializable id) throws HibernateException {
		return delegate.addInitializedCollection(persister, collection, id);
	}

	@Override
	public PersistentCollection getCollection(CollectionKey collectionKey) {
		return delegate.getCollection(collectionKey);
	}

	@Override
	public void addNonLazyCollection(PersistentCollection collection) {
		delegate.addNonLazyCollection(collection);
	}

	@Override
	public void initializeNonLazyCollections() throws HibernateException {
		delegate.initializeNonLazyCollections();
	}

	@Override
	public PersistentCollection getCollectionHolder(Object array) {
		return delegate.getCollectionHolder(array);
	}

	@Override
	public void addCollectionHolder(PersistentCollection holder) {
		delegate.addCollectionHolder(holder);
	}

	@Override
	public PersistentCollection removeCollectionHolder(Object array) {
		return delegate.removeCollectionHolder(array);
	}

	@Override
	public Serializable getSnapshot(PersistentCollection coll) {
		return delegate.getSnapshot(coll);
	}

	@Override
	public CollectionEntry getCollectionEntryOrNull(Object collection) {
		return delegate.getCollectionEntryOrNull(collection);
	}

	@Override
	public Object getProxy(EntityKey key) {
		return delegate.getProxy(key);
	}

	@Override
	public void addProxy(EntityKey key, Object proxy) {
		delegate.addProxy(key, proxy);
	}

	@Override
	public Object removeProxy(EntityKey key) {
		return delegate.removeProxy(key);
	}

	@Override
	@Deprecated
	public HashSet getNullifiableEntityKeys() {
		return delegate.getNullifiableEntityKeys();
	}

	@Override
	@Deprecated
	public Map getEntitiesByKey() {
		return delegate.getEntitiesByKey();
	}

	@Override
	public Map.Entry<Object, EntityEntry>[] reentrantSafeEntityEntries() {
		return delegate.reentrantSafeEntityEntries();
	}

	@Override
	@Deprecated
	public Map getEntityEntries() {
		return delegate.getEntityEntries();
	}

	@Override
	public int getNumberOfManagedEntities() {
		return delegate.getNumberOfManagedEntities();
	}

	@Override
	@Deprecated
	public Map getCollectionEntries() {
		return delegate.getCollectionEntries();
	}

	@Override
	public void forEachCollectionEntry(BiConsumer<PersistentCollection, CollectionEntry> action, boolean concurrent) {
		delegate.forEachCollectionEntry(action, concurrent);
	}

	@Override
	@Deprecated
	public Map getCollectionsByKey() {
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
		delegate.setFlushing(flushing);
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
	public Serializable getOwnerId(String entityName, String propertyName, Object childEntity, Map mergeMap) {
		return delegate.getOwnerId(entityName, propertyName, childEntity, mergeMap);
	}

	@Override
	public Object getIndexInOwner(String entity, String property, Object childObject, Map mergeMap) {
		return delegate.getIndexInOwner(entity, property, childObject, mergeMap);
	}

	@Override
	public void addNullProperty(EntityKey ownerKey, String propertyName) {
		delegate.addNullProperty(ownerKey, propertyName);
	}

	@Override
	public boolean isPropertyNull(EntityKey ownerKey, String propertyName) {
		return delegate.isPropertyNull(ownerKey, propertyName);
	}

	@Override
	public boolean isDefaultReadOnly() {
		return delegate.isDefaultReadOnly();
	}

	@Override
	public void setDefaultReadOnly(boolean readOnly) {
		delegate.setDefaultReadOnly(readOnly);
	}

	@Override
	public boolean isReadOnly(Object entityOrProxy) {
		return delegate.isReadOnly(entityOrProxy);
	}

	@Override
	public void setReadOnly(Object entityOrProxy, boolean readOnly) {
		delegate.setReadOnly(entityOrProxy, readOnly);
	}

	@Override
	public void replaceDelayedEntityIdentityInsertKeys(EntityKey oldKey, Serializable generatedId) {
		delegate.replaceDelayedEntityIdentityInsertKeys(oldKey, generatedId);
	}

	@Override
	public void addChildParent(Object child, Object parent) {
		delegate.addChildParent(child, parent);
	}

	@Override
	public void removeChildParent(Object child) {
		delegate.removeChildParent(child);
	}

	@Override
	public void registerInsertedKey(EntityPersister persister, Serializable id) {
		delegate.registerInsertedKey(persister, id);
	}

	@Override
	public boolean wasInsertedDuringTransaction(EntityPersister persister, Serializable id) {
		return delegate.wasInsertedDuringTransaction(persister, id);
	}

	@Override
	public boolean containsNullifiableEntityKey(Supplier<EntityKey> sek) {
		return delegate.containsNullifiableEntityKey(sek);
	}

	@Override
	public void registerNullifiableEntityKey(EntityKey key) {
		delegate.registerNullifiableEntityKey(key);
	}

	@Override
	public boolean isNullifiableEntityKeysEmpty() {
		return delegate.isNullifiableEntityKeysEmpty();
	}

	@Override
	public int getCollectionEntriesSize() {
		return delegate.getCollectionEntriesSize();
	}

	@Override
	public CollectionEntry removeCollectionEntry(PersistentCollection collection) {
		return delegate.removeCollectionEntry(collection);
	}

	@Override
	public void clearCollectionsByKey() {
		delegate.clearCollectionsByKey();
	}

	@Override
	public PersistentCollection addCollectionByKey(CollectionKey collectionKey, PersistentCollection persistentCollection) {
		return delegate.addCollectionByKey(collectionKey, persistentCollection);
	}

	@Override
	public void removeCollectionByKey(CollectionKey collectionKey) {
		delegate.removeCollectionByKey(collectionKey);
	}

	@Override
	public Iterator managedEntitiesIterator() {
		return delegate.managedEntitiesIterator();
	}

	@Override
	public NaturalIdHelper getNaturalIdHelper() {
		return delegate.getNaturalIdHelper();
	}
}
