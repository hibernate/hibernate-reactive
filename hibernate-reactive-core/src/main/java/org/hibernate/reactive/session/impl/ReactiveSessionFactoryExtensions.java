/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import java.util.Objects;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.AutoFlushEventListener;
import org.hibernate.event.spi.ClearEventListener;
import org.hibernate.event.spi.DeleteEventListener;
import org.hibernate.event.spi.DirtyCheckEventListener;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.EvictEventListener;
import org.hibernate.event.spi.FlushEntityEventListener;
import org.hibernate.event.spi.FlushEventListener;
import org.hibernate.event.spi.InitializeCollectionEventListener;
import org.hibernate.event.spi.LoadEventListener;
import org.hibernate.event.spi.LockEventListener;
import org.hibernate.event.spi.MergeEventListener;
import org.hibernate.event.spi.PersistEventListener;
import org.hibernate.event.spi.PostCollectionRecreateEventListener;
import org.hibernate.event.spi.PostCollectionRemoveEventListener;
import org.hibernate.event.spi.PostCollectionUpdateEventListener;
import org.hibernate.event.spi.PostDeleteEventListener;
import org.hibernate.event.spi.PostInsertEventListener;
import org.hibernate.event.spi.PostLoadEventListener;
import org.hibernate.event.spi.PostUpdateEventListener;
import org.hibernate.event.spi.PreCollectionRecreateEventListener;
import org.hibernate.event.spi.PreCollectionRemoveEventListener;
import org.hibernate.event.spi.PreCollectionUpdateEventListener;
import org.hibernate.event.spi.PreDeleteEventListener;
import org.hibernate.event.spi.PreInsertEventListener;
import org.hibernate.event.spi.PreLoadEventListener;
import org.hibernate.event.spi.PreUpdateEventListener;
import org.hibernate.event.spi.RefreshEventListener;
import org.hibernate.event.spi.ReplicateEventListener;
import org.hibernate.event.spi.ResolveNaturalIdEventListener;
import org.hibernate.event.spi.SaveOrUpdateEventListener;
import org.hibernate.reactive.event.ReactiveEventListenerGroup;
import org.hibernate.reactive.event.impl.ReactiveEventListenerGroupAdaptor;
import org.hibernate.service.spi.ServiceRegistryImplementor;

public final class ReactiveSessionFactoryExtensions {

	public final ReactiveEventListenerGroup<AutoFlushEventListener> eventListenerGroup_AUTO_FLUSH;
	public final ReactiveEventListenerGroup<ClearEventListener> eventListenerGroup_CLEAR;
	public final ReactiveEventListenerGroup<DeleteEventListener> eventListenerGroup_DELETE;
	public final ReactiveEventListenerGroup<DirtyCheckEventListener> eventListenerGroup_DIRTY_CHECK;
	public final ReactiveEventListenerGroup<EvictEventListener> eventListenerGroup_EVICT;
	public final ReactiveEventListenerGroup<FlushEntityEventListener> eventListenerGroup_FLUSH_ENTITY;
	public final ReactiveEventListenerGroup<FlushEventListener> eventListenerGroup_FLUSH;
	public final ReactiveEventListenerGroup<InitializeCollectionEventListener> eventListenerGroup_INIT_COLLECTION;
	public final ReactiveEventListenerGroup<LoadEventListener> eventListenerGroup_LOAD;
	public final ReactiveEventListenerGroup<LockEventListener> eventListenerGroup_LOCK;
	public final ReactiveEventListenerGroup<MergeEventListener> eventListenerGroup_MERGE;
	public final ReactiveEventListenerGroup<PersistEventListener> eventListenerGroup_PERSIST;
	public final ReactiveEventListenerGroup<PersistEventListener> eventListenerGroup_PERSIST_ONFLUSH;
	public final ReactiveEventListenerGroup<PostCollectionRecreateEventListener> eventListenerGroup_POST_COLLECTION_RECREATE;
	public final ReactiveEventListenerGroup<PostCollectionRemoveEventListener> eventListenerGroup_POST_COLLECTION_REMOVE;
	public final ReactiveEventListenerGroup<PostCollectionUpdateEventListener> eventListenerGroup_POST_COLLECTION_UPDATE;
	public final ReactiveEventListenerGroup<PostDeleteEventListener> eventListenerGroup_POST_COMMIT_DELETE;
	public final ReactiveEventListenerGroup<PostDeleteEventListener> eventListenerGroup_POST_DELETE;
	public final ReactiveEventListenerGroup<PostInsertEventListener> eventListenerGroup_POST_COMMIT_INSERT;
	public final ReactiveEventListenerGroup<PostInsertEventListener> eventListenerGroup_POST_INSERT;
	public final ReactiveEventListenerGroup<PostLoadEventListener> eventListenerGroup_POST_LOAD; //Frequently used by 2LC initialization:
	public final ReactiveEventListenerGroup<PostUpdateEventListener> eventListenerGroup_POST_COMMIT_UPDATE;
	public final ReactiveEventListenerGroup<PostUpdateEventListener> eventListenerGroup_POST_UPDATE;
	public final ReactiveEventListenerGroup<PreCollectionRecreateEventListener> eventListenerGroup_PRE_COLLECTION_RECREATE;
	public final ReactiveEventListenerGroup<PreCollectionRemoveEventListener> eventListenerGroup_PRE_COLLECTION_REMOVE;
	public final ReactiveEventListenerGroup<PreCollectionUpdateEventListener> eventListenerGroup_PRE_COLLECTION_UPDATE;
	public final ReactiveEventListenerGroup<PreDeleteEventListener> eventListenerGroup_PRE_DELETE;
	public final ReactiveEventListenerGroup<PreInsertEventListener> eventListenerGroup_PRE_INSERT;
	public final ReactiveEventListenerGroup<PreLoadEventListener> eventListenerGroup_PRE_LOAD;
	public final ReactiveEventListenerGroup<PreUpdateEventListener> eventListenerGroup_PRE_UPDATE;
	public final ReactiveEventListenerGroup<RefreshEventListener> eventListenerGroup_REFRESH;
	public final ReactiveEventListenerGroup<ReplicateEventListener> eventListenerGroup_REPLICATE;
	public final ReactiveEventListenerGroup<ResolveNaturalIdEventListener> eventListenerGroup_RESOLVE_NATURAL_ID;
	public final ReactiveEventListenerGroup<SaveOrUpdateEventListener> eventListenerGroup_SAVE;
	public final ReactiveEventListenerGroup<SaveOrUpdateEventListener> eventListenerGroup_SAVE_UPDATE;
	public final ReactiveEventListenerGroup<SaveOrUpdateEventListener> eventListenerGroup_UPDATE;

	public ReactiveSessionFactoryExtensions(SessionFactoryImplementor sessionFactory) {
		Objects.requireNonNull( sessionFactory );
		final ServiceRegistryImplementor serviceRegistry = sessionFactory.getServiceRegistry();
		// Pre-compute all iterators on Event listeners:
		final EventListenerRegistry eventListenerRegistry = serviceRegistry.getService( EventListenerRegistry.class );
		this.eventListenerGroup_AUTO_FLUSH = listeners( eventListenerRegistry, EventType.AUTO_FLUSH );
		this.eventListenerGroup_CLEAR = listeners( eventListenerRegistry, EventType.CLEAR );
		this.eventListenerGroup_DELETE = listeners( eventListenerRegistry, EventType.DELETE );
		this.eventListenerGroup_DIRTY_CHECK = listeners( eventListenerRegistry, EventType.DIRTY_CHECK );
		this.eventListenerGroup_EVICT = listeners( eventListenerRegistry, EventType.EVICT );
		this.eventListenerGroup_FLUSH = listeners( eventListenerRegistry, EventType.FLUSH );
		this.eventListenerGroup_FLUSH_ENTITY = listeners( eventListenerRegistry, EventType.FLUSH_ENTITY );
		this.eventListenerGroup_INIT_COLLECTION = listeners( eventListenerRegistry, EventType.INIT_COLLECTION );
		this.eventListenerGroup_LOAD = listeners( eventListenerRegistry, EventType.LOAD );
		this.eventListenerGroup_LOCK = listeners( eventListenerRegistry, EventType.LOCK );
		this.eventListenerGroup_MERGE = listeners( eventListenerRegistry, EventType.MERGE );
		this.eventListenerGroup_PERSIST = listeners( eventListenerRegistry, EventType.PERSIST );
		this.eventListenerGroup_PERSIST_ONFLUSH = listeners( eventListenerRegistry, EventType.PERSIST_ONFLUSH );
		this.eventListenerGroup_POST_COLLECTION_RECREATE = listeners( eventListenerRegistry, EventType.POST_COLLECTION_RECREATE );
		this.eventListenerGroup_POST_COLLECTION_REMOVE = listeners( eventListenerRegistry, EventType.POST_COLLECTION_REMOVE );
		this.eventListenerGroup_POST_COLLECTION_UPDATE = listeners( eventListenerRegistry, EventType.POST_COLLECTION_UPDATE );
		this.eventListenerGroup_POST_COMMIT_DELETE = listeners( eventListenerRegistry, EventType.POST_COMMIT_DELETE );
		this.eventListenerGroup_POST_COMMIT_INSERT = listeners( eventListenerRegistry, EventType.POST_COMMIT_INSERT );
		this.eventListenerGroup_POST_COMMIT_UPDATE = listeners( eventListenerRegistry, EventType.POST_COMMIT_UPDATE );
		this.eventListenerGroup_POST_DELETE = listeners( eventListenerRegistry, EventType.POST_DELETE );
		this.eventListenerGroup_POST_INSERT = listeners( eventListenerRegistry, EventType.POST_INSERT );
		this.eventListenerGroup_POST_LOAD = listeners( eventListenerRegistry, EventType.POST_LOAD );
		this.eventListenerGroup_POST_UPDATE = listeners( eventListenerRegistry, EventType.POST_UPDATE );
		this.eventListenerGroup_PRE_COLLECTION_RECREATE = listeners( eventListenerRegistry, EventType.PRE_COLLECTION_RECREATE );
		this.eventListenerGroup_PRE_COLLECTION_REMOVE = listeners( eventListenerRegistry, EventType.PRE_COLLECTION_REMOVE );
		this.eventListenerGroup_PRE_COLLECTION_UPDATE = listeners( eventListenerRegistry, EventType.PRE_COLLECTION_UPDATE );
		this.eventListenerGroup_PRE_DELETE = listeners( eventListenerRegistry, EventType.PRE_DELETE );
		this.eventListenerGroup_PRE_INSERT = listeners( eventListenerRegistry, EventType.PRE_INSERT );
		this.eventListenerGroup_PRE_LOAD = listeners( eventListenerRegistry, EventType.PRE_LOAD );
		this.eventListenerGroup_PRE_UPDATE = listeners( eventListenerRegistry, EventType.PRE_UPDATE );
		this.eventListenerGroup_REFRESH = listeners( eventListenerRegistry, EventType.REFRESH );
		this.eventListenerGroup_REPLICATE = listeners( eventListenerRegistry, EventType.REPLICATE );
		this.eventListenerGroup_RESOLVE_NATURAL_ID = listeners( eventListenerRegistry, EventType.RESOLVE_NATURAL_ID );
		this.eventListenerGroup_SAVE = listeners( eventListenerRegistry, EventType.SAVE );
		this.eventListenerGroup_SAVE_UPDATE = listeners( eventListenerRegistry, EventType.SAVE_UPDATE );
		this.eventListenerGroup_UPDATE = listeners( eventListenerRegistry, EventType.UPDATE );
	}

	private static <T> ReactiveEventListenerGroup<T> listeners(EventListenerRegistry elr, EventType<T> type) {
		return new ReactiveEventListenerGroupAdaptor( elr.getEventListenerGroup( type ) );
	}

}
