/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.Internal;
import org.hibernate.LockOptions;
import org.hibernate.TransientObjectException;
import org.hibernate.TransientPropertyValueException;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.event.spi.DeleteContext;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.MergeContext;
import org.hibernate.event.spi.PersistContext;
import org.hibernate.event.spi.RefreshContext;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.type.CollectionType;
import org.hibernate.type.EntityType;
import org.hibernate.type.Type;

import static org.hibernate.engine.internal.ManagedTypeHelper.isHibernateProxy;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.falseFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * The possible {@link CascadingAction cascading actions} for
 * a {@link Stage.Session reactive session}.
 */
public class CascadingActions {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	/**
	 * Disallow instantiation
	 */
	private CascadingActions() {
	}

	/**
	 * @see org.hibernate.Session#remove(Object)
	 */
	public static final CascadingAction<DeleteContext> REMOVE = new BaseCascadingAction<>( org.hibernate.engine.spi.CascadingActions.REMOVE ) {
		@Override
		public CompletionStage<Void> cascade(
				EventSource session,
				Object child,
				String entityName,
				DeleteContext context,
				boolean isCascadeDeleteEnabled) {
			LOG.tracev( "Cascading to delete: {0}", entityName );
			final ReactiveSession reactiveSession = session.unwrap( ReactiveSession.class );
			//TODO: force-fetching it here circumvents the unloaded-delete optimization
			//      so we don't actually want to do this
			return reactiveSession.reactiveFetch( child, true )
					.thenCompose( c -> reactiveSession.reactiveRemove( entityName, c, isCascadeDeleteEnabled, context ) );
		}
	};

	/**
	 * @see org.hibernate.Session#persist(Object)
	 */
	public static final CascadingAction<PersistContext> PERSIST = new BaseCascadingAction<>( org.hibernate.engine.spi.CascadingActions.PERSIST ) {
		@Override
		public CompletionStage<Void> cascade(
				EventSource session,
				Object child,
				String entityName,
				PersistContext context,
				boolean isCascadeDeleteEnabled)
				throws HibernateException {
			LOG.tracev( "Cascading to persist: {0}", entityName );
			return session.unwrap( ReactiveSession.class ).reactivePersist( child, context );
		}
	};

	/**
	 * Execute persist during flush time
	 *
	 * @see org.hibernate.Session#persist(Object)
	 */
	public static final CascadingAction<PersistContext> PERSIST_ON_FLUSH = new BaseCascadingAction<>( org.hibernate.engine.spi.CascadingActions.PERSIST_ON_FLUSH ) {
		@Override
		public CompletionStage<Void> cascade(
				EventSource session,
				Object child,
				String entityName,
				PersistContext context,
				boolean isCascadeDeleteEnabled)
				throws HibernateException {
			LOG.tracev( "Cascading to persist on flush: {0}", entityName );
			return session.unwrap( ReactiveSession.class ).reactivePersistOnFlush( child, context );
		}

		private boolean isInManagedState(Object child, EventSource session) {
			EntityEntry entry = session.getPersistenceContextInternal().getEntry( child );
			if ( entry == null ) {
				return false;
			}
			else {
				switch ( entry.getStatus() ) {
					case MANAGED:
					case READ_ONLY:
					case SAVING:
						return true;
					default:
						return false;
				}
			}
		}

		@Override
		public CompletionStage<Void> noCascade(
				EventSource session,
				Object parent,
				EntityPersister persister,
				Type propertyType,
				int propertyIndex) {
			if ( propertyType.isEntityType() ) {
				final Object child = persister.getValue( parent, propertyIndex );
				if ( child != null
						&& !isInManagedState( child, session )
						&& !isHibernateProxy( child ) ) { //a proxy cannot be transient and it breaks ForeignKeys.isTransient
					final String childEntityName =
							( (EntityType) propertyType ).getAssociatedEntityName( session.getFactory() );
					return ForeignKeys.isTransient( childEntityName, child, null, session )
							.thenAccept( isTrans -> {
								if ( isTrans ) {
									String parentEntityName = persister.getEntityName();
									String propertyName = persister.getPropertyNames()[propertyIndex];
									throw new TransientPropertyValueException(
											"object references an unsaved transient instance - save the transient instance before flushing",
											childEntityName,
											parentEntityName,
											propertyName
									);
								}
							} );
				}
			}
			return voidFuture();
		}
	};

	@Internal
	public static final CascadingAction<Void> CHECK_ON_FLUSH = new BaseCascadingAction<>( org.hibernate.engine.spi.CascadingActions.CHECK_ON_FLUSH ) {
		@Override
		public CompletionStage<Void> cascade(
				EventSource session,
				Object child,
				String entityName,
				Void context,
				boolean isCascadeDeleteEnabled) throws HibernateException {
			if ( child != null ) {
				return isChildTransient( session, child, entityName ).thenCompose( isTransient -> {
					if ( isTransient ) {
						return failedFuture( new TransientObjectException(
								"persistent instance references an unsaved transient instance of '" + entityName + "' (save the transient instance before flushing)" ) );
					}
					return voidFuture();
				} );
			}
			return voidFuture();
		}
	};

	private static CompletionStage<Boolean> isChildTransient(EventSource session, Object child, String entityName) {
		if ( isHibernateProxy( child ) ) {
			// a proxy is always non-transient
			// and ForeignKeys.isTransient()
			// is not written to expect a proxy
			// TODO: but the proxied entity might have been deleted!
			return falseFuture();
		}
		else {
			final EntityEntry entry = session.getPersistenceContextInternal().getEntry( child );
			if ( entry != null ) {
				// if it's associated with the session
				// we are good, even if it's not yet
				// inserted, since ordering problems
				// are detected and handled elsewhere
				boolean deleted = entry.getStatus().isDeletedOrGone();
				return completedFuture( deleted );
			}
			else {
				// TODO: check if it is a merged entity which has not yet been flushed
				// Currently this throws if you directly reference a new transient
				// instance after a call to merge() that results in its managed copy
				// being scheduled for insertion, if the insert has not yet occurred.
				// This is not terrible: it's more correct to "swap" the reference to
				// point to the managed instance, but it's probably too heavy-handed.
				return ForeignKeys.isTransient( entityName, child, null, session );
			}
		}
	}

	/**
	 * @see org.hibernate.Session#merge(Object)
	 */
	public static final CascadingAction<MergeContext> MERGE =
			new BaseCascadingAction<>( org.hibernate.engine.spi.CascadingActions.MERGE ) {
				@Override
				public CompletionStage<Void> cascade(
						EventSource session,
						Object child,
						String entityName,
						MergeContext context,
						boolean isCascadeDeleteEnabled)
						throws HibernateException {
					LOG.tracev( "Cascading to refresh: {0}", entityName );
					return session.unwrap( ReactiveSession.class ).reactiveMerge( child, context );
				}
			};


	/**
	 * @see org.hibernate.Session#refresh(Object)
	 */
	public static final CascadingAction<RefreshContext> REFRESH = new BaseCascadingAction<>( org.hibernate.engine.spi.CascadingActions.REFRESH ) {
		@Override
		public CompletionStage<Void> cascade(
				EventSource session,
				Object child,
				String entityName,
				RefreshContext context,
				boolean isCascadeDeleteEnabled)
				throws HibernateException {
			LOG.tracev( "Cascading to refresh: {0}", entityName );
			return session.unwrap( ReactiveSession.class ).reactiveRefresh( child, context );
		}
	};

	/**
	 * @see org.hibernate.Session#lock(Object, org.hibernate.LockMode)
	 */
	public static final CascadingAction<LockOptions> LOCK = new BaseCascadingAction<>( org.hibernate.engine.spi.CascadingActions.LOCK ) {
		@Override
		public CompletionStage<Void> cascade(
				EventSource session,
				Object child,
				String entityName,
				LockOptions context,
				boolean isCascadeDeleteEnabled)
				throws HibernateException {
			LOG.tracev( "Cascading to lock: {0}", entityName );
			return session.unwrap( ReactiveSession.class ).reactiveLock( child, context );
		}
	};

	public abstract static class BaseCascadingAction<C> implements CascadingAction<C> {
		private final org.hibernate.engine.spi.CascadingAction<C> delegate;

		BaseCascadingAction(org.hibernate.engine.spi.CascadingAction<C> delegate) {
			this.delegate = delegate;
		}

		@Override
		public Iterator<?> getCascadableChildrenIterator(EventSource session, CollectionType collectionType, Object collection) {
			return delegate.getCascadableChildrenIterator( session, collectionType, collection );
		}

		@Override
		public boolean deleteOrphans() {
			return delegate.deleteOrphans();
		}

		@Override
		public org.hibernate.engine.spi.CascadingAction<C> delegate() {
			return delegate;
		}

		/**
		 * @see BaseCascadingAction#noCascade(EventSource, Object, EntityPersister, Type, int)
		 */
		@Override
		public CompletionStage<Void> noCascade(EventSource session, Object parent, EntityPersister persister, Type propertyType, int propertyIndex) {
			return voidFuture();
		}

		@Override
		public boolean performOnLazyProperty() {
			return delegate.performOnLazyProperty();
		}

		@Override
		public String toString() {
			return delegate.toString();
		}
	}

}
