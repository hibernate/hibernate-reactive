/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import org.hibernate.HibernateException;
import org.hibernate.LockOptions;
import org.hibernate.TransientPropertyValueException;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.internal.MergeContext;
import org.hibernate.event.spi.EventSource;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.internal.util.collections.IdentitySet;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.type.CollectionType;
import org.hibernate.type.EntityType;
import org.hibernate.type.Type;
import org.jboss.logging.Logger;

import java.util.Iterator;
import java.util.concurrent.CompletionStage;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * The possible {@link CascadingAction cascading actions} for
 * a {@link Stage.Session reactive session}.
 */
public class CascadingActions {
	private static final CoreMessageLogger LOG = Logger.getMessageLogger(
			CoreMessageLogger.class,
			CascadingAction.class.getName()
	);

	/**
	 * Disallow instantiation
	 */
	private CascadingActions() {
	}

	/**
	 * @see org.hibernate.Session#delete(Object)
	 */
	public static final CascadingAction<IdentitySet> DELETE =
			new BaseCascadingAction<IdentitySet>(org.hibernate.engine.spi.CascadingActions.DELETE) {
		@Override
		public CompletionStage<Void> cascade(
				EventSource session,
				Object child,
				String entityName,
				IdentitySet context,
				boolean isCascadeDeleteEnabled) {
			LOG.tracev( "Cascading to delete: {0}", entityName );
			return session.unwrap(ReactiveSession.class).reactiveFetch( child, true )
					.thenCompose( c -> session.unwrap(ReactiveSession.class)
							.reactiveRemove( c, isCascadeDeleteEnabled, context ) );
		}
	};

	/**
	 * @see org.hibernate.Session#persist(Object)
	 */
	public static final CascadingAction<IdentitySet> PERSIST =
			new BaseCascadingAction<IdentitySet>(org.hibernate.engine.spi.CascadingActions.PERSIST) {
		@Override
		public CompletionStage<Void> cascade(
				EventSource session,
				Object child,
				String entityName,
				IdentitySet context,
				boolean isCascadeDeleteEnabled)
				throws HibernateException {
			LOG.tracev( "Cascading to persist: {0}", entityName );
			return session.unwrap(ReactiveSession.class).reactivePersist( child, context );
		}
	};

	/**
	 * Execute persist during flush time
	 *
	 * @see org.hibernate.Session#persist(Object)
	 */
	public static final CascadingAction<IdentitySet> PERSIST_ON_FLUSH =
			new BaseCascadingAction<IdentitySet>(org.hibernate.engine.spi.CascadingActions.PERSIST_ON_FLUSH) {
		@Override
		public CompletionStage<Void> cascade(
				EventSource session,
				Object child,
				String entityName,
				IdentitySet context,
				boolean isCascadeDeleteEnabled)
				throws HibernateException {
			LOG.tracev( "Cascading to persist on flush: {0}", entityName );
			return session.unwrap(ReactiveSession.class).reactivePersistOnFlush( child, context );
		}

		private boolean isInManagedState(Object child, EventSource session) {
			EntityEntry entry = session.getPersistenceContextInternal().getEntry( child );
			return entry != null &&
					( entry.getStatus() == Status.MANAGED
							|| entry.getStatus() == Status.READ_ONLY
							|| entry.getStatus() == Status.SAVING );
		}

		@Override
		public CompletionStage<Void> noCascade(EventSource session, Object parent, EntityPersister persister, Type propertyType, int propertyIndex) {
			if ( propertyType.isEntityType() ) {
				Object child = persister.getPropertyValue( parent, propertyIndex );
				if ( child != null
						&& !isInManagedState( child, session )
						&& !(child instanceof HibernateProxy ) ) { //a proxy cannot be transient and it breaks ForeignKeys.isTransient
					final String childEntityName = ((EntityType) propertyType).getAssociatedEntityName( session.getFactory() );
					return ForeignKeys.isTransient( childEntityName, child, null, session)
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

	/**
	 * @see org.hibernate.Session#merge(Object)
	 */
	public static final CascadingAction<MergeContext> MERGE =
			new BaseCascadingAction<MergeContext>(org.hibernate.engine.spi.CascadingActions.MERGE) {
				@Override
				public CompletionStage<Void> cascade(
						EventSource session,
						Object child,
						String entityName,
						MergeContext context,
						boolean isCascadeDeleteEnabled)
						throws HibernateException {
					LOG.tracev("Cascading to refresh: {0}", entityName);
					return session.unwrap(ReactiveSession.class).reactiveMerge( child, context );
				}
			};


	/**
	 * @see org.hibernate.Session#refresh(Object)
	 */
	public static final CascadingAction<IdentitySet> REFRESH =
			new BaseCascadingAction<IdentitySet>(org.hibernate.engine.spi.CascadingActions.REFRESH) {
		@Override
		public CompletionStage<Void> cascade(
				EventSource session,
				Object child,
				String entityName,
				IdentitySet context,
				boolean isCascadeDeleteEnabled)
				throws HibernateException {
			LOG.tracev("Cascading to refresh: {0}", entityName);
			return session.unwrap(ReactiveSession.class).reactiveRefresh( child, context );
		}
	};

	/**
	 * @see org.hibernate.Session#lock(Object, org.hibernate.LockMode)
	 */
	public static final CascadingAction<LockOptions> LOCK =
			new BaseCascadingAction<LockOptions>(org.hibernate.engine.spi.CascadingActions.LOCK) {
				@Override
				public CompletionStage<Void> cascade(
						EventSource session,
						Object child,
						String entityName,
						LockOptions context,
						boolean isCascadeDeleteEnabled)
						throws HibernateException {
					LOG.tracev("Cascading to lock: {0}", entityName);
					return session.unwrap(ReactiveSession.class).reactiveLock(child, context);
				}
			};

	public abstract static class BaseCascadingAction<C> implements CascadingAction<C> {
		private final org.hibernate.engine.spi.CascadingAction delegate;

		BaseCascadingAction(org.hibernate.engine.spi.CascadingAction delegate) {
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
		public org.hibernate.engine.spi.CascadingAction delegate() {
			return delegate;
		}

		@Override
		public boolean requiresNoCascadeChecking() {
			return delegate.requiresNoCascadeChecking();
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
