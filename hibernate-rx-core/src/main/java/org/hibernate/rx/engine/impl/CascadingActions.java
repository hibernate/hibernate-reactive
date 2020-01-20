/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.rx.engine.impl;

import org.hibernate.HibernateException;
import org.hibernate.event.spi.EventSource;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.rx.RxSessionInternal;
import org.hibernate.type.CollectionType;
import org.hibernate.type.Type;
import org.jboss.logging.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * @author Steve Ebersole
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
	public static final CascadingAction DELETE =
			new BaseCascadingAction(org.hibernate.engine.spi.CascadingActions.DELETE) {
		@Override
		public CompletionStage <?> cascade(
				EventSource session,
				Object child,
				String entityName,
				Object anything,
				boolean isCascadeDeleteEnabled) {
			LOG.tracev( "Cascading to delete: {0}", entityName );
			return session.unwrap(RxSessionInternal.class).rxFetch(child)
					.thenCompose( c -> session.unwrap(RxSessionInternal.class)
							.rxRemove( c.get(), isCascadeDeleteEnabled, (Set) anything ) );
		}
	};

	/**
	 * @see org.hibernate.Session#persist(Object)
	 */
	public static final CascadingAction PERSIST =
			new BaseCascadingAction(org.hibernate.engine.spi.CascadingActions.PERSIST) {
		@Override
		public CompletionStage <?> cascade(
				EventSource session,
				Object child,
				String entityName,
				Object anything,
				boolean isCascadeDeleteEnabled)
				throws HibernateException {
			LOG.tracev( "Cascading to persist: {0}", entityName );
			return session.unwrap(RxSessionInternal.class).rxPersist( child, (Map) anything );
		}
	};

	/**
	 * Execute persist during flush time
	 *
	 * @see org.hibernate.Session#persist(Object)
	 */
	public static final CascadingAction PERSIST_ON_FLUSH =
			new BaseCascadingAction(org.hibernate.engine.spi.CascadingActions.PERSIST_ON_FLUSH) {
		@Override
		public CompletionStage <?> cascade(
				EventSource session,
				Object child,
				String entityName,
				Object anything,
				boolean isCascadeDeleteEnabled)
				throws HibernateException {
			LOG.tracev( "Cascading to persist on flush: {0}", entityName );
			return session.unwrap(RxSessionInternal.class).rxPersistOnFlush( child, (Map) anything );
		}
	};

	/**
	 * @see org.hibernate.Session#merge(Object)
	 */
	public static final CascadingAction MERGE =
			new BaseCascadingAction(org.hibernate.engine.spi.CascadingActions.MERGE) {
				@Override
				public CompletionStage <?> cascade(
						EventSource session,
						Object child,
						String entityName,
						Object anything,
						boolean isCascadeDeleteEnabled)
						throws HibernateException {
					LOG.tracev("Cascading to refresh: {0}", entityName);
					return session.unwrap(RxSessionInternal.class).rxMerge( child, (Map) anything );
				}
			};


	/**
	 * @see org.hibernate.Session#refresh(Object)
	 */
	public static final CascadingAction REFRESH =
			new BaseCascadingAction(org.hibernate.engine.spi.CascadingActions.REFRESH) {
		@Override
		public CompletionStage <?> cascade(
				EventSource session,
				Object child,
				String entityName,
				Object anything,
				boolean isCascadeDeleteEnabled)
				throws HibernateException {
			LOG.tracev("Cascading to refresh: {0}", entityName);
			return session.unwrap(RxSessionInternal.class).rxRefresh( child, (Map) anything );
		}
	};

	public abstract static class BaseCascadingAction implements CascadingAction {
		private final org.hibernate.engine.spi.CascadingAction delegate;

		BaseCascadingAction(org.hibernate.engine.spi.CascadingAction delegate) {
			this.delegate = delegate;
		}

		@Override
		public Iterator<Object> getCascadableChildrenIterator(EventSource session, CollectionType collectionType, Object collection) {
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

		@Override
		public void noCascade(EventSource session, Object parent, EntityPersister persister, Type propertyType, int propertyIndex) {
			delegate.noCascade( session, parent, persister, propertyType, propertyIndex );
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
