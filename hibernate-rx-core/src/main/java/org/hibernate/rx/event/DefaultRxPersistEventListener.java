/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.rx.event;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.ObjectDeletedException;
import org.hibernate.PersistentObjectException;
import org.hibernate.engine.spi.CascadingAction;
import org.hibernate.engine.spi.CascadingActions;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.service.spi.DuplicationStrategy;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.PersistEvent;
import org.hibernate.event.spi.PersistEventListener;
import org.hibernate.id.ForeignGenerator;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.jpa.event.spi.CallbackRegistryConsumer;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.rx.event.internal.AbstractRxSaveEventListener;
import org.hibernate.rx.event.spi.RxPersistEventListener;
import org.hibernate.rx.util.RxUtil;

/**
 * Defines the default create event listener used by hibernate for creating
 * transient entities in response to generated create events.
 *
 * @author Gavin King
 */
public class DefaultRxPersistEventListener
		extends AbstractRxSaveEventListener
		implements PersistEventListener, RxPersistEventListener, CallbackRegistryConsumer {
	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( DefaultRxPersistEventListener.class );

	@Override
	protected CascadingAction getCascadeAction() {
		return CascadingActions.PERSIST;
	}

	@Override
	protected Boolean getAssumedUnsaved() {
		return Boolean.TRUE;
	}

	/**
	 * Handle the given create event.
	 *
	 * @param event The create event to be handled.
	 */
	public CompletionStage<Void> rxOnPersist(PersistEvent event) throws HibernateException {
		return rxOnPersist( event, new IdentityHashMap( 10 ) );
	}

	/**
	 * Handle the given create event.
	 *
	 * @param event The create event to be handled.
	 */
	public CompletionStage<Void> rxOnPersist(PersistEvent event, Map createCache) throws HibernateException {
		final SessionImplementor source = event.getSession();
		final Object object = event.getObject();
		final Object entity;
		if ( object instanceof HibernateProxy ) {
			LazyInitializer li = ( (HibernateProxy) object ).getHibernateLazyInitializer();
			if ( li.isUninitialized() ) {
				if ( li.getSession() == source ) {
					return RxUtil.nullFuture(); //NOTE EARLY EXIT!
				}
				else {
					return RxUtil.failedFuture( new PersistentObjectException( "uninitialized proxy passed to persist()" ) );
				}
			}
			entity = li.getImplementation();
		}
		else {
			entity = object;
		}

		final String entityName;
		if ( event.getEntityName() != null ) {
			entityName = event.getEntityName();
		}
		else {
			entityName = source.bestGuessEntityName( entity );
			event.setEntityName( entityName );
		}

		final EntityEntry entityEntry = source.getPersistenceContextInternal().getEntry( entity );
		EntityState entityState = getEntityState( entity, entityName, entityEntry, source );
		if ( entityState == EntityState.DETACHED ) {
			// JPA 2, in its version of a "foreign generated", allows the id attribute value
			// to be manually set by the user, even though this manual value is irrelevant.
			// The issue is that this causes problems with the Hibernate unsaved-value strategy
			// which comes into play here in determining detached/transient state.
			//
			// Detect if we have this situation and if so null out the id value and calculate the
			// entity state again.

			// NOTE: entityEntry must be null to get here, so we cannot use any of its values
			EntityPersister persister = source.getFactory().getEntityPersister( entityName );
			if ( ForeignGenerator.class.isInstance( persister.getIdentifierGenerator() ) ) {
				if ( LOG.isDebugEnabled() && persister.getIdentifier( entity, source ) != null ) {
					LOG.debug( "Resetting entity id attribute to null for foreign generator" );
				}
				persister.setIdentifier( entity, null, source );
				entityState = getEntityState( entity, entityName, entityEntry, source );
			}
		}

		switch ( entityState ) {
			case DETACHED: {
				return RxUtil.failedFuture( new PersistentObjectException(
						"detached entity passed to persist: " +
								getLoggableName( event.getEntityName(), entity )
				) );
			}
			case PERSISTENT: {
				entityIsPersistent( event, createCache );
				return RxUtil.nullFuture();
			}
			case TRANSIENT: {
				return entityIsTransient( event, createCache );
			}
			case DELETED: {
				entityEntry.setStatus( Status.MANAGED );
				entityEntry.setDeletedState( null );
				event.getSession().getActionQueue().unScheduleDeletion( entityEntry, event.getObject() );
				entityIsDeleted( event, createCache );
				return RxUtil.nullFuture();
			}
			default: {
				return RxUtil.failedFuture( new ObjectDeletedException(
						"deleted entity passed to persist",
						null,
						getLoggableName( event.getEntityName(), entity )
				) );
			}
		}

	}

	@SuppressWarnings({ "unchecked" })
	protected void entityIsPersistent(PersistEvent event, Map createCache) {
		LOG.trace( "Ignoring persistent instance" );
		final EventSource source = event.getSession();

		//TODO: check that entry.getIdentifier().equals(requestedId)

		final Object entity = source.getPersistenceContextInternal().unproxy( event.getObject() );
		final EntityPersister persister = source.getEntityPersister( event.getEntityName(), entity );

		if ( createCache.put( entity, entity ) == null ) {
			justCascade( createCache, source, entity, persister );
		}
	}

	private void justCascade(Map createCache, EventSource source, Object entity, EntityPersister persister) {
		//TODO: merge into one method!
		cascadeBeforeSave( source, persister, entity, createCache );
		cascadeAfterSave( source, persister, entity, createCache );
	}

	/**
	 * Handle the given create event.
	 *
	 * @param event The save event to be handled.
	 * @param createCache The copy cache of entity instance to merge/copy instance.
	 */
	@SuppressWarnings({ "unchecked" })
	protected CompletionStage<Void> entityIsTransient(PersistEvent event, Map createCache) {
		LOG.trace( "Saving transient instance" );

		final EventSource source = event.getSession();
		final Object entity = source.getPersistenceContextInternal().unproxy( event.getObject() );

		if ( createCache.put( entity, entity ) == null ) {
			return saveWithGeneratedId( entity, event.getEntityName(), createCache, source, false )
					.thenApply( v -> null );
		}
		return RxUtil.nullFuture();
	}

	@SuppressWarnings({ "unchecked" })
	private void entityIsDeleted(PersistEvent event, Map createCache) {
		final EventSource source = event.getSession();

		final Object entity = source.getPersistenceContextInternal().unproxy( event.getObject() );
		final EntityPersister persister = source.getEntityPersister( event.getEntityName(), entity );

		if ( LOG.isTraceEnabled() ) {
			LOG.tracef(
					"un-scheduling entity deletion [%s]",
					MessageHelper.infoString(
							persister,
							persister.getIdentifier( entity, source ),
							source.getFactory()
					)
			);
		}

		if ( createCache.put( entity, entity ) == null ) {
			justCascade( createCache, source, entity, persister );
		}
	}

	@Override
	public void onPersist(PersistEvent event) throws HibernateException {
		// fake method
	}

	@Override
	public void onPersist(PersistEvent event, Map createdAlready) throws HibernateException {
		// fake method
	}

	public static class EventContextManagingPersistEventListenerDuplicationStrategy implements DuplicationStrategy {

		public static final DuplicationStrategy INSTANCE = new DefaultRxPersistEventListener.EventContextManagingPersistEventListenerDuplicationStrategy();

		private EventContextManagingPersistEventListenerDuplicationStrategy() {
		}

		@Override
		public boolean areMatch(Object listener, Object original) {
			if ( listener instanceof DefaultRxPersistEventListener && original instanceof PersistEventListener ) {
				return true;
			}

			return false;
		}

		@Override
		public Action getAction() {
			return Action.REPLACE_ORIGINAL;
		}
	}
}
