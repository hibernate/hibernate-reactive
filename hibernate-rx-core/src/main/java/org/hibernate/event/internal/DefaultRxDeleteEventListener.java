package org.hibernate.event.internal;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.TransientObjectException;
import org.hibernate.action.internal.OrphanRemovalAction;
import org.hibernate.engine.internal.ForeignKeys;
import org.hibernate.engine.internal.Nullability;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.service.spi.DuplicationStrategy;
import org.hibernate.event.spi.DeleteEvent;
import org.hibernate.event.spi.DeleteEventListener;
import org.hibernate.event.spi.EventSource;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.rx.RxHibernateSession;
import org.hibernate.rx.engine.impl.RxEntityDeleteAction;
import org.hibernate.rx.event.RxDeleteEvent;
import org.hibernate.type.Type;
import org.hibernate.type.TypeHelper;

public class DefaultRxDeleteEventListener extends DefaultDeleteEventListener {

	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( DefaultRxDeleteEventListener.class );

	@Override
	public void onDelete(DeleteEvent event, Set transientEntities) throws HibernateException {
		final EventSource source = event.getSession();
		CompletionStage<Void> deleteStage = ((RxDeleteEvent) event).getStage();

		final PersistenceContext persistenceContext = source.getPersistenceContextInternal();
		Object entity = persistenceContext.unproxyAndReassociate( event.getObject() );

		EntityEntry entityEntry = persistenceContext.getEntry( entity );
		final EntityPersister persister;
		final Serializable id;
		final Object version;

		if ( entityEntry == null ) {
			LOG.trace( "Entity was not persistent in delete processing" );

			persister = source.getEntityPersister( event.getEntityName(), entity );

			if ( ForeignKeys.isTransient( persister.getEntityName(), entity, null, source ) ) {
				deleteTransientEntity( source, entity, event.isCascadeDeleteEnabled(), persister, transientEntities );
				// EARLY EXIT!!!
				return;
			}
			performDetachedEntityDeletionCheck( event );

			id = persister.getIdentifier( entity, source );

			if ( id == null ) {
				throw new TransientObjectException(
						"the detached instance passed to delete() had a null identifier"
				);
			}

			final EntityKey key = source.generateEntityKey( id, persister );

			persistenceContext.checkUniqueness( key, entity );

			new OnUpdateVisitor( source, id, entity ).process( entity, persister );

			version = persister.getVersion( entity );

			entityEntry = persistenceContext.addEntity(
					entity,
					(persister.isMutable() ? Status.MANAGED : Status.READ_ONLY),
					persister.getPropertyValues( entity ),
					key,
					version,
					LockMode.NONE,
					true,
					persister,
					false
			);
			persister.afterReassociate( entity, source );
		}
		else {
			LOG.trace( "Deleting a persistent instance" );

			if ( entityEntry.getStatus() == Status.DELETED || entityEntry.getStatus() == Status.GONE ) {
				LOG.trace( "Object was already deleted" );
				return;
			}
			persister = entityEntry.getPersister();
			id = entityEntry.getId();
			version = entityEntry.getVersion();
		}

		/*if ( !persister.isMutable() ) {
			throw new HibernateException(
					"attempted to delete an object of immutable class: " +
					MessageHelper.infoString(persister)
				);
		}*/

		if ( invokeDeleteLifecycle( source, entity, persister ) ) {
			return;
		}

		deleteEntity(
				source,
				entity,
				entityEntry,
				event.isCascadeDeleteEnabled(),
				event.isOrphanRemovalBeforeUpdates(),
				persister,
				transientEntities,
				deleteStage
		);

		if ( source.getFactory().getSettings().isIdentifierRollbackEnabled() ) {
			persister.resetIdentifier( entity, id, version, source );
		}
	}

	protected void deleteEntity(
			final EventSource session,
			final Object entity,
			final EntityEntry entityEntry,
			final boolean isCascadeDeleteEnabled,
			final boolean isOrphanRemovalBeforeUpdates,
			final EntityPersister persister,
			final Set transientEntities,
			final CompletionStage<Void> deleteStage) {

		if ( LOG.isTraceEnabled() ) {
			LOG.tracev(
					"Deleting {0}",
					MessageHelper.infoString( persister, entityEntry.getId(), session.getFactory() )
			);
		}

		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
		final Type[] propTypes = persister.getPropertyTypes();
		final Object version = entityEntry.getVersion();

		final Object[] currentState;
		if ( entityEntry.getLoadedState() == null ) {
			//ie. the entity came in from update()
			currentState = persister.getPropertyValues( entity );
		}
		else {
			currentState = entityEntry.getLoadedState();
		}

		final Object[] deletedState = createDeletedState( persister, currentState, session );
		entityEntry.setDeletedState( deletedState );

		session.getInterceptor().onDelete(
				entity,
				entityEntry.getId(),
				deletedState,
				persister.getPropertyNames(),
				propTypes
		);

		// before any callbacks, etc, so subdeletions see that this deletion happened first
		persistenceContext.setEntryStatus( entityEntry, Status.DELETED );
		final EntityKey key = session.generateEntityKey( entityEntry.getId(), persister );

		cascadeBeforeDelete( session, persister, entity, entityEntry, transientEntities );

		new ForeignKeys.Nullifier(  entity, true, false, session, persister ).nullifyTransientReferences( entityEntry.getDeletedState() );
		new Nullability( session ).checkNullability( entityEntry.getDeletedState(), persister, Nullability.NullabilityCheckType.DELETE );
		persistenceContext.getNullifiableEntityKeys().add( key );

		if ( isOrphanRemovalBeforeUpdates ) {
			// TODO: The removeOrphan concept is a temporary "hack" for HHH-6484.  This should be removed once action/task
			// ordering is improved.
			session.getActionQueue().addAction(
					new OrphanRemovalAction(
							entityEntry.getId(),
							deletedState,
							version,
							entity,
							persister,
							isCascadeDeleteEnabled,
							session
					)
			);
		}
		else {
			// Ensures that containing deletions happen before sub-deletions
			( (RxHibernateSession) session ).getRxActionQueue().addAction(
					new RxEntityDeleteAction(
							entityEntry.getId(),
							deletedState,
							version,
							entity,
							persister,
							isCascadeDeleteEnabled,
							session,
							deleteStage
					)
			);
		}

		cascadeAfterDelete( session, persister, entity, transientEntities );
	}

	private Object[] createDeletedState(EntityPersister persister, Object[] currentState, EventSource session) {
		Type[] propTypes = persister.getPropertyTypes();
		final Object[] deletedState = new Object[propTypes.length];
//		TypeFactory.deepCopy( currentState, propTypes, persister.getPropertyUpdateability(), deletedState, session );
		boolean[] copyability = new boolean[propTypes.length];
		java.util.Arrays.fill( copyability, true );
		TypeHelper.deepCopy( currentState, propTypes, copyability, deletedState, session );
		return deletedState;
	}

	public static class EventContextManagingDeleteEventListenerDuplicationStrategy implements DuplicationStrategy {

		public static final DuplicationStrategy INSTANCE = new DefaultRxDeleteEventListener.EventContextManagingDeleteEventListenerDuplicationStrategy();

		private EventContextManagingDeleteEventListenerDuplicationStrategy() {
		}

		@Override
		public boolean areMatch(Object listener, Object original) {
			if ( listener instanceof DefaultRxDeleteEventListener && original instanceof DeleteEventListener ) {
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
