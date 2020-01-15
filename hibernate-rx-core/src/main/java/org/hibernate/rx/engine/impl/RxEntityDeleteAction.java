package org.hibernate.rx.engine.impl;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.action.internal.EntityDeleteAction;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.cfg.NotYetImplementedException;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.service.spi.EventListenerGroup;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.PostCommitDeleteEventListener;
import org.hibernate.event.spi.PostDeleteEvent;
import org.hibernate.event.spi.PostDeleteEventListener;
import org.hibernate.event.spi.PreDeleteEvent;
import org.hibernate.event.spi.PreDeleteEventListener;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.rx.action.spi.RxExecutable;
import org.hibernate.rx.persister.impl.RxEntityPersister;
import org.hibernate.rx.util.RxUtil;
import org.hibernate.stat.spi.StatisticsImplementor;

public class RxEntityDeleteAction extends EntityDeleteAction implements RxExecutable {
	private final boolean isCascadeDeleteEnabled;
	private Object[] state;
	private Object version;
	private SoftLock lock;
	private Object[] naturalIdValues;

	/**
	 * Constructs an EntityDeleteAction.
	 *
	 * @param id The entity identifier
	 * @param state The current (extracted) entity state
	 * @param version The current entity version
	 * @param instance The entity instance
	 * @param persister The entity persister
	 * @param isCascadeDeleteEnabled Whether cascade delete is enabled
	 * @param session The session
	 */
	public RxEntityDeleteAction(
			Serializable id,
			Object[] state,
			Object version,
			Object instance,
			EntityPersister persister,
			boolean isCascadeDeleteEnabled,
			SessionImplementor session) {
		super( id, state, version, instance, persister, isCascadeDeleteEnabled, session );
		this.state = state;
		this.version = version;
		this.isCascadeDeleteEnabled = isCascadeDeleteEnabled;

		// before remove we need to remove the local (transactional) natural id cross-reference
		this.naturalIdValues = session.getPersistenceContextInternal()
				.getNaturalIdHelper()
				.removeLocalNaturalIdCrossReference(
						getPersister(),
						getId(),
						state
				);
	}

	@Override
	public void execute() throws HibernateException {
		throw new NotYetImplementedException();
	}

	@Override
	public CompletionStage<Void> rxExecute() throws HibernateException {
		final Serializable id = getId();
		final EntityPersister persister = getPersister();
		final SharedSessionContractImplementor session = getSession();
		final Object instance = getInstance();

		final boolean veto = preDelete();

		Object version = this.version;
		if ( persister.isVersionPropertyGenerated() ) {
			// we need to grab the version value from the entity, otherwise
			// we have issues with generated-version entities that may have
			// multiple actions queued during the same flush
			version = persister.getVersion( instance );
		}

		final Object ck;
		if ( persister.canWriteToCache() ) {
			final EntityDataAccess cache = persister.getCacheAccessStrategy();
			ck = cache.generateCacheKey( id, persister, session.getFactory(), session.getTenantIdentifier() );
			lock = cache.lockItem( session, ck, version );
		}
		else {
			ck = null;
		}

		CompletionStage<?> deleteStep = RxUtil.nullFuture();
		if ( !isCascadeDeleteEnabled && !veto ) {
			deleteStep = ( (RxEntityPersister) persister ).deleteRx( id, version, instance, session );
		}

		return deleteStep.thenAccept( deleteAR -> {
			//postDelete:
			// After actually deleting a row, record the fact that the instance no longer
			// exists on the database (needed for identity-column key generation), and
			// remove it from the session cache
			final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
			final EntityEntry entry = persistenceContext.removeEntry( instance );
			if ( entry == null ) {
				throw new AssertionFailure( "possible nonthreadsafe access to session" );
			}
			entry.postDelete();

			persistenceContext.removeEntity( entry.getEntityKey() );
			persistenceContext.removeProxy( entry.getEntityKey() );

			if ( persister.canWriteToCache() ) {
				persister.getCacheAccessStrategy().remove( session, ck );
			}

			persistenceContext.getNaturalIdHelper().removeSharedNaturalIdCrossReference(
					persister,
					id,
					naturalIdValues
			);

			postDelete();

			final StatisticsImplementor statistics = getSession().getFactory().getStatistics();
			if ( statistics.isStatisticsEnabled() && !veto ) {
				statistics.deleteEntity( getPersister().getEntityName() );
			}
		} );
	}

	private boolean preDelete() {
		boolean veto = false;
		final EventListenerGroup<PreDeleteEventListener> listenerGroup = listenerGroup( EventType.PRE_DELETE );
		if ( listenerGroup.isEmpty() ) {
			return veto;
		}
		final PreDeleteEvent event = new PreDeleteEvent( getInstance(), getId(), state, getPersister(), eventSource() );
		for ( PreDeleteEventListener listener : listenerGroup.listeners() ) {
			veto |= listener.onPreDelete( event );
		}
		return veto;
	}

	private void postDelete() {
		final EventListenerGroup<PostDeleteEventListener> listenerGroup = listenerGroup( EventType.POST_DELETE );
		if ( listenerGroup.isEmpty() ) {
			return;
		}
		final PostDeleteEvent event = new PostDeleteEvent(
				getInstance(),
				getId(),
				state,
				getPersister(),
				eventSource()
		);
		for ( PostDeleteEventListener listener : listenerGroup.listeners() ) {
			listener.onPostDelete( event );
		}
	}

	private void postCommitDelete(boolean success) {
		final EventListenerGroup<PostDeleteEventListener> listenerGroup = listenerGroup( EventType.POST_COMMIT_DELETE );
		if ( listenerGroup.isEmpty() ) {
			return;
		}
		final PostDeleteEvent event = new PostDeleteEvent(
				getInstance(),
				getId(),
				state,
				getPersister(),
				eventSource()
		);
		for ( PostDeleteEventListener listener : listenerGroup.listeners() ) {
			if ( PostCommitDeleteEventListener.class.isInstance( listener ) ) {
				if ( success ) {
					listener.onPostDelete( event );
				}
				else {
					( (PostCommitDeleteEventListener) listener ).onPostDeleteCommitFailed( event );
				}
			}
			else {
				//default to the legacy implementation that always fires the event
				listener.onPostDelete( event );
			}
		}
	}
}
