/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

import static org.hibernate.pretty.MessageHelper.infoString;
import static org.hibernate.reactive.engine.impl.Cascade.fetchLazyAssociationsBeforeCascade;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.TransientObjectException;
import org.hibernate.engine.internal.CascadePoint;
import org.hibernate.engine.internal.Nullability;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.internal.OnUpdateVisitor;
import org.hibernate.event.service.spi.JpaBootstrapSensitive;
import org.hibernate.event.spi.DeleteContext;
import org.hibernate.event.spi.DeleteEvent;
import org.hibernate.event.spi.DeleteEventListener;
import org.hibernate.event.spi.EventSource;
import org.hibernate.jpa.event.spi.CallbackRegistry;
import org.hibernate.jpa.event.spi.CallbackRegistryConsumer;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.engine.ReactiveActionQueue;
import org.hibernate.reactive.engine.impl.Cascade;
import org.hibernate.reactive.engine.impl.CascadingActions;
import org.hibernate.reactive.engine.impl.ForeignKeys;
import org.hibernate.reactive.engine.impl.ReactiveEntityDeleteAction;
import org.hibernate.reactive.engine.impl.ReactiveOrphanRemovalAction;
import org.hibernate.reactive.event.ReactiveDeleteEventListener;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.type.Type;
import org.hibernate.type.TypeHelper;

/**
 * A reactive {@link org.hibernate.event.internal.DefaultDeleteEventListener}.
 */
public class DefaultReactiveDeleteEventListener
		implements DeleteEventListener, ReactiveDeleteEventListener, CallbackRegistryConsumer, JpaBootstrapSensitive {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private CallbackRegistry callbackRegistry;
	private boolean jpaBootstrap;

	@Override
	public void injectCallbackRegistry(CallbackRegistry callbackRegistry) {
		this.callbackRegistry = callbackRegistry;
	}

	@Override
	public void wasJpaBootstrap(boolean wasJpaBootstrap) {
		this.jpaBootstrap = wasJpaBootstrap;
	}

	/**
	 * Handle the given delete event.
	 *
	 * @param event The delete event to be handled.
	 * @deprecated only the reactive version is supported
	 * @see #reactiveOnDelete(DeleteEvent)
	 */
	@Deprecated
	@Override
	public void onDelete(DeleteEvent event) {
		throw LOG.nonReactiveMethodCall( "reactiveOnDelete" );
	}

	/**
	 * @deprecated only the reactive version is supported
	 * @see #reactiveOnDelete(DeleteEvent, DeleteContext)
	 */
	@Deprecated
	@Override
	public void onDelete(DeleteEvent event, DeleteContext transientEntities) throws HibernateException {
		throw LOG.nonReactiveMethodCall( "reactiveOnDelete" );
	}

	/**
	 * Handle the given delete event.
	 *
	 * @param event The delete event to be handled.
	 *
	 */
	@Override
	public CompletionStage<Void> reactiveOnDelete(DeleteEvent event) throws HibernateException {
		return reactiveOnDelete( event, DeleteContext.create() );
	}

	/**
	 * Handle the given delete event.  This is the cascaded form.
	 *
	 * @param event The delete event.
	 * @param transientEntities The cache of entities already deleted
	 *
	 */
	@Override
	public CompletionStage<Void> reactiveOnDelete(DeleteEvent event, DeleteContext transientEntities) throws HibernateException {
		EventSource source = event.getSession();
		if ( event.getObject() instanceof CompletionStage ) {
			CompletionStage<Object> objectStage = (CompletionStage<Object>) event.getObject();
			return objectStage
					.thenCompose( objectEvent -> fetchAndDelete( event, transientEntities, source, objectEvent ) );
		}

		return fetchAndDelete( event, transientEntities, source, event.getObject() );
	}

	private CompletionStage<Void> fetchAndDelete(
			DeleteEvent event,
			DeleteContext transientEntities,
			EventSource source,
			Object objectEvent) {
		boolean detached = event.getEntityName() != null
				? !source.contains( event.getEntityName(), objectEvent )
				: !source.contains( objectEvent );
		if ( detached ) {
			// Hibernate Reactive doesn't support detached instances in remove()
			throw new IllegalArgumentException( "unmanaged instance passed to remove()" );
		}

		//Object entity = persistenceContext.unproxyAndReassociate( event.getObject() );

		return ( (ReactiveSession) source ).reactiveFetch( objectEvent, true )
				.thenCompose( entity -> reactiveOnDelete( event, transientEntities, entity ) );
	}

	private CompletionStage<Void> reactiveOnDelete(DeleteEvent event, DeleteContext transientEntities, Object entity) {

		EventSource source = event.getSession();
		PersistenceContext persistenceContext = source.getPersistenceContextInternal();

		EntityEntry entityEntry = persistenceContext.getEntry( entity );

		if ( entityEntry == null ) {
			LOG.trace( "Entity was not persistent in delete processing" );

			final EntityPersister persister = source.getEntityPersister( event.getEntityName(), entity );

			return ForeignKeys.isTransient( persister.getEntityName(), entity, null, source.getSession() )
					.thenCompose( trans -> {
						if ( trans ) {
							// EARLY EXIT!!!
							return deleteTransientEntity(
									source,
									entity,
									event.isCascadeDeleteEnabled(),
									persister,
									transientEntities
							);
						}
						performDetachedEntityDeletionCheck(event);

						final Object id = persister.getIdentifier( entity, source);

						if ( id == null ) {
							throw new TransientObjectException(
									"the detached instance passed to delete() had a null identifier"
							);
						}

						final EntityKey key = source.generateEntityKey( id, persister );

						persistenceContext.checkUniqueness( key, entity );

						new OnUpdateVisitor(source, id, entity ).process( entity, persister );

						final Object version = persister.getVersion( entity );

						EntityEntry entry = persistenceContext.addEntity(
								entity,
								( persister.isMutable() ? Status.MANAGED : Status.READ_ONLY ),
								persister.getPropertyValues( entity ),
								key,
								version,
								LockMode.NONE,
								true,
								persister,
								false
						);
						persister.afterReassociate( entity, source);

						callbackRegistry.preRemove( entity );

						return deleteEntity(
								source,
								entity,
								entry,
								event.isCascadeDeleteEnabled(),
								event.isOrphanRemovalBeforeUpdates(),
								persister,
								transientEntities
						)
						.thenAccept( v -> {
							if ( source.getFactory().getSessionFactoryOptions().isIdentifierRollbackEnabled() ) {
								persister.resetIdentifier( entity, id, version, source);
							}
						} );
					} );
		}
		else {
			LOG.trace( "Deleting a persistent instance" );

			Status status = entityEntry.getStatus();
			if ( status == Status.DELETED || status == Status.GONE ) {
				LOG.trace( "Object was already deleted" );
				return voidFuture();
			}
			final EntityPersister persister = entityEntry.getPersister();
			Object id = entityEntry.getId();
			Object version = entityEntry.getVersion();

			callbackRegistry.preRemove( entity );

			return deleteEntity(
					source,
					entity,
					entityEntry,
					event.isCascadeDeleteEnabled(),
					event.isOrphanRemovalBeforeUpdates(),
					persister,
					transientEntities
			)
			.thenAccept( v -> {
				if ( source.getFactory().getSessionFactoryOptions().isIdentifierRollbackEnabled() ) {
					persister.resetIdentifier( entity, id, version, source);
				}
			} );
		}
	}

	/**
	 * Called when we have recognized an attempt to delete a detached entity.
	 * <p>
	 * This is perfectly valid in Hibernate usage; JPA, however, forbids this.
	 * Thus, this is a hook for HEM to affect this behavior.
	 *
	 * @param event The event.
	 */
	protected void performDetachedEntityDeletionCheck(DeleteEvent event) {
		if ( jpaBootstrap ) {
			disallowDeletionOfDetached( event );
		}
		// ok in normal Hibernate usage to delete a detached entity; JPA however
		// forbids it, thus this is a hook for HEM to affect this behavior
	}

	private void disallowDeletionOfDetached(DeleteEvent event) {
		EventSource source = event.getSession();
		String entityName = event.getEntityName();
		EntityPersister persister = source.getEntityPersister( entityName, event.getObject() );
		Object id = persister.getIdentifier( event.getObject(), source );
		entityName = entityName == null ? source.guessEntityName( event.getObject() ) : entityName;
		throw new IllegalArgumentException( "Removing a detached instance " + entityName + "#" + id );
	}

	/**
	 * We encountered a delete request on a transient instance.
	 * <p>
	 * This is a deviation from historical Hibernate (pre-3.2) behavior to
	 * align with the JPA spec, which states that transient entities can be
	 * passed to remove operation in which case cascades still need to be
	 * performed.
	 *
	 * @param session The session which is the source of the event
	 * @param entity The entity being delete processed
	 * @param cascadeDeleteEnabled Is cascading of deletes enabled
	 * @param persister The entity persister
	 * @param transientEntities A cache of already visited transient entities
	 * (to avoid infinite recursion).
	 */
	protected CompletionStage<Void> deleteTransientEntity(
			EventSource session,
			Object entity,
			boolean cascadeDeleteEnabled,
			EntityPersister persister,
			DeleteContext transientEntities) {
		LOG.handlingTransientEntity();
		if ( !transientEntities.add( entity ) ) {
			LOG.trace( "Already handled transient entity; skipping" );
			return voidFuture();
		}
		transientEntities.add( entity );
		return cascadeBeforeDelete( session, persister, entity, null, transientEntities )
				.thenCompose( v -> cascadeAfterDelete( session, persister, entity, transientEntities ) );
	}

	/**
	 * Perform the entity deletion.  Well, as with most operations, does not
	 * really perform it; just schedules an action/execution with the
	 * {@link org.hibernate.engine.spi.ActionQueue} for execution during flush.
	 *
	 * @param session The originating session
	 * @param entity The entity to delete
	 * @param entityEntry The entity's entry in the {@link PersistenceContext}
	 * @param isCascadeDeleteEnabled Is delete cascading enabled?
	 * @param persister The entity persister.
	 * @param transientEntities A cache of already deleted entities.
	 */
	protected CompletionStage<Void> deleteEntity(
			final EventSource session,
			final Object entity,
			final EntityEntry entityEntry,
			final boolean isCascadeDeleteEnabled,
			final boolean isOrphanRemovalBeforeUpdates,
			final EntityPersister persister,
			final DeleteContext transientEntities) {

		if ( LOG.isTraceEnabled() ) {
			LOG.tracev(
					"Deleting {0}",
					infoString( persister, entityEntry.getId(), session.getFactory() )
			);
		}

		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
		final Type[] propTypes = persister.getPropertyTypes();
		final Object version = entityEntry.getVersion();

		final Object[] currentState = entityEntry.getLoadedState() == null
				? persister.getValues( entity ) // i.e. the entity came in from update()
				: entityEntry.getLoadedState();

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

		CompletionStage<Void> beforeDelete = cascadeBeforeDelete( session, persister, entity, entityEntry, transientEntities );

		CompletionStage<Void> nullifyAndAction = new ForeignKeys.Nullifier(
				entity,
				true,
				false,
				session,
				persister
		).nullifyTransientReferences( entityEntry.getDeletedState() )
				.thenAccept( v -> {
					new Nullability( session ).checkNullability(
							entityEntry.getDeletedState(),
							persister,
							Nullability.NullabilityCheckType.DELETE
					);
					persistenceContext.registerNullifiableEntityKey( key );

					ReactiveActionQueue actionQueue = actionQueue( session );

					if ( isOrphanRemovalBeforeUpdates ) {
						// TODO: The removeOrphan concept is a temporary "hack" for HHH-6484.  This should be removed once action/task
						// ordering is improved.
						actionQueue.addAction(
								new ReactiveOrphanRemovalAction(
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
						actionQueue.addAction(
								new ReactiveEntityDeleteAction(
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
				} );

		CompletionStage<Void> afterDelete = cascadeAfterDelete( session, persister, entity, transientEntities );

		return beforeDelete.thenCompose( v -> nullifyAndAction ).thenCompose( v -> afterDelete );
	}

	private ReactiveActionQueue actionQueue(EventSource session) {
		return session.unwrap( ReactiveSession.class ).getReactiveActionQueue();
	}

	private Object[] createDeletedState(EntityPersister persister, Object[] currentState, EventSource session) {
		Type[] propTypes = persister.getPropertyTypes();
		final Object[] deletedState = new Object[propTypes.length];
//      TypeFactory.deepCopy( currentState, propTypes, persister.getPropertyUpdateability(), deletedState, session );
		boolean[] copyability = new boolean[propTypes.length];
		java.util.Arrays.fill( copyability, true );
		TypeHelper.deepCopy( currentState, propTypes, copyability, deletedState, session );
		return deletedState;
	}

	protected CompletionStage<Void> cascadeBeforeDelete(
			EventSource session,
			EntityPersister persister,
			Object entity,
			EntityEntry entityEntry,
			DeleteContext transientEntities) throws HibernateException {
		// cascade-delete to collections BEFORE the collection owner is deleted
		return fetchLazyAssociationsBeforeCascade( CascadingActions.DELETE, persister, entity, session )
				.thenCompose(
						v -> new Cascade<DeleteContext>(
								CascadingActions.DELETE,
								CascadePoint.AFTER_INSERT_BEFORE_DELETE,
								persister, entity, transientEntities, session
						).cascade()
				);
	}

	protected CompletionStage<Void> cascadeAfterDelete(
			EventSource session,
			EntityPersister persister,
			Object entity,
			DeleteContext transientEntities) throws HibernateException {
		// cascade-delete to many-to-one AFTER the parent was deleted
		return new Cascade<>(
				CascadingActions.DELETE,
				CascadePoint.BEFORE_INSERT_AFTER_DELETE,
				persister, entity, transientEntities, session
		).cascade();
	}
}
