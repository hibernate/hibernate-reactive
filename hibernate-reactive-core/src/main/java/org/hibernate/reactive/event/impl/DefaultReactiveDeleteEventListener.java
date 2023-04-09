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
import org.hibernate.bytecode.enhance.spi.LazyPropertyInitializer;
import org.hibernate.bytecode.spi.BytecodeEnhancementMetadata;
import org.hibernate.engine.internal.CascadePoint;
import org.hibernate.engine.internal.Nullability;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.internal.OnUpdateVisitor;
import org.hibernate.event.internal.PostDeleteEventListenerStandardImpl;
import org.hibernate.event.service.spi.JpaBootstrapSensitive;
import org.hibernate.event.spi.DeleteContext;
import org.hibernate.event.spi.DeleteEvent;
import org.hibernate.event.spi.DeleteEventListener;
import org.hibernate.event.spi.EventSource;
import org.hibernate.internal.EmptyInterceptor;
import org.hibernate.internal.FastSessionServices;
import org.hibernate.jpa.event.spi.CallbackRegistry;
import org.hibernate.jpa.event.spi.CallbackRegistryConsumer;
import org.hibernate.jpa.event.spi.CallbackType;
import org.hibernate.metamodel.spi.MappingMetamodelImplementor;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.property.access.internal.PropertyAccessStrategyBackRefImpl;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.reactive.engine.ReactiveActionQueue;
import org.hibernate.reactive.engine.impl.Cascade;
import org.hibernate.reactive.engine.impl.CascadingActions;
import org.hibernate.reactive.engine.impl.ForeignKeys;
import org.hibernate.reactive.engine.impl.ReactiveCollectionRemoveAction;
import org.hibernate.reactive.engine.impl.ReactiveEntityDeleteAction;
import org.hibernate.reactive.engine.impl.ReactiveOrphanRemovalAction;
import org.hibernate.reactive.event.ReactiveDeleteEventListener;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.type.CollectionType;
import org.hibernate.type.CompositeType;
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
		if ( optimizeUnloadedDelete( event ) ) {
			return voidFuture();
		}
		else {
			final EventSource source = event.getSession();
			Object object = event.getObject();
			if ( object instanceof CompletionStage ) {
				final CompletionStage<Object> objectStage = (CompletionStage<Object>) object;
				return objectStage.thenCompose( objectEvent -> fetchAndDelete( event, transientEntities, source, objectEvent ) );
			}
			else {
				return fetchAndDelete( event, transientEntities, source, object);
			}
		}
	}

	private boolean optimizeUnloadedDelete(DeleteEvent event) {
		final Object object = event.getObject();
		final LazyInitializer lazyInitializer = HibernateProxy.extractLazyInitializer( object );
		if ( lazyInitializer != null ) {
			if ( lazyInitializer.isUninitialized() ) {
				final EventSource source = event.getSession();
				final EntityPersister persister = source.getFactory().getMappingMetamodel()
						.findEntityDescriptor( lazyInitializer.getEntityName() );
				final Object id = lazyInitializer.getIdentifier();
				final EntityKey key = source.generateEntityKey( id, persister );
				final PersistenceContext persistenceContext = source.getPersistenceContextInternal();
				if ( !persistenceContext.containsEntity( key )
						&& canBeDeletedWithoutLoading( source, persister ) ) {
					// optimization for deleting certain entities without loading them
					persistenceContext.reassociateProxy( object, id );
					if ( !persistenceContext.containsDeletedUnloadedEntityKey( key ) ) {
						persistenceContext.registerDeletedUnloadedEntityKey( key );

						if ( persister.hasOwnedCollections() ) {
							// we're deleting an unloaded proxy with collections
							for ( Type type : persister.getPropertyTypes() ) { //TODO: when we enable this for subclasses use getSubclassPropertyTypeClosure()
								deleteOwnedCollections( type, id, source );
							}
						}

						((ReactiveSession) source).getReactiveActionQueue()
								.addAction( new ReactiveEntityDeleteAction( id, persister, source ) );
					}
					return true;
				}
			}
		}
		return false;
	}

	private static void deleteOwnedCollections(Type type, Object key, EventSource session) {
		final MappingMetamodelImplementor mappingMetamodel = session.getFactory().getMappingMetamodel();
		final ReactiveActionQueue actionQueue = ((ReactiveSession) session).getReactiveActionQueue();
		if ( type.isCollectionType() ) {
			final String role = ( (CollectionType) type ).getRole();
			final CollectionPersister persister = mappingMetamodel.getCollectionDescriptor(role);
			if ( !persister.isInverse() ) {
				actionQueue.addAction( new ReactiveCollectionRemoveAction( persister, key, session ) );
			}
		}
		else if ( type.isComponentType() ) {
			final Type[] subtypes = ( (CompositeType) type ).getSubtypes();
			for ( Type subtype : subtypes ) {
				deleteOwnedCollections( subtype, key, session );
			}
		}
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
		final PersistenceContext persistenceContext = event.getSession().getPersistenceContextInternal();
		final EntityEntry entityEntry = persistenceContext.getEntry( entity );
		if ( entityEntry == null ) {
			return deleteTransientInstance( event, transientEntities, entity );
		}
		else {
			return deletePersistentInstance( event, transientEntities, entity, entityEntry );
		}
	}

	private CompletionStage<Void> deleteTransientInstance(DeleteEvent event, DeleteContext transientEntities, Object entity) {
		LOG.trace( "Entity was not persistent in delete processing" );

		final EventSource source = event.getSession();

		final EntityPersister persister = source.getEntityPersister( event.getEntityName(), entity);
		return ForeignKeys.isTransient( persister.getEntityName(), entity, null, source.getSession() )
				.thenCompose( trans -> {
					if ( trans ) {
						return deleteTransientEntity( source, entity, persister, transientEntities );
					}
					else {
						performDetachedEntityDeletionCheck( event );

						final Object id = persister.getIdentifier( entity, source );
						if ( id == null ) {
							throw new TransientObjectException(
									"the detached instance passed to delete() had a null identifier"
							);
						}

						final PersistenceContext persistenceContext = source.getPersistenceContextInternal();
						final EntityKey key = source.generateEntityKey( id, persister );

						persistenceContext.checkUniqueness( key, entity );

						new OnUpdateVisitor( source, id, entity ).process( entity, persister );

						final Object version = persister.getVersion( entity );

						final EntityEntry entry = persistenceContext.addEntity(
								entity,
								persister.isMutable() ? Status.MANAGED : Status.READ_ONLY,
								persister.getValues( entity  ),
								key,
								version,
								LockMode.NONE,
								true,
								persister,
								false
						);
						persister.afterReassociate( entity, source );

						return delete( event, transientEntities, source, entity, persister, id, version, entry );
					}
				} );
	}

	private CompletionStage<Void> deletePersistentInstance(
			DeleteEvent event,
			DeleteContext transientEntities,
			Object entity,
			EntityEntry entityEntry) {
		LOG.trace( "Deleting a persistent instance" );
		final EventSource source = event.getSession();
		if ( entityEntry.getStatus().isDeletedOrGone()
				|| source.getPersistenceContextInternal()
				.containsDeletedUnloadedEntityKey( entityEntry.getEntityKey() ) ) {
			LOG.trace( "Object was already deleted" );
			return voidFuture();
		}
		else {
			return delete(
					event,
					transientEntities,
					source,
					entity,
					entityEntry.getPersister(),
					entityEntry.getId(),
					entityEntry.getVersion(),
					entityEntry
			);
		}
	}

	private CompletionStage<Void> delete(
			DeleteEvent event,
			DeleteContext transientEntities,
			EventSource source,
			Object entity,
			EntityPersister persister,
			Object id,
			Object version,
			EntityEntry entry) {
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
						persister.resetIdentifier( entity, id, version, source );
					}
				} );
	}

	/**
	 * Can we delete the row represented by the proxy without loading the entity?
	 */
	private boolean canBeDeletedWithoutLoading(EventSource source, EntityPersister persister) {
		return source.getInterceptor() == EmptyInterceptor.INSTANCE
			&& !persister.implementsLifecycle()
			&& !persister.hasSubclasses() //TODO: should be unnecessary, using EntityPersister.getSubclassPropertyTypeClosure(), etc
			&& !persister.hasCascadeDelete()
			&& !persister.hasNaturalIdentifier()
			&& !persister.hasCollectionNotReferencingPK()
			&& !hasRegisteredRemoveCallbacks( persister )
			&& !hasCustomEventListeners( source );
	}

	private static boolean hasCustomEventListeners(EventSource source) {
		final FastSessionServices fss = source.getFactory().getFastSessionServices();
		// Bean Validation adds a PRE_DELETE listener
		// and Envers adds a POST_DELETE listener
		return fss.eventListenerGroup_PRE_DELETE.count() > 0
			|| fss.eventListenerGroup_POST_DELETE.count() > 1
			|| fss.eventListenerGroup_POST_DELETE.count() == 1
				&& !(fss.eventListenerGroup_POST_DELETE.listeners().iterator().next()
						instanceof PostDeleteEventListenerStandardImpl);
	}

	private boolean hasRegisteredRemoveCallbacks(EntityPersister persister) {
		final Class<?> mappedClass = persister.getMappedClass();
		return callbackRegistry.hasRegisteredCallbacks( mappedClass, CallbackType.PRE_REMOVE )
			|| callbackRegistry.hasRegisteredCallbacks( mappedClass, CallbackType.POST_REMOVE );
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
	 * @param persister The entity persister
	 * @param transientEntities A cache of already visited transient entities
	 * (to avoid infinite recursion).
	 */
	protected CompletionStage<Void> deleteTransientEntity(
			EventSource session,
			Object entity,
			EntityPersister persister,
			DeleteContext transientEntities) {
		LOG.handlingTransientEntity();
		if ( transientEntities.add( entity ) ) {
			return cascadeBeforeDelete( session, persister, entity, transientEntities )
					.thenCompose( v -> cascadeAfterDelete( session, persister, entity, transientEntities ) );
		}
		else {
			LOG.trace( "Already handled transient entity; skipping" );
			return voidFuture();
		}
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
			LOG.tracev( "Deleting {0}", infoString( persister, entityEntry.getId(), session.getFactory() ) );
		}

		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
		final Object version = entityEntry.getVersion();

		final Object[] currentState = entityEntry.getLoadedState() == null
				? persister.getValues( entity ) // i.e. the entity came in from update()
				: entityEntry.getLoadedState();

		final Object[] deletedState = createDeletedState( persister, entity, currentState, session );
		entityEntry.setDeletedState( deletedState );

		session.getInterceptor().onDelete(
				entity,
				entityEntry.getId(),
				deletedState,
				persister.getPropertyNames(),
				persister.getPropertyTypes()
		);

		// before any callbacks, etc., so subdeletions see that this deletion happened first
		persistenceContext.setEntryStatus( entityEntry, Status.DELETED );
		final EntityKey key = session.generateEntityKey( entityEntry.getId(), persister );

		return cascadeBeforeDelete( session, persister, entity, transientEntities )
				.thenCompose( v -> new ForeignKeys.Nullifier(
						entity,
						true,
						false,
						session,
						persister
				).nullifyTransientReferences( entityEntry.getDeletedState() )
						.thenAccept( vv -> {
							new Nullability( session ).checkNullability(
									entityEntry.getDeletedState(),
									persister,
									Nullability.NullabilityCheckType.DELETE
							);
							persistenceContext.registerNullifiableEntityKey( key );

							final ReactiveActionQueue actionQueue = actionQueue( session );

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
						} ) )
				.thenCompose( v -> cascadeAfterDelete( session, persister, entity, transientEntities ) );
	}

	private ReactiveActionQueue actionQueue(EventSource session) {
		return session.unwrap( ReactiveSession.class ).getReactiveActionQueue();
	}

	private Object[] createDeletedState(
			EntityPersister persister,
			Object parent,
			Object[] currentState,
			EventSource eventSource) {
		final Type[] types = persister.getPropertyTypes();
		final Object[] deletedState = new Object[types.length];
		if ( !persister.hasCollections() || !persister.hasUninitializedLazyProperties( parent ) ) {
			boolean[] copyability = new boolean[types.length];
			java.util.Arrays.fill( copyability, true );
			TypeHelper.deepCopy( currentState, types, copyability, deletedState, eventSource );
			return deletedState;
		}

		final String[] propertyNames = persister.getPropertyNames();
		final BytecodeEnhancementMetadata enhancementMetadata = persister.getBytecodeEnhancementMetadata();
		for ( int i = 0; i < types.length; i++) {
			if ( types[i].isCollectionType() && !enhancementMetadata.isAttributeLoaded( parent, propertyNames[i] ) ) {
				final CollectionType collectionType = (CollectionType) types[i];
				final CollectionPersister collectionDescriptor = persister.getFactory().getMappingMetamodel()
						.getCollectionDescriptor( collectionType.getRole() );
				if ( collectionDescriptor.needsRemove() || collectionDescriptor.hasCache() ) {
					final Object keyOfOwner = collectionType.getKeyOfOwner( parent, eventSource.getSession() );
					// This will make sure that a CollectionEntry exists
					deletedState[i] = collectionType.getCollection( keyOfOwner, eventSource.getSession(), parent, false );
				}
				else {
					deletedState[i] = currentState[i];
				}
			}
			else if ( currentState[i] == LazyPropertyInitializer.UNFETCHED_PROPERTY
					|| currentState[i] == PropertyAccessStrategyBackRefImpl.UNKNOWN ) {
				deletedState[i] = currentState[i];
			}
			else {
				deletedState[i] = types[i].deepCopy( currentState[i], eventSource.getFactory() );
			}
		}
		return deletedState;
	}

	protected CompletionStage<Void> cascadeBeforeDelete(
			EventSource session,
			EntityPersister persister,
			Object entity,
			DeleteContext transientEntities) throws HibernateException {
		// cascade-delete to collections BEFORE the collection owner is deleted
		return fetchLazyAssociationsBeforeCascade( CascadingActions.DELETE, persister, entity, session )
				.thenCompose(
						v -> new Cascade<>(
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
