/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.LockMode;
import org.hibernate.NonUniqueObjectException;
import org.hibernate.action.internal.AbstractEntityInsertAction;
import org.hibernate.action.internal.EntityIdentityInsertAction;
import org.hibernate.engine.internal.CascadePoint;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityEntryExtraState;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SelfDirtinessTracker;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.internal.WrapVisitor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.generator.BeforeExecutionGenerator;
import org.hibernate.generator.Generator;
import org.hibernate.id.IdentifierGenerationException;
import org.hibernate.jpa.event.spi.CallbackRegistry;
import org.hibernate.jpa.event.spi.CallbackRegistryConsumer;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.engine.ReactiveActionQueue;
import org.hibernate.reactive.engine.impl.Cascade;
import org.hibernate.reactive.engine.impl.CascadingAction;
import org.hibernate.reactive.engine.impl.ReactiveEntityIdentityInsertAction;
import org.hibernate.reactive.engine.impl.ReactiveEntityRegularInsertAction;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.type.Type;
import org.hibernate.type.TypeHelper;

import static org.hibernate.engine.internal.ManagedTypeHelper.processIfSelfDirtinessTracker;
import static org.hibernate.engine.internal.Versioning.getVersion;
import static org.hibernate.engine.internal.Versioning.seedVersion;
import static org.hibernate.generator.EventType.INSERT;
import static org.hibernate.id.IdentifierGeneratorHelper.SHORT_CIRCUIT_INDICATOR;
import static org.hibernate.pretty.MessageHelper.infoString;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.nullFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * Functionality common to persist and merge event listeners.
 *
 * @see DefaultReactivePersistEventListener
 * @see DefaultReactivePersistOnFlushEventListener
 * @see DefaultReactiveMergeEventListener
 */
abstract class AbstractReactiveSaveEventListener<C> implements CallbackRegistryConsumer {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private CallbackRegistry callbackRegistry;

	public void injectCallbackRegistry(CallbackRegistry callbackRegistry) {
		this.callbackRegistry = callbackRegistry;
	}

	/**
	 * Prepares the save call using the given requested id.
	 *
	 * @param entity The entity to be saved.
	 * @param requestedId The id to which to associate the entity.
	 * @param entityName The name of the entity being saved.
	 * @param context Generally cascade-specific information.
	 * @param source The session which is the source of this save event.
	 *
	 * @return The id used to save the entity.
	 */
	protected CompletionStage<Void> reactiveSaveWithRequestedId(
			Object entity,
			Object requestedId,
			String entityName,
			C context,
			EventSource source) {
		callbackRegistry.preCreate( entity );

		return reactivePerformSave(
				entity,
				requestedId,
				source.getEntityPersister( entityName, entity ),
				false,
				context,
				source,
				true
		);
	}

	/**
	 * Prepares the save call using a newly generated id.
	 *
	 * @param entity The entity to be saved
	 * @param entityName The entity-name for the entity to be saved
	 * @param context Generally cascade-specific information.
	 * @param source The session which is the source of this save event.
	 * @param requiresImmediateIdAccess does the event context require
	 * access to the identifier immediately after execution of this method (if
	 * not, post-insert style id generators may be postponed if we are outside
	 * a transaction).
	 *
	 * @return The id used to save the entity; may be null depending on the
	 * type of id generator used and the requiresImmediateIdAccess value
	 */
	protected CompletionStage<Void> reactiveSaveWithGeneratedId(
			Object entity,
			String entityName,
			C context,
			EventSource source,
			boolean requiresImmediateIdAccess) {
		callbackRegistry.preCreate( entity );

		processIfSelfDirtinessTracker( entity, SelfDirtinessTracker::$$_hibernate_clearDirtyAttributes );

		final EntityPersister persister = source.getEntityPersister( entityName, entity );
		final Generator generator = persister.getGenerator();
		if ( !generator.generatedOnExecution() ) {
			if ( generator instanceof ReactiveIdentifierGenerator ) {
				return ( (ReactiveIdentifierGenerator<?>) generator )
						.generate( ( ReactiveConnectionSupplier ) source, entity )
						.thenCompose( generatedId -> performSaveWithId( entity, context, source, persister, generator, generatedId ) );
			}

			final Object generatedId = ( (BeforeExecutionGenerator) generator ).generate( source, entity, null, INSERT );
			return performSaveWithId( entity, context, source, persister, generator, generatedId );
		}
		else {
			return reactivePerformSave( entity, null, persister, true, context, source, requiresImmediateIdAccess );
		}
	}

	private CompletionStage<Void> performSaveWithId(
			Object entity,
			C context,
			EventSource source,
			EntityPersister persister,
			Generator generator,
			Object generatedId) {
		if ( generatedId == null ) {
			throw new IdentifierGenerationException( "null id generated for: " + entity.getClass() );
		}
		if ( generatedId == SHORT_CIRCUIT_INDICATOR ) {
			source.getIdentifier( entity );
			return voidFuture();
		}
		if ( LOG.isDebugEnabled() ) {
			LOG.debugf(
					"Generated identifier: %s, using strategy: %s",
					persister.getIdentifierType().toLoggableString( generatedId, source.getFactory() ),
					generator.getClass().getName()
			);
		}
		return reactivePerformSave( entity, generatedId, persister, false, context, source, true );
	}

	/**
	 * Prepares the save call by checking the session caches for a pre-existing
	 * entity and performing any lifecycle callbacks.
	 *
	 * @param entity The entity to be saved.
	 * @param id The id by which to save the entity.
	 * @param persister The entity's persister instance.
	 * @param useIdentityColumn Is an identity column being used?
	 * @param context Generally cascade-specific information.
	 * @param source The session from which the event originated.
	 * @param requiresImmediateIdAccess does the event context require
	 * access to the identifier immediately after execution of this method (if
	 * not, post-insert style id generators may be postponed if we are outside
	 * a transaction).
	 *
	 * @return The id used to save the entity; may be null depending on the
	 * type of id generator used and the requiresImmediateIdAccess value
	 */
	protected CompletionStage<Void> reactivePerformSave(
			Object entity,
			Object id,
			EntityPersister persister,
			boolean useIdentityColumn,
			C context,
			EventSource source,
			boolean requiresImmediateIdAccess) {

		if ( LOG.isTraceEnabled() ) {
			LOG.tracev( "Saving {0}", infoString( persister, id, source.getFactory() ) );
		}

		return entityKey( entity, id, persister, useIdentityColumn, source )
				.thenCompose( key -> reactivePerformSaveOrReplicate(
						entity,
						key,
						persister,
						useIdentityColumn,
						context,
						source,
						requiresImmediateIdAccess
				) );
	}

	private CompletionStage<EntityKey> entityKey(Object entity, Object id, EntityPersister persister, boolean useIdentityColumn, EventSource source) {
		return useIdentityColumn
				? nullFuture()
				: generateEntityKey( id, persister, source )
						.thenApply( generatedKey -> {
							persister.setIdentifier( entity, id, source );
							return generatedKey;
						} );
	}

	private CompletionStage<EntityKey> generateEntityKey(Object id, EntityPersister persister, EventSource source) {
		final EntityKey key = source.generateEntityKey( id, persister );
		final PersistenceContext persistenceContext = source.getPersistenceContextInternal();
		final Object old = persistenceContext.getEntity( key );
		if ( old != null ) {
			if ( persistenceContext.getEntry( old ).getStatus() == Status.DELETED ) {
				return source.unwrap( ReactiveSession.class )
						.reactiveForceFlush( persistenceContext.getEntry( old ) )
						.thenApply( v -> key );
			}
			else {
				return failedFuture( new NonUniqueObjectException( id, persister.getEntityName() ) );
			}
		}
		else {
			return completedFuture( key );
		}
	}

	/**
	 * Performs all the actual work needed to save an entity (well to get the save moved to
	 * the execution queue).
	 *
	 * @param entity The entity to be saved
	 * @param key The id to be used for saving the entity (or null, in the case of identity columns)
	 * @param persister The entity's persister instance.
	 * @param useIdentityColumn Should an identity column be used for id generation?
	 * @param context Generally cascade-specific information.
	 * @param source The session which is the source of the current event.
	 * @param requiresImmediateIdAccess Is access to the identifier required immediately
	 * after the completion of the save?  persist(), for example, does not require this...
	 *
	 * @return The id used to save the entity; may be null depending on the
	 * type of id generator used and the requiresImmediateIdAccess value
	 */
	protected CompletionStage<Void> reactivePerformSaveOrReplicate(
			Object entity,
			EntityKey key,
			EntityPersister persister,
			boolean useIdentityColumn,
			C context,
			EventSource source,
			boolean requiresImmediateIdAccess) {

		final Object id = key == null ? null : key.getIdentifier();

		final boolean inTransaction = source.isTransactionInProgress();
		final boolean shouldDelayIdentityInserts = !inTransaction && !requiresImmediateIdAccess;
		final PersistenceContext persistenceContext = source.getPersistenceContextInternal();

		// Put a placeholder in entries, so we don't recurse back and try to save() the
		// same object again. QUESTION: should this be done before onSave() is called?
		// likewise, should it be done before onUpdate()?
		final EntityEntry original = persistenceContext.addEntry(
				entity,
				Status.SAVING,
				null,
				null,
				id,
				null,
				LockMode.WRITE,
				useIdentityColumn,
				persister,
				false
		);

		return cascadeBeforeSave( source, persister, entity, context )
				.thenCompose( v -> addInsertAction(
							// We have to do this after cascadeBeforeSave completes,
							// since it could result in generation of parent ids,
							// which we will need as foreign keys in the insert
							cloneAndSubstituteValues( entity, persister, context, source, id ),
							id,
							entity,
							persister,
							useIdentityColumn,
							source,
							shouldDelayIdentityInserts
					) )
				.thenCompose( insert -> cascadeAfterSave( source, persister, entity, context )
					.thenAccept( unused -> {
						final Object finalId = handleGeneratedId( useIdentityColumn, id, insert );
						final EntityEntry newEntry = persistenceContext.getEntry( entity );
						if ( newEntry != original ) {
							final EntityEntryExtraState extraState = newEntry.getExtraState( EntityEntryExtraState.class );
							if ( extraState == null ) {
								newEntry.addExtraState( original.getExtraState( EntityEntryExtraState.class ) );
							}
						}
					} )
				);
	}

	private static Object handleGeneratedId(boolean useIdentityColumn, Object id, AbstractEntityInsertAction insert) {
		if ( useIdentityColumn && insert.isEarlyInsert() ) {
			if ( insert instanceof EntityIdentityInsertAction ) {
				final Object generatedId = ( (EntityIdentityInsertAction) insert ).getGeneratedId();
				insert.handleNaturalIdPostSaveNotifications( generatedId );
				return generatedId;
			}
			throw new IllegalStateException(
					"Insert should be using an identity column, but action is of unexpected type: " + insert.getClass()
							.getName() );
		}
		else {
			return id;
		}
	}

	private Object[] cloneAndSubstituteValues(Object entity, EntityPersister persister, C context, EventSource source, Object id) {
		final Object[] values = persister.getPropertyValuesToInsert( entity, getMergeMap( context ), source );
		final Type[] types = persister.getPropertyTypes();

		boolean substitute = substituteValuesIfNecessary( entity, id, values, persister, source );
		if ( persister.hasCollections() ) {
			substitute = visitCollectionsBeforeSave( entity, id, values, types, source ) || substitute;
		}

		if ( substitute ) {
			persister.setValues( entity, values );
		}

		TypeHelper.deepCopy(
				values,
				types,
				persister.getPropertyUpdateability(),
				values,
				source
		);
		return values;
	}

	protected Map<Object,Object> getMergeMap(C anything) {
		return null;
	}

	private CompletionStage<AbstractEntityInsertAction> addInsertAction(
			Object[] values,
			Object id,
			Object entity,
			EntityPersister persister,
			boolean useIdentityColumn,
			EventSource source,
			boolean shouldDelayIdentityInserts) {
		final ReactiveActionQueue actionQueue = source.unwrap(ReactiveSession.class).getReactiveActionQueue();
		if ( useIdentityColumn ) {
			final ReactiveEntityIdentityInsertAction insert = new ReactiveEntityIdentityInsertAction(
					values, entity, persister, false, source, shouldDelayIdentityInserts
			);
			return actionQueue.addAction( insert ).thenApply( v -> insert );
		}
		else {
			final ReactiveEntityRegularInsertAction insert = new ReactiveEntityRegularInsertAction(
					id, values, entity, getVersion( values, persister ), persister, false, source
			);
			return actionQueue.addAction( insert ).thenApply( v -> insert );
		}
	}

	/**
	 * Handles the calls needed to perform pre-save cascades for the given entity.
	 *
	 * @param source The session from which the save event originated.
	 * @param persister The entity's persister instance.
	 * @param entity The entity to be saved.
	 * @param context Generally cascade-specific data
	 */
	protected CompletionStage<Void> cascadeBeforeSave(
			EventSource source,
			EntityPersister persister,
			Object entity,
			C context) {
		// cascade-save to many-to-one BEFORE the parent is saved
		return new Cascade<>(
				getCascadeReactiveAction(),
				CascadePoint.BEFORE_INSERT_AFTER_DELETE,
				persister, entity, context, source
		).cascade();
	}

	/**
	 * Handles to calls needed to perform post-save cascades.
	 *
	 * @param source The session from which the event originated.
	 * @param persister The entity's persister instance.
	 * @param entity The entity beng saved.
	 * @param context Generally cascade-specific data
	 */
	protected CompletionStage<Void> cascadeAfterSave(
			EventSource source,
			EntityPersister persister,
			Object entity,
			C context) {
		// cascade-save to collections AFTER the collection owner was saved
		return new Cascade<>(
				getCascadeReactiveAction(),
				CascadePoint.AFTER_INSERT_BEFORE_DELETE,
				persister, entity, context, source
		).cascade();
	}

	protected abstract CascadingAction<C> getCascadeReactiveAction();

	/**
	 * Perform any property value substitution that is necessary
	 * (interceptor callback, version initialization...)
	 *
	 * @param entity The entity
	 * @param id The entity identifier
	 * @param values The snapshot entity state
	 * @param persister The entity persister
	 * @param source The originating session
	 *
	 * @return True if the snapshot state changed such that
	 *         reinjection of the values into the entity is required.
	 */
	protected boolean substituteValuesIfNecessary(
			Object entity,
			Object id,
			Object[] values,
			EntityPersister persister,
			SessionImplementor source) {
		boolean substitute = source.getInterceptor().onSave(
				entity,
				id,
				values,
				persister.getPropertyNames(),
				persister.getPropertyTypes()
		);

		//keep the existing version number in the case of replicate!
		if ( persister.isVersioned() ) {
			substitute = seedVersion( entity, values, persister, source ) || substitute;
		}
		return substitute;
	}

	protected boolean visitCollectionsBeforeSave(
			Object entity,
			Object id,
			Object[] values,
			Type[] types,
			EventSource source) {
		final WrapVisitor visitor = new WrapVisitor( entity, id, source );
		// substitutes into values by side effect
		visitor.processEntityPropertyValues( values, types );
		return visitor.isSubstitutionRequired();
	}

}
