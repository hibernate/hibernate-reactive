package org.hibernate.rx.event.impl;


import org.hibernate.LockMode;
import org.hibernate.NonUniqueObjectException;
import org.hibernate.action.internal.AbstractEntityInsertAction;
import org.hibernate.action.internal.EntityIdentityInsertAction;
import org.hibernate.engine.internal.CascadePoint;
import org.hibernate.engine.internal.Versioning;
import org.hibernate.engine.spi.*;
import org.hibernate.event.internal.AbstractSaveEventListener;
import org.hibernate.event.spi.EventSource;
import org.hibernate.id.IdentifierGenerationException;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.jpa.event.spi.CallbackRegistry;
import org.hibernate.jpa.event.spi.CallbackRegistryConsumer;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.rx.RxSessionInternal;
import org.hibernate.rx.engine.impl.Cascade;
import org.hibernate.rx.engine.impl.CascadingAction;
import org.hibernate.rx.engine.impl.RxEntityInsertAction;
import org.hibernate.rx.persister.entity.impl.RxEntityPersister;
import org.hibernate.rx.util.impl.RxUtil;
import org.hibernate.type.Type;
import org.hibernate.type.TypeHelper;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * A convenience base class for listeners responding to save events.
 *
 * @author Steve Ebersole.
 */
abstract class AbstractRxSaveEventListener
		implements CallbackRegistryConsumer {

	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( AbstractRxSaveEventListener.class );

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
	 * @param anything Generally cascade-specific information.
	 * @param source The session which is the source of this save event.
	 *
	 * @return The id used to save the entity.
	 */
	protected CompletionStage<Void> rxSaveWithRequestedId(
			Object entity,
			Serializable requestedId,
			String entityName,
			Object anything,
			EventSource source) {
		callbackRegistry.preCreate( entity );

		return rxPerformSave(
				entity,
				requestedId,
				source.getEntityPersister( entityName, entity ),
				false,
				anything,
				source,
				true
		);
	}

	/**
	 * Prepares the save call using a newly generated id.
	 *
	 * @param entity The entity to be saved
	 * @param entityName The entity-name for the entity to be saved
	 * @param anything Generally cascade-specific information.
	 * @param source The session which is the source of this save event.
	 * @param requiresImmediateIdAccess does the event context require
	 * access to the identifier immediately after execution of this method (if
	 * not, post-insert style id generators may be postponed if we are outside
	 * a transaction).
	 *
	 * @return The id used to save the entity; may be null depending on the
	 * type of id generator used and the requiresImmediateIdAccess value
	 */
	protected CompletionStage<Void> rxSaveWithGeneratedId(
			Object entity,
			String entityName,
			Object anything,
			EventSource source,
			boolean requiresImmediateIdAccess) {
		callbackRegistry.preCreate( entity );

		if ( entity instanceof SelfDirtinessTracker ) {
			( (SelfDirtinessTracker) entity ).$$_hibernate_clearDirtyAttributes();
		}
		EntityPersister persister = source.getEntityPersister( entityName, entity );
		Serializable assignedId = persister.getIdentifier( entity, source.getSession() );
		return RxEntityPersister.get(persister)
				.getIdentifierGenerator()
				.generate( source.getFactory() )
				.thenCompose( id -> rxPerformSave(entity,
						assignIdIfNecessary(id, assignedId, entityName),
						persister, false, anything, source, true) );
	}

	private static Serializable assignIdIfNecessary(Optional<Integer> generatedId, Serializable assignedId, String entityName) {
		Serializable id;
		if ( !generatedId.isPresent() ) {
			if (assignedId == null) {
				throw new IdentifierGenerationException("ids for this class must be manually assigned before calling save(): " + entityName);
			}
			return assignedId;
		}
		else {
			id = generatedId.get();
		}
		return id;
	}

	/**
	 * Prepares the save call by checking the session caches for a pre-existing
	 * entity and performing any lifecycle callbacks.
	 *
	 * @param entity The entity to be saved.
	 * @param id The id by which to save the entity.
	 * @param persister The entity's persister instance.
	 * @param useIdentityColumn Is an identity column being used?
	 * @param anything Generally cascade-specific information.
	 * @param source The session from which the event originated.
	 * @param requiresImmediateIdAccess does the event context require
	 * access to the identifier immediately after execution of this method (if
	 * not, post-insert style id generators may be postponed if we are outside
	 * a transaction).
	 *
	 * @return The id used to save the entity; may be null depending on the
	 * type of id generator used and the requiresImmediateIdAccess value
	 */
	protected CompletionStage<Void> rxPerformSave(
			Object entity,
			Serializable id,
			EntityPersister persister,
			boolean useIdentityColumn,
			Object anything,
			EventSource source,
			boolean requiresImmediateIdAccess) {

		if ( LOG.isTraceEnabled() ) {
			LOG.tracev( "Saving {0}", MessageHelper.infoString( persister, id, source.getFactory() ) );
		}

		final EntityKey key;
		if ( !useIdentityColumn ) {
			key = source.generateEntityKey( id, persister );
			final PersistenceContext persistenceContext = source.getPersistenceContextInternal();
			Object old = persistenceContext.getEntity( key );
			if ( old != null ) {
				if ( persistenceContext.getEntry( old ).getStatus() == Status.DELETED ) {
					source.forceFlush( persistenceContext.getEntry( old ) );
				}
				else {
					return RxUtil.failedFuture( new NonUniqueObjectException( id, persister.getEntityName() ) );
				}
			}
			persister.setIdentifier( entity, id, source );
		}
		else {
			key = null;
		}

		return rxPerformSaveOrReplicate(
				entity,
				key,
				persister,
				useIdentityColumn,
				anything,
				source,
				requiresImmediateIdAccess
		);
	}

	/**
	 * Performs all the actual work needed to save an entity (well to get the save moved to
	 * the execution queue).
	 *
	 * @param entity The entity to be saved
	 * @param key The id to be used for saving the entity (or null, in the case of identity columns)
	 * @param persister The entity's persister instance.
	 * @param useIdentityColumn Should an identity column be used for id generation?
	 * @param anything Generally cascade-specific information.
	 * @param source The session which is the source of the current event.
	 * @param requiresImmediateIdAccess Is access to the identifier required immediately
	 * after the completion of the save?  persist(), for example, does not require this...
	 *
	 * @return The id used to save the entity; may be null depending on the
	 * type of id generator used and the requiresImmediateIdAccess value
	 */
	protected CompletionStage<Void> rxPerformSaveOrReplicate(
			Object entity,
			EntityKey key,
			EntityPersister persister,
			boolean useIdentityColumn,
			Object anything,
			EventSource source,
			boolean requiresImmediateIdAccess) {

		Serializable id = key == null ? null : key.getIdentifier();

		boolean inTrx = source.isTransactionInProgress();
		boolean shouldDelayIdentityInserts = !inTrx && !requiresImmediateIdAccess;
		final PersistenceContext persistenceContext = source.getPersistenceContextInternal();

		// Put a placeholder in entries, so we don't recurse back and try to save() the
		// same object again. QUESTION: should this be done before onSave() is called?
		// likewise, should it be done before onUpdate()?
		EntityEntry original = persistenceContext.addEntry(
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

		CompletionStage<Void> cascadeBeforeSave = rxCascadeBeforeSave(source, persister, entity, anything);

		Object[] values = persister.getPropertyValuesToInsert( entity, getMergeMap( anything ), source );
		Type[] types = persister.getPropertyTypes();

		boolean substitute = helper.substituteValuesIfNecessary( entity, id, values, persister, source );

		if ( persister.hasCollections() ) {
			substitute = substitute || helper.visitCollectionsBeforeSave( entity, id, values, types, source );
		}

		if ( substitute ) {
			persister.setPropertyValues( entity, values );
		}

		TypeHelper.deepCopy(
				values,
				types,
				persister.getPropertyUpdateability(),
				values,
				source
		);

		CompletionStage<AbstractEntityInsertAction> insert = addInsertAction(
				values, id, entity, persister, useIdentityColumn, source, shouldDelayIdentityInserts
		);

		CompletionStage<Void> cascadeAfterSave = rxCascadeAfterSave(source, persister, entity, anything);

		EntityEntry newEntry = persistenceContext.getEntry( entity );

		if ( newEntry != original ) {
			EntityEntryExtraState extraState = newEntry.getExtraState( EntityEntryExtraState.class );
			if ( extraState == null ) {
				newEntry.addExtraState( original.getExtraState( EntityEntryExtraState.class ) );
			}
		}

		return cascadeBeforeSave
				.thenCompose(v -> insert)
				.thenCompose(v -> cascadeAfterSave);
//				.thenAccept( v -> {
			// postpone initializing id in case the insert has non-nullable transient dependencies
			// that are not resolved until cascadeAfterSave() is executed

//			Serializable newId = id;
//			if ( useIdentityColumn && insert.isEarlyInsert() ) {
//				if ( !EntityIdentityInsertAction.class.isInstance( insert ) ) {
//					throw new IllegalStateException(
//							"Insert should be using an identity column, but action is of unexpected type: " +
//									insert.getClass().getName()
//					);
//				}
//				newId = ( (EntityIdentityInsertAction) insert ).getGeneratedId();
//
//				insert.handleNaturalIdPostSaveNotifications( newId );
//			}
//			return newId;
//		} );
	}

	protected Map getMergeMap(Object anything) {
		return null;
	}

	private CompletionStage<AbstractEntityInsertAction> addInsertAction(
			Object[] values,
			Serializable id,
			Object entity,
			EntityPersister persister,
			boolean useIdentityColumn,
			EventSource source,
			boolean shouldDelayIdentityInserts) {
		if ( useIdentityColumn ) {
			EntityIdentityInsertAction insert = new EntityIdentityInsertAction(
					values, entity, persister, false, source, shouldDelayIdentityInserts
			);
			return ( (RxSessionInternal) source ).getRxActionQueue().addAction( insert ).thenApply(v -> insert );
		}
		else {
			Object version = Versioning.getVersion( values, persister );
			RxEntityInsertAction insert = new RxEntityInsertAction(
					id, values, entity, version, persister, false, source
			);
			return ( (RxSessionInternal) source ).getRxActionQueue().addAction( insert ).thenApply(v -> insert );
		}
	}

	/**
	 * Handles the calls needed to perform pre-save cascades for the given entity.
	 *
	 * @param source The session from whcih the save event originated.
	 * @param persister The entity's persister instance.
	 * @param entity The entity to be saved.
	 * @param anything Generally cascade-specific data
	 */
	protected CompletionStage<Void> rxCascadeBeforeSave(
			EventSource source,
			EntityPersister persister,
			Object entity,
			Object anything) {

		// cascade-save to many-to-one BEFORE the parent is saved
		final PersistenceContext persistenceContext = source.getPersistenceContextInternal();
		persistenceContext.incrementCascadeLevel();
		try {
			return new Cascade(
					getCascadeRxAction(),
					CascadePoint.BEFORE_INSERT_AFTER_DELETE,
					persister,
					entity,
					source)
					.cascade(anything);
		}
		finally {
			persistenceContext.decrementCascadeLevel();
		}
	}

	/**
	 * Handles to calls needed to perform post-save cascades.
	 *
	 * @param source The session from which the event originated.
	 * @param persister The entity's persister instance.
	 * @param entity The entity beng saved.
	 * @param anything Generally cascade-specific data
	 */
	protected CompletionStage<Void> rxCascadeAfterSave(
			EventSource source,
			EntityPersister persister,
			Object entity,
			Object anything) {

		final PersistenceContext persistenceContext = source.getPersistenceContextInternal();
		// cascade-save to collections AFTER the collection owner was saved
		persistenceContext.incrementCascadeLevel();
		try {
			return new Cascade(getCascadeRxAction(),
					CascadePoint.AFTER_INSERT_BEFORE_DELETE,
					persister, entity, source)
					.cascade(anything);
		}
		finally {
			persistenceContext.decrementCascadeLevel();
		}
	}

	protected abstract CascadingAction getCascadeRxAction();

	//TO AVOID TOTAL COPY/PASTE JOB

	protected static final Helper helper = new Helper();

	public static class Helper extends AbstractSaveEventListener {
		@Override
		protected org.hibernate.engine.spi.CascadingAction getCascadeAction() {
			throw new UnsupportedOperationException();
		}

		@Override
		protected EntityState getEntityState(Object entity, String entityName, EntityEntry entry, SessionImplementor source) {
			return super.getEntityState(entity, entityName, entry, source);
		}

		@Override
		protected boolean visitCollectionsBeforeSave(Object entity, Serializable id, Object[] values, Type[] types, EventSource source) {
			return super.visitCollectionsBeforeSave(entity, id, values, types, source);
		}

		@Override
		protected boolean substituteValuesIfNecessary(Object entity, Serializable id, Object[] values, EntityPersister persister, SessionImplementor source) {
			return super.substituteValuesIfNecessary(entity, id, values, persister, source);
		}

		@Override
		protected String getLoggableName(String entityName, Object entity) {
			return super.getLoggableName(entityName, entity);
		}

		@Override
		protected Boolean getAssumedUnsaved() {
			//TODO: this is wrong for merge()
			return true;
		}
	};

}
