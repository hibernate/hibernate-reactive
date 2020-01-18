package org.hibernate.rx.event.impl;


import org.hibernate.LockMode;
import org.hibernate.NonUniqueObjectException;
import org.hibernate.action.internal.AbstractEntityInsertAction;
import org.hibernate.action.internal.EntityIdentityInsertAction;
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
import org.hibernate.rx.engine.impl.RxEntityInsertAction;
import org.hibernate.rx.persister.entity.impl.RxEntityPersister;
import org.hibernate.rx.util.impl.RxUtil;
import org.hibernate.type.Type;
import org.hibernate.type.TypeHelper;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

/**
 * A convenience base class for listeners responding to save events.
 *
 * @author Steve Ebersole.
 */
abstract class AbstractRxSaveEventListener
		extends AbstractSaveEventListener
		implements CallbackRegistryConsumer {
	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( AbstractRxSaveEventListener.class );
	private CallbackRegistry callbackRegistry;

	public void injectCallbackRegistry(CallbackRegistry callbackRegistry) {
		super.injectCallbackRegistry(callbackRegistry);
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
	protected CompletionStage<Serializable> rxSaveWithRequestedId(
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
	protected CompletionStage<Serializable> rxSaveWithGeneratedId(
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
		return RxEntityPersister.get(persister).getIdentifierGenerator()
				.generate( source.getFactory() )
				.thenCompose( generatedId -> {
					Serializable id;
					if ( !generatedId.isPresent() ) {
						id = persister.getIdentifier( entity, source.getSession() );
						if (id == null) {
							return RxUtil.failedFuture(new IdentifierGenerationException("ids for this class must be manually assigned before calling save(): " + entityName));
						}
					}
					else {
						id = generatedId.get();
					}
					return rxPerformSave(entity, id, persister, false, anything, source, true);
				});
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
	protected CompletionStage<Serializable> rxPerformSave(
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

		if ( invokeSaveLifecycle( entity, persister, source ) ) {
			return RxUtil.completedFuture( id ); //EARLY EXIT
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
	protected CompletionStage<Serializable> rxPerformSaveOrReplicate(
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

		cascadeBeforeSave( source, persister, entity, anything );

		Object[] values = persister.getPropertyValuesToInsert( entity, getMergeMap( anything ), source );
		Type[] types = persister.getPropertyTypes();

		boolean substitute = substituteValuesIfNecessary( entity, id, values, persister, source );

		if ( persister.hasCollections() ) {
			substitute = substitute || visitCollectionsBeforeSave( entity, id, values, types, source );
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

		CompletionStage<AbstractEntityInsertAction> insertCs = addInsertAction(
				values, id, entity, persister, useIdentityColumn, source, shouldDelayIdentityInserts
		);
		return insertCs.thenApply( insert -> {
			// postpone initializing id in case the insert has non-nullable transient dependencies
			// that are not resolved until cascadeAfterSave() is executed
			cascadeAfterSave( source, persister, entity, anything );
			Serializable newId = id;
			if ( useIdentityColumn && insert.isEarlyInsert() ) {
				if ( !EntityIdentityInsertAction.class.isInstance( insert ) ) {
					throw new IllegalStateException(
							"Insert should be using an identity column, but action is of unexpected type: " +
									insert.getClass().getName()
					);
				}
				newId = ( (EntityIdentityInsertAction) insert ).getGeneratedId();

				insert.handleNaturalIdPostSaveNotifications( newId );
			}

			EntityEntry newEntry = persistenceContext.getEntry( entity );

			if ( newEntry != original ) {
				EntityEntryExtraState extraState = newEntry.getExtraState( EntityEntryExtraState.class );
				if ( extraState == null ) {
					newEntry.addExtraState( original.getExtraState( EntityEntryExtraState.class ) );
				}
			}

			return newId;
		} );
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
					values, entity, persister, isVersionIncrementDisabled(), source, shouldDelayIdentityInserts
			);
			return ( (RxSessionInternal) source ).getRxActionQueue().addAction( insert ).thenApply(v -> insert );
		}
		else {
			Object version = Versioning.getVersion( values, persister );
			RxEntityInsertAction insert = new RxEntityInsertAction(
					id, values, entity, version, persister, isVersionIncrementDisabled(), source
			);
			return ( (RxSessionInternal) source ).getRxActionQueue().addAction( insert ).thenApply(v -> insert );
		}
	}

}
