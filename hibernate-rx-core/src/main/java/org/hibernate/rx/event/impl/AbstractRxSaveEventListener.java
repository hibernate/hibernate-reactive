package org.hibernate.rx.event.impl;


import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.NonUniqueObjectException;
import org.hibernate.action.internal.AbstractEntityInsertAction;
import org.hibernate.engine.internal.CascadePoint;
import org.hibernate.engine.internal.Versioning;
import org.hibernate.engine.spi.*;
import org.hibernate.event.internal.WrapVisitor;
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
import org.hibernate.rx.engine.impl.RxEntityIdentityInsertAction;
import org.hibernate.rx.engine.impl.RxEntityRegularInsertAction;
import org.hibernate.rx.persister.entity.impl.RxEntityPersister;
import org.hibernate.rx.util.impl.RxUtil;
import org.hibernate.type.IntegerType;
import org.hibernate.type.LongType;
import org.hibernate.type.Type;
import org.hibernate.type.TypeHelper;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * Functionality common to persist and merge event listeners.
 *
 * @see DefaultRxPersistEventListener
 * @see DefaultRxPersistOnFlushEventListener
 * @see DefaultRxMergeEventListener
 */
abstract class AbstractRxSaveEventListener<C>
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
	 * @param context Generally cascade-specific information.
	 * @param source The session which is the source of this save event.
	 *
	 * @return The id used to save the entity.
	 */
	protected CompletionStage<Void> rxSaveWithRequestedId(
			Object entity,
			Serializable requestedId,
			String entityName,
			C context,
			EventSource source) {
		callbackRegistry.preCreate( entity );

		return rxPerformSave(
				entity,
				requestedId,
				source.getEntityPersister( entityName, entity ),
				false,
				context,
				source,
				true
		);
	}

	private static Serializable assignIdIfNecessary(Object generatedId, Object entity, String entityName, EventSource source) {
		EntityPersister persister = source.getEntityPersister(entityName, entity);
		if ( generatedId != null ) {
			if (generatedId instanceof Long) {
				Long longId = (Long) generatedId;
				Type identifierType = persister.getIdentifierType();
				if (identifierType == LongType.INSTANCE) {
					return longId;
				}
				else if (identifierType == IntegerType.INSTANCE) {
					return longId.intValue();
				}
				else {
					throw new HibernateException("cannot generate identifiers of type "
							+ identifierType.getReturnedClass().getSimpleName() + " for: " + entityName);
				}
			}
			else {
				return (Serializable) generatedId;
			}
		}
		else {
			Serializable assignedId = persister.getIdentifier( entity, source.getSession() );
			if (assignedId == null) {
				throw new IdentifierGenerationException("ids for this class must be manually assigned before calling save(): " + entityName);
			}
			return assignedId;
		}
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
	protected CompletionStage<Void> rxSaveWithGeneratedId(
			Object entity,
			String entityName,
			C context,
			EventSource source,
			boolean requiresImmediateIdAccess) {
		callbackRegistry.preCreate( entity );

		if ( entity instanceof SelfDirtinessTracker ) {
			( (SelfDirtinessTracker) entity ).$$_hibernate_clearDirtyAttributes();
		}

		EntityPersister persister = source.getEntityPersister( entityName, entity );
		boolean autoincrement = persister.isIdentifierAssignedByInsert();
		return ((RxEntityPersister) persister)
				.getRxIdentifierGenerator()
				.generate( source )
				.thenCompose( id ->
						rxPerformSave(
								entity,
								autoincrement ? null : assignIdIfNecessary( id, entity, entityName, source ),
								persister,
								autoincrement,
								context,
								source,
								!autoincrement || requiresImmediateIdAccess
						));
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
	protected CompletionStage<Void> rxPerformSave(
			Object entity,
			Serializable id,
			EntityPersister persister,
			boolean useIdentityColumn,
			C context,
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
				context,
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
	 * @param context Generally cascade-specific information.
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
			C context,
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

		return cascadeBeforeSave( source, persister, entity, context )
				.thenCompose(v -> {
					// We have to do this after cascadeBeforeSave completes,
					// since it could result in generation of parent ids,
					// which we will need as foreign keys in the insert

					Object[] values = persister.getPropertyValuesToInsert( entity, getMergeMap( context ), source );
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

					CompletionStage<AbstractEntityInsertAction> insert = addInsertAction(
							values, id, entity, persister, useIdentityColumn, source, shouldDelayIdentityInserts
					);

					EntityEntry newEntry = persistenceContext.getEntry( entity );

					if ( newEntry != original ) {
						EntityEntryExtraState extraState = newEntry.getExtraState( EntityEntryExtraState.class );
						if ( extraState == null ) {
							newEntry.addExtraState( original.getExtraState( EntityEntryExtraState.class ) );
						}
					}

					return insert;
				} )
				.thenCompose( vv -> cascadeAfterSave( source, persister, entity, context ) );

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

	protected Map<?,?> getMergeMap(Object context) {
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
			RxEntityIdentityInsertAction insert = new RxEntityIdentityInsertAction(
					values, entity, persister, false, source, shouldDelayIdentityInserts
			);
			return ( (RxSessionInternal) source ).getRxActionQueue()
					.addAction( insert )
					.thenApply(v -> insert );
		}
		else {
			Object version = Versioning.getVersion( values, persister );
			RxEntityRegularInsertAction insert = new RxEntityRegularInsertAction(
					id, values, entity, version, persister, false, source
			);
			return ( (RxSessionInternal) source ).getRxActionQueue()
					.addAction( insert )
					.thenApply(v -> insert );
		}
	}

	/**
	 * Handles the calls needed to perform pre-save cascades for the given entity.
	 *
	 * @param source The session from whcih the save event originated.
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
				getCascadeRxAction(),
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
				getCascadeRxAction(),
				CascadePoint.AFTER_INSERT_BEFORE_DELETE,
				persister, entity, context, source
		).cascade();
	}

	protected abstract CascadingAction<C> getCascadeRxAction();

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
			Serializable id,
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
			substitute = Versioning.seedVersion(
					values,
					persister.getVersionProperty(),
					persister.getVersionType(),
					source
			) || substitute;
		}
		return substitute;
	}

	protected boolean visitCollectionsBeforeSave(
			Object entity,
			Serializable id,
			Object[] values,
			Type[] types,
			EventSource source) {
		WrapVisitor visitor = new WrapVisitor( entity, id, source );
		// substitutes into values by side-effect
		visitor.processEntityPropertyValues( values, types );
		return visitor.isSubstitutionRequired();
	}

}
