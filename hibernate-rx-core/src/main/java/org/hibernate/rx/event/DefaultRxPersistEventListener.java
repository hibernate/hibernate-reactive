package org.hibernate.rx.event;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.NonUniqueObjectException;
import org.hibernate.action.internal.AbstractEntityInsertAction;
import org.hibernate.action.internal.EntityIdentityInsertAction;
import org.hibernate.engine.internal.Versioning;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityEntryExtraState;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.SelfDirtinessTracker;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.internal.DefaultPersistEventListener;
import org.hibernate.event.service.spi.DuplicationStrategy;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.PersistEvent;
import org.hibernate.event.spi.PersistEventListener;
import org.hibernate.id.IdentifierGenerationException;
import org.hibernate.id.IdentifierGeneratorHelper;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.jpa.event.spi.CallbackRegistry;

import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.rx.RxHibernateSession;
import org.hibernate.rx.engine.impl.RxEntityInsertAction;

import org.hibernate.rx.engine.spi.RxActionQueue;
import org.hibernate.type.Type;
import org.hibernate.type.TypeHelper;

public class DefaultRxPersistEventListener extends DefaultPersistEventListener {
	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( DefaultRxPersistEventListener.class );

	private CallbackRegistry callbackRegistry;

	@Override
	public void injectCallbackRegistry(CallbackRegistry callbackRegistry) {
		super.injectCallbackRegistry( callbackRegistry );
		this.callbackRegistry = callbackRegistry;
	}

	@Override
	protected void entityIsTransient(PersistEvent event, Map createCache) {
		LOG.trace( "Saving transient instance" );
		final EventSource source = event.getSession();
		RxPersistEvent rxEvent = (RxPersistEvent) event;
		final Object entity = source.getPersistenceContextInternal().unproxy(event.getObject());

		RxPersistEvent rxPersistEvent = (RxPersistEvent) event;
		CompletionStage<Void> stage = rxPersistEvent.getStage();
		if ( createCache.put( entity, entity ) == null ) {
			saveWithGeneratedId( entity, event.getEntityName(), createCache, source, false, stage );
		}
	}

	protected void saveWithGeneratedId(
			Object entity,
			String entityName,
			Object anything,
			EventSource source,
			boolean requiresImmediateIdAccess,
			CompletionStage<Void> insertStage) {
		callbackRegistry.preCreate( entity );

		if ( entity instanceof SelfDirtinessTracker ) {
			( (SelfDirtinessTracker) entity ).$$_hibernate_clearDirtyAttributes();
		}

		EntityPersister persister = source.getEntityPersister( entityName, entity );
		Serializable generatedId = persister.getIdentifierGenerator().generate( source, entity );
		if ( generatedId == null ) {
			throw new IdentifierGenerationException( "null id generated for:" + entity.getClass() );
		}
		else if ( generatedId == IdentifierGeneratorHelper.SHORT_CIRCUIT_INDICATOR ) {
			// FIXME: CAn I delete this?
			source.getIdentifier( entity );
		}
		else if ( generatedId == IdentifierGeneratorHelper.POST_INSERT_INDICATOR ) {
			performSave( entity, null, persister, true, anything, source, requiresImmediateIdAccess, insertStage );
		}
		else {
			// TODO: define toString()s for generators
			if ( LOG.isDebugEnabled() ) {
				LOG.debugf(
						"Generated identifier: %s, using strategy: %s",
						persister.getIdentifierType().toLoggableString( generatedId, source.getFactory() ),
						persister.getIdentifierGenerator().getClass().getName()
				);
			}

			performSave( entity, generatedId, persister, false, anything, source, true, insertStage );
		}
	}

	protected Serializable performSave(
			Object entity,
			Serializable id,
			EntityPersister persister,
			boolean useIdentityColumn,
			Object anything,
			EventSource source,
			boolean requiresImmediateIdAccess,
			CompletionStage<Void> insertStage) {

		if ( LOG.isTraceEnabled() ) {
			LOG.tracev( "Saving {0}", MessageHelper.infoString( persister, id, source.getFactory() ) );
		}

		final EntityKey key;
		if ( !useIdentityColumn ) {
			key = source.generateEntityKey( id, persister );
			Object old = source.getPersistenceContext().getEntity( key );
			if ( old != null ) {
				if ( source.getPersistenceContext().getEntry( old ).getStatus() == Status.DELETED ) {
					source.forceFlush( source.getPersistenceContext().getEntry( old ) );
				}
				else {
					throw new NonUniqueObjectException( id, persister.getEntityName() );
				}
			}
			persister.setIdentifier( entity, id, source );
		}
		else {
			key = null;
		}

		if ( invokeSaveLifecycle( entity, persister, source ) ) {
			return id; //EARLY EXIT
		}

		return performSaveOrReplicate(
				entity,
				key,
				persister,
				useIdentityColumn,
				anything,
				source,
				requiresImmediateIdAccess,
				insertStage
		);
	}

	protected Serializable performSaveOrReplicate(
			Object entity,
			EntityKey key,
			EntityPersister persister,
			boolean useIdentityColumn,
			Object anything,
			EventSource source,
			boolean requiresImmediateIdAccess,
			CompletionStage<Void> insertStage) {

		Serializable id = key == null ? null : key.getIdentifier();

		boolean inTrx = source.isTransactionInProgress();
		boolean shouldDelayIdentityInserts = !inTrx && !requiresImmediateIdAccess;

		// Put a placeholder in entries, so we don't recurse back and try to save() the
		// same object again. QUESTION: should this be done before onSave() is called?
		// likewise, should it be done before onUpdate()?

		// todo (6.0) : Should we do something here like `org.hibernate.sql.results.spi.JdbcValuesSourceProcessingState#registerLoadingEntity` ?
		EntityEntry original = source.getPersistenceContext().addEntry(
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

//		final List<Navigable> navigables = entityDescriptor.getNavigables();

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

		AbstractEntityInsertAction insert = addInsertAction(
				values, id, entity, persister, useIdentityColumn, source, shouldDelayIdentityInserts, insertStage
		);

		// postpone initializing id in case the insert has non-nullable transient dependencies
		// that are not resolved until cascadeAfterSave() is executed
		cascadeAfterSave( source, persister, entity, anything );
		if ( useIdentityColumn && insert.isEarlyInsert() ) {
			if ( !EntityIdentityInsertAction.class.isInstance( insert ) ) {
				throw new IllegalStateException(
						"Insert should be using an identity column, but action is of unexpected type: " +
								insert.getClass().getName()
				);
			}
			id = ((EntityIdentityInsertAction) insert).getGeneratedId();

			insert.handleNaturalIdPostSaveNotifications( id );
		}

		EntityEntry newEntry = source.getPersistenceContext().getEntry( entity );

		if ( newEntry != original ) {
			EntityEntryExtraState extraState = newEntry.getExtraState( EntityEntryExtraState.class );
			if ( extraState == null ) {
				newEntry.addExtraState( original.getExtraState( EntityEntryExtraState.class ) );
			}
		}

		return id;
	}

	private static boolean isPartOfTransaction(EventSource source) {
		return source.isTransactionInProgress() && source.getTransactionCoordinator().isJoined();
	}

	private AbstractEntityInsertAction addInsertAction(
			Object[] values,
			Serializable id,
			Object entity,
			EntityPersister persister,
			boolean useIdentityColumn,
			EventSource source,
			boolean shouldDelayIdentityInserts,
			CompletionStage<Void> insertStage) {
//		AbstractEntityInsertAction insertAction = null;
//		if ( useIdentityColumn ) {
//			insertAction = new RxEntityIdentityInsertAction(
//					values,
//					entity,
//					descriptor,
//					isVersionIncrementDisabled(),
//					source,
//					shouldDelayIdentityInserts,
//					null // Stage
//			);
//		}
//		else {
			Object version = Versioning.getVersion( values, persister );
			RxEntityInsertAction insertAction = new RxEntityInsertAction(
					id,
					values,
					entity,
					version,
					persister,
					isVersionIncrementDisabled(),
					source,
					insertStage
			);
//		}
		RxActionQueue rxActionQueue = ( (RxHibernateSession) source ).getRxActionQueue();
		rxActionQueue.addAction( insertAction );
		return insertAction;

	}
	@Override
	public void onPersist(PersistEvent event, Map createdAlready) throws HibernateException {
		super.onPersist( event, createdAlready );
	}

	public static class EventContextManagingPersistEventListenerDuplicationStrategy implements DuplicationStrategy {

		public static final DuplicationStrategy INSTANCE = new EventContextManagingPersistEventListenerDuplicationStrategy();

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
