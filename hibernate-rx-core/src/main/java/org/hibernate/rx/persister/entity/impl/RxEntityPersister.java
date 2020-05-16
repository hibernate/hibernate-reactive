package org.hibernate.rx.persister.entity.impl;

import org.hibernate.HibernateException;
import org.hibernate.LockOptions;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.MultiLoadOptions;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.rx.loader.entity.impl.RxAbstractEntityLoader;
import org.jboss.logging.Logger;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * A reactive {@link EntityPersister}. Supports non-blocking
 * insert/update/delete operations.
 *
 * @see RxAbstractEntityPersister
 */
public interface RxEntityPersister extends EntityPersister {
	Logger log = Logger.getLogger( RxEntityPersister.class );

	//TODO: we only support Long for now, but eventually
	//      we need to do something more general
	RxIdentifierGenerator<?> getRxIdentifierGenerator();
	
	/**
	 * Insert the given instance state without blocking.
	 *
	 * @see EntityPersister#insert(Serializable, Object[], Object, SharedSessionContractImplementor)
	 */
	CompletionStage<?> insertRx(
			Serializable id,
			Object[] fields,
			Object object,
			SharedSessionContractImplementor session);

	/**
	 * Insert the given instance state without blocking.
	 *
	 * @see EntityPersister#insert(Object[], Object, SharedSessionContractImplementor)
	 */
	CompletionStage<Serializable> insertRx(
			Object[] fields,
			Object object,
			SharedSessionContractImplementor session);

	/**
	 * Delete the given instance without blocking.
	 *
	 * @see EntityPersister#delete(Serializable, Object, Object, SharedSessionContractImplementor) 
	 */
	CompletionStage<?> deleteRx(
			Serializable id,
			Object version,
			Object object,
			SharedSessionContractImplementor session)
					throws HibernateException;

	/**
	 * Update the given instance state without blocking.
	 *
	 * @see EntityPersister#update(Serializable, Object[], int[], boolean, Object[], Object, Object, Object, SharedSessionContractImplementor)
	 */
	CompletionStage<?> updateRx(
			Serializable id,
			Object[] fields, int[] dirtyFields,
			boolean hasDirtyCollection,
			Object[] oldFields, Object oldVersion,
			Object object,
			Object rowId,
			SharedSessionContractImplementor session)
					throws HibernateException;

	CompletionStage<List<Object>> rxMultiLoad(
	 		Serializable[] ids,
			SessionImplementor session,
			MultiLoadOptions loadOptions);

	default CompletionStage<Object> rxLoad(Serializable id, Object optionalObject, LockOptions lockOptions, SharedSessionContractImplementor session) {
		return rxLoad( id, optionalObject, lockOptions, session, null );
	}

	default CompletionStage<Object> rxLoad(Serializable id, Object optionalObject, LockOptions lockOptions, SharedSessionContractImplementor session, Boolean readOnly) {
		if ( log.isTraceEnabled() ) {
			log.tracev( "Fetching entity: {0}", MessageHelper.infoString( this, id, getFactory() ) );
		}

		return getAppropriateLoader( lockOptions, session ).load( id, optionalObject, session, lockOptions, readOnly );
	}

	RxAbstractEntityLoader getAppropriateLoader(LockOptions lockOptions, SharedSessionContractImplementor session);

	CompletionStage<Boolean> rxIsTransient(Object entity, SessionImplementor session);
}
