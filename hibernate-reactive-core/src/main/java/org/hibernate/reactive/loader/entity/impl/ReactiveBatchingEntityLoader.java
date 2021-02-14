/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.entity.impl;

import org.hibernate.LockOptions;
import org.hibernate.engine.internal.BatchFetchQueueHelper;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.entity.BatchingEntityLoaderBuilder;
import org.hibernate.loader.entity.UniqueEntityLoader;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.reactive.loader.entity.ReactiveUniqueEntityLoader;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static org.hibernate.pretty.MessageHelper.infoString;
import static org.hibernate.reactive.util.impl.CompletionStages.logSqlException;
import static org.hibernate.reactive.util.impl.CompletionStages.returnOrRethrow;

/**
 * The base contract for loaders capable of performing batch-fetch loading of entities using multiple primary key
 * values in the SQL <tt>WHERE</tt> clause.
 *
 * @author Gavin King
 * @author Steve Ebersole
 *
 * @see BatchingEntityLoaderBuilder
 * @see UniqueEntityLoader
 */
public abstract class ReactiveBatchingEntityLoader implements ReactiveUniqueEntityLoader {

	private final EntityPersister persister;

	public ReactiveBatchingEntityLoader(OuterJoinLoadable persister) {
		this.persister = persister;
	}

	public EntityPersister persister() {
		return persister;
	}

	@Override
	public CompletionStage<Object> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session) {
		return load( id, optionalObject, session, LockOptions.NONE );
	}

	@Override
	public CompletionStage<Object> load(
			Serializable id,
			Object optionalObject,
			SharedSessionContractImplementor session,
			LockOptions lockOptions,
			Boolean readOnly) {
		return load( id, optionalObject, session, lockOptions, readOnly );
	}

	@Override
	public CompletionStage<Object> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session, Boolean readOnly) {
		return load( id, optionalObject, session, LockOptions.NONE, readOnly );
	}

	@Override
	public CompletionStage<Object> load(Object id, SharedSessionContractImplementor session, LockOptions lockOptions) {
		throw new UnsupportedOperationException();
	}

	protected QueryParameters buildQueryParameters(
			Serializable id,
			Serializable[] ids,
			Object optionalObject,
			LockOptions lockOptions,
			Boolean readOnly) {
		Type[] types = new Type[ids.length];
		Arrays.fill( types, persister().getIdentifierType() );

		QueryParameters qp = new QueryParameters();
		qp.setPositionalParameterTypes( types );
		qp.setPositionalParameterValues( ids );
		qp.setOptionalObject( optionalObject );
		qp.setOptionalEntityName( persister().getEntityName() );
		qp.setOptionalId( id );
		qp.setLockOptions( lockOptions );
		if ( readOnly != null ) {
			qp.setReadOnly( readOnly );
		}
		return qp;
	}

	protected Object getObjectFromList(List<?> results, Serializable id, SharedSessionContractImplementor session) {
		for ( Object obj : results ) {
			final boolean equal = persister.getIdentifierType().isEqual(
					id,
					session.getContextEntityIdentifier( obj ),
					session.getFactory()
			);
			if ( equal ) {
				return obj;
			}
		}
		return null;
	}

	protected CompletionStage<Object> doBatchLoad(
			Serializable id,
			ReactiveEntityLoader loaderToUse,
			SharedSessionContractImplementor session,
			Serializable[] ids,
			Object optionalObject,
			LockOptions lockOptions,
			Boolean readOnly) {
//			if ( log.isDebugEnabled() ) {
//				log.debugf( "Batch loading entity: %s", infoString( persister, ids, session.getFactory() ) );
//			}

		QueryParameters parameters = buildQueryParameters(id, ids, optionalObject, lockOptions, readOnly);
		return loaderToUse.doReactiveQueryAndInitializeNonLazyCollections( (SessionImplementor) session, parameters, false )
				.handle( (list, err) -> {
//						log.debug( "Done entity batch load" );
					// The EntityKey for any entity that is not found will remain in the batch.
					// Explicitly remove the EntityKeys for entities that were not found to
					// avoid including them in future batches that get executed.
					Object result = getObjectFromList(list, id, session);

					BatchFetchQueueHelper.removeNotFoundBatchLoadableEntityKeys(
							ids,
							list,
							persister(),
							session
					);
					logSqlException( err,
							() -> "could not load an entity batch: "
									+ infoString( persister(), ids, session.getFactory() ),
							loaderToUse.getSQLString()
					);
					return returnOrRethrow( err, result );
				} );
	}

}
