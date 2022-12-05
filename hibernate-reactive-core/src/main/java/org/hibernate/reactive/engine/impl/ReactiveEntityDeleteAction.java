/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

import java.util.concurrent.CompletionStage;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.action.internal.EntityDeleteAction;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cfg.NotYetImplementedException;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.engine.ReactiveExecutable;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.stat.spi.StatisticsImplementor;

/**
 * A reactific {@link EntityDeleteAction}.
 */
public class ReactiveEntityDeleteAction extends EntityDeleteAction implements ReactiveExecutable {

	public ReactiveEntityDeleteAction(
			Object id,
			Object[] state,
			Object version,
			Object instance,
			EntityPersister persister,
			boolean isCascadeDeleteEnabled,
			EventSource session) {
		super( id, state, version, instance, persister, isCascadeDeleteEnabled, session );
	}

	@Override
	public void execute() throws HibernateException {
		throw new NotYetImplementedException();
	}

	@Override
	public CompletionStage<Void> reactiveExecute() throws HibernateException {
		final Object id = getId();
		final EntityPersister persister = getPersister();
		final SharedSessionContractImplementor session = getSession();
		final Object instance = getInstance();

		final boolean veto = preDelete();

		Object version = getVersion();
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
			setLock( cache.lockItem( session, ck, version ) );
		}
		else {
			ck = null;
		}

		CompletionStage<Void> deleteStep = !isCascadeDeleteEnabled() && !veto
				? ( (ReactiveEntityPersister) persister ).deleteReactive( id, version, instance, session )
				: voidFuture();

		return deleteStep.thenAccept( v -> {
			//postDelete:
			// After actually deleting a row, record the fact that the instance no longer
			// exists on the database (needed for identity-column key generation), and
			// remove it from the session cache
			final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
			final EntityEntry entry = persistenceContext.removeEntry( instance );
			if ( entry == null ) {
				throw new AssertionFailure( "possible non-threadsafe access to session" );
			}
			entry.postDelete();

			persistenceContext.removeEntity( entry.getEntityKey() );
			persistenceContext.removeProxy( entry.getEntityKey() );

			if ( persister.canWriteToCache() ) {
				persister.getCacheAccessStrategy().remove( session, ck );
			}

			persistenceContext.getNaturalIdResolutions()
					.removeSharedResolution( id, getNaturalIdValues(), persister );

			postDelete();

			final StatisticsImplementor statistics = getSession().getFactory().getStatistics();
			if ( statistics.isStatisticsEnabled() && !veto ) {
				statistics.deleteEntity( getPersister().getEntityName() );
			}
		} );
	}

}
