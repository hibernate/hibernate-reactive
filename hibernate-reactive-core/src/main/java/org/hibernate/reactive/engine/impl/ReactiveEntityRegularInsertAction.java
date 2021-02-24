/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.action.internal.EntityInsertAction;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.entry.CacheEntry;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * A reactific {@link EntityInsertAction}.
 */
public class ReactiveEntityRegularInsertAction extends EntityInsertAction implements ReactiveEntityInsertAction {

	private final boolean isVersionIncrementDisabled;
	private boolean executed;
	private boolean transientReferencesNullified;

	public ReactiveEntityRegularInsertAction(
			Serializable id,
			Object[] state,
			Object instance,
			Object version,
			EntityPersister persister,
			boolean isVersionIncrementDisabled,
			SharedSessionContractImplementor session) {
		super( id, state, instance, version, persister, isVersionIncrementDisabled, session );
		this.isVersionIncrementDisabled = isVersionIncrementDisabled;
	}

	@Override
	public void execute() throws HibernateException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletionStage<Void> reactiveExecute() throws HibernateException {

		return reactiveNullifyTransientReferencesIfNotAlready().thenCompose( v-> {

			EntityPersister persister = getPersister();
			final SharedSessionContractImplementor session = getSession();
			final Object instance = getInstance();
			final Serializable id = getId();

			// FIXME: It needs to become async
			final boolean veto = preInsert();

			// Don't need to lock the cache here, since if someone
			// else inserted the same pk first, the insert would fail
			CompletionStage<Void> insertStage;
			if ( !veto ) {
				ReactiveEntityPersister reactivePersister = (ReactiveEntityPersister) persister;
				insertStage = reactivePersister.insertReactive( id, getState(), instance, session )
						.thenApply( res -> {
							EntityEntry entry = session.getPersistenceContext().getEntry( instance );
							if ( entry == null ) {
								throw new AssertionFailure( "possible non-threadsafe access to session" );
							}
							entry.postInsert( getState() );
							return entry;
						} )
						.thenCompose( entry -> processInsertGeneratedProperties( reactivePersister, session, instance, id, entry )
								.thenAccept( vv -> session.getPersistenceContext().registerInsertedKey( persister, getId() ) ) );
			}
			else {
				insertStage = voidFuture();
			}

			return insertStage.thenApply( res -> {
				final SessionFactoryImplementor factory = session.getFactory();

				if ( isCachePutEnabled( persister, session ) ) {
					final CacheEntry ce = persister.buildCacheEntry(
							instance,
							getState(),
							getVersion(),
							session
					);
					setCacheEntry( persister.getCacheEntryStructure().structure( ce ) );
					final EntityDataAccess cache = persister.getCacheAccessStrategy();
					final Object ck = cache.generateCacheKey( id, persister, factory, session.getTenantIdentifier() );

					final boolean put = cacheInsert( persister, ck );

					if ( put && factory.getStatistics().isStatisticsEnabled() ) {
						factory.getStatistics().entityCachePut(
								persister.getNavigableRole(),
								persister.getCacheAccessStrategy().getRegion().getName()
						);
					}
				}

				handleNaturalIdPostSaveNotifications( id );

				postInsert();

				if ( factory.getStatistics().isStatisticsEnabled() && !veto ) {
					factory.getStatistics().insertEntity( getEntityName() );
				}

				markExecuted();
				return null;
			} );
		} );
	}

	private CompletionStage<Void> processInsertGeneratedProperties(
			ReactiveEntityPersister persister,
			SharedSessionContractImplementor session,
			Object instance,
			Serializable id,
			EntityEntry entry) {
		if ( persister.hasInsertGeneratedProperties() ) {
			if ( persister.isVersionPropertyGenerated() ) {
				throw new UnsupportedOperationException( "generated version attribute not supported in Hibernate Reactive" );
//				setVersion( Versioning.getVersion( getState(), persister ) );
			}
			return persister.reactiveProcessInsertGenerated( id, instance, getState(), session )
					.thenAccept( v -> entry.postUpdate( instance, getState(), getVersion() ) );

		}
		return voidFuture();
	}

	@Override
	public EntityKey getEntityKey() {
		return super.getEntityKey();
	}

	@Override
	protected void markExecuted() {
		super.markExecuted();
		executed = true;
	}

	@Override
	public boolean isExecuted() {
		return executed;
	}

	@Override
	public boolean isVersionIncrementDisabled() {
		return isVersionIncrementDisabled;
	}

	@Override
	public boolean areTransientReferencesNullified() {
		return transientReferencesNullified;
	}

	@Override
	public void setTransientReferencesNullified() {
		transientReferencesNullified = true;
	}
}
