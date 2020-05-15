/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.rx.engine.impl;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.action.internal.EntityInsertAction;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.entry.CacheEntry;
import org.hibernate.engine.internal.Versioning;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.rx.persister.entity.impl.RxEntityPersister;
import org.hibernate.rx.util.impl.RxUtil;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

/**
 * A reactific {@link EntityInsertAction}.
 */
public class RxEntityRegularInsertAction extends EntityInsertAction implements RxEntityInsertAction {

	private final boolean isVersionIncrementDisabled;
	private boolean executed;
	private boolean transientReferencesNullified;

	public RxEntityRegularInsertAction(
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
	public CompletionStage<Void> rxExecute() throws HibernateException {

		return rxNullifyTransientReferencesIfNotAlready().thenCompose( v-> {

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
				insertStage = ((RxEntityPersister) persister)
						.insertRx( id, getState(), instance, session )
						.thenApply( res -> {
							PersistenceContext persistenceContext = session.getPersistenceContext();
							final EntityEntry entry = persistenceContext.getEntry( instance );
							if ( entry == null ) {
								throw new AssertionFailure( "possible non-threadsafe access to session" );
							}

							entry.postInsert( getState() );

							if ( persister.hasInsertGeneratedProperties() ) {
								persister.processInsertGeneratedProperties( id, instance, getState(), session );
								if ( persister.isVersionPropertyGenerated() ) {
									setVersion( Versioning.getVersion( getState(), persister ) );
								}
								entry.postUpdate( instance, getState(), getVersion() );
							}

							persistenceContext.registerInsertedKey( persister, getId() );
							return null;
						} );
			}
			else {
				insertStage = RxUtil.nullFuture();
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
