package org.hibernate.rx.impl;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.rx.RxQuery;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.RxSessionInternal;

import javax.persistence.LockModeType;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * Implements the {@link RxSession} API. This delegating class is
 * needed to avoid name clashes when implementing both
 * {@code RxSession} and {@link org.hibernate.Session}.
 */
public class RxSessionImpl implements RxSession {

	private final RxSessionInternal delegate;

	public RxSessionImpl(RxSessionInternal session) {
		this.delegate = session;
	}

	@Override
	public CompletionStage<RxSession> flush() {
//		checkOpen();
		return delegate.rxFlush().thenApply( v-> this );
	}

	@Override
	public <T> CompletionStage<T> fetch(T association) {
		return delegate.rxFetch(association);
	}

	@Override
	public <T> T getReference(Class<T> entityClass, Object id) {
		//it's important that this method does not hit the database!
		//TODO: how can we guarantee that?
		return delegate.getReference( entityClass, id );
	}

	@Override
	public <T> CompletionStage<T> find(Class<T> entityClass, Object primaryKey) {
		return delegate.rxFind( entityClass, primaryKey, null, null );
	}

	@Override
	public <T> CompletionStage<List<T>> find(Class<T> entityClass, Object... ids) {
		return delegate.rxFind( entityClass, ids );
	}

	public <T> CompletionStage<T> find(
			Class<T> entityClass,
			Object primaryKey,
			Map<String, Object> properties) {
		return delegate.rxFind( entityClass, primaryKey, null, properties );
	}

	public <T> CompletionStage<T> find(Class<T> entityClass, Object primaryKey, LockModeType lockModeType) {
		return delegate.rxFind( entityClass, primaryKey, lockModeType, null );
	}

	public <T> CompletionStage<T> find(
			Class<T> entityClass,
			Object primaryKey,
			LockModeType lockModeType,
			Map<String, Object> properties) {
		return delegate.rxFind(entityClass, primaryKey, lockModeType, properties);
	}

	@Override
	public CompletionStage<RxSession> persist(Object entity) {
		return delegate.rxPersist( entity ).thenApply( v-> this );
	}

	@Override
	public CompletionStage<RxSession> remove(Object entity) {
		return delegate.rxRemove( entity ).thenApply( v-> this );
	}

	@Override
	public <T> CompletionStage<T> merge(T entity) {
		return delegate.rxMerge( entity );
	}

	@Override
	public CompletionStage<RxSession> refresh(Object entity) {
		return delegate.rxRefresh( entity ).thenApply( v-> this );
	}

	@Override
	public <R> RxQuery<R> createQuery(Class<R> resultType, String jpql) {
		return null;
	}

	@Override
	public FlushMode getFlushMode() {
		switch ( delegate.getHibernateFlushMode() ) {
			case MANUAL:
				return FlushMode.MANUAL;
			case COMMIT:
				return FlushMode.COMMIT;
			case AUTO:
				return FlushMode.AUTO;
			case ALWAYS:
				return FlushMode.ALWAYS;
			default:
				throw new IllegalStateException("impossible flush mode");
		}
	}

	@Override
	public RxSession setFlushMode(FlushMode flushMode) {
		switch (flushMode) {
			case COMMIT:
				delegate.setHibernateFlushMode(org.hibernate.FlushMode.COMMIT);
				break;
			case AUTO:
				delegate.setHibernateFlushMode(org.hibernate.FlushMode.AUTO);
				break;
			case MANUAL:
				delegate.setHibernateFlushMode(org.hibernate.FlushMode.MANUAL);
				break;
			case ALWAYS:
				delegate.setHibernateFlushMode(org.hibernate.FlushMode.ALWAYS);
				break;
		}
		return this;
	}

	public CacheMode getCacheMode() {
		return delegate.getCacheMode();
	}

	public RxSession setCacheMode(CacheMode cacheMode) {
		delegate.setCacheMode(cacheMode);
		return this;
	}

	@Override
	public RxSession detach(Object entity) {
		delegate.detach(entity);
		return this;
	}

	@Override
	public RxSession clear() {
		delegate.clear();
		return this;
	}

	@Override
	public RxSession enableFetchProfile(String name) {
		delegate.enableFetchProfile(name);
		return this;
	}

	@Override
	public RxSession disableFetchProfile(String name) {
		delegate.disableFetchProfile(name);
		return this;
	}

	@Override
	public boolean isFetchProfileEnabled(String name) {
		return delegate.isFetchProfileEnabled(name);
	}

	//Filters can't be tested until we have HQL
	/*@Override
	public Filter enableFilter(String filterName) {
		return delegate.enableFilter(filterName);
	}

	@Override
	public void disableFilter(String filterName) {
		delegate.disableFilter(filterName);
	}

	@Override
	public Filter getEnabledFilter(String filterName) {
		return delegate.getEnabledFilter(filterName);
	}*/

	@Override
	public void close() {
		delegate.close();
	}

}
