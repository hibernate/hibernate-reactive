package org.hibernate.reactive.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import javax.persistence.LockModeType;

import org.hibernate.CacheMode;
import org.hibernate.Filter;
import org.hibernate.FlushMode;
import org.hibernate.reactive.stage.RxQuery;
import org.hibernate.reactive.stage.RxSession;
import org.hibernate.reactive.internal.RxSessionInternal;
import org.hibernate.reactive.query.impl.RxQueryImpl;

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
		return delegate.rxFetch(association, false);
	}

	@Override
	public <T> CompletionStage<T> unproxy(T association) {
		return delegate.rxFetch(association, true);
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
	public <R> RxQuery<R> createQuery(String jpql, Class<R> resultType) {
		return new RxQueryImpl<>( delegate.createRxQuery( jpql, resultType ) );
	}

	@Override
	public <R> RxQuery<R> createQuery(String jpql) {
		return new RxQueryImpl<>( delegate.createRxQuery( jpql ) );
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

	@Override
	public RxSession setDefaultReadOnly(boolean readOnly) {
		delegate.setDefaultReadOnly(readOnly);
		return this;
	}

	@Override
	public RxSession setReadOnly(Object entityOrProxy, boolean readOnly) {
		delegate.setReadOnly(entityOrProxy, readOnly);
		return this;
	}

	@Override
	public boolean isReadOnly(Object entityOrProxy) {
		return delegate.isReadOnly(entityOrProxy);
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

	@Override
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
	}

	@Override
	public void close() {
		delegate.close();
	}

}
