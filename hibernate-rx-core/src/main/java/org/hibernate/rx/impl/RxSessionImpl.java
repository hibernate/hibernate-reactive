package org.hibernate.rx.impl;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.rx.RxQuery;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.RxSessionFactory;
import org.hibernate.rx.RxSessionInternal;
import org.hibernate.rx.engine.spi.RxSessionFactoryImplementor;

import javax.persistence.LockModeType;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Implementats the RxSession API. This delegating class is needed to
 * avoid name clashes when implementing both RxSession and Session.
 */
public class RxSessionImpl implements RxSession {

	private final RxSessionFactory factory;
	private final RxSessionInternal delegate;

	public RxSessionImpl(RxSessionFactoryImplementor factory, RxSessionInternal session) {
		this.factory = factory;
		this.delegate = session;
	}

	@Override
	public CompletionStage<RxSession> flush() {
//		checkOpen();
		return delegate.rxFlush().thenApply( v-> this );
	}

	@Override
	public <T> CompletionStage<Optional<T>> fetch(T association) {
		return delegate.rxFetch(association);
	}

	@Override
	public <T> T getReference(Class<T> entityClass, Object id) {
		//it's important that this method does not hit the database!
		//TODO: how can we guarantee that?
		return delegate.getReference( entityClass, id );
	}

	@Override
	public <T> CompletionStage<Optional<T>> find(Class<T> entityClass, Object primaryKey) {
		return delegate.rxFind( entityClass, primaryKey, null, null );
	}

	public <T> CompletionStage<Optional<T>> find(
			Class<T> entityClass,
			Object primaryKey,
			Map<String, Object> properties) {
		return delegate.rxFind( entityClass, primaryKey, null, properties );
	}

	public <T> CompletionStage<Optional<T>> find(Class<T> entityClass, Object primaryKey, LockModeType lockModeType) {
		return delegate.rxFind( entityClass, primaryKey, lockModeType, null );
	}

	public <T> CompletionStage<Optional<T>> find(
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
		return delegate.rxRemove( entity ).thenApply(v-> this );
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

	@Override
	public void close() {
		delegate.close();
	}

}
