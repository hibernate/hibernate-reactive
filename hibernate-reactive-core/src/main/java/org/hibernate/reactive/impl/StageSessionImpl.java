package org.hibernate.reactive.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import javax.persistence.LockModeType;

import org.hibernate.CacheMode;
import org.hibernate.Filter;
import org.hibernate.FlushMode;
import org.hibernate.reactive.query.impl.StageQueryImpl;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.util.impl.CompletionStages;

/**
 * Implements the {@link Stage.Session} API. This delegating class is
 * needed to avoid name clashes when implementing both
 * {@code Session} and {@link org.hibernate.Session}.
 */
public class StageSessionImpl implements Stage.Session {

	private final ReactiveSessionInternal delegate;

	public StageSessionImpl(ReactiveSessionInternal session) {
		this.delegate = session;
	}

	@Override
	public CompletionStage<Stage.Session> flush() {
//		checkOpen();
		return delegate.reactiveFlush().thenApply( v-> this );
	}

	@Override
	public <T> CompletionStage<T> fetch(T association) {
		return delegate.reactiveFetch(association, false);
	}

	@Override
	public <T> CompletionStage<T> unproxy(T association) {
		return delegate.reactiveFetch(association, true);
	}

	@Override
	public <T> T getReference(Class<T> entityClass, Object id) {
		//it's important that this method does not hit the database!
		//TODO: how can we guarantee that?
		return delegate.getReference( entityClass, id );
	}

	@Override
	public <T> CompletionStage<T> find(Class<T> entityClass, Object primaryKey) {
		return delegate.reactiveFind( entityClass, primaryKey, null, null );
	}

	@Override
	public <T> CompletionStage<List<T>> find(Class<T> entityClass, Object... ids) {
		return delegate.reactiveFind( entityClass, ids );
	}

	public <T> CompletionStage<T> find(
			Class<T> entityClass,
			Object primaryKey,
			Map<String, Object> properties) {
		return delegate.reactiveFind( entityClass, primaryKey, null, properties );
	}

	public <T> CompletionStage<T> find(Class<T> entityClass, Object primaryKey, LockModeType lockModeType) {
		return delegate.reactiveFind( entityClass, primaryKey, lockModeType, null );
	}

	public <T> CompletionStage<T> find(
			Class<T> entityClass,
			Object primaryKey,
			LockModeType lockModeType,
			Map<String, Object> properties) {
		return delegate.reactiveFind(entityClass, primaryKey, lockModeType, properties);
	}

	@Override
	public CompletionStage<Stage.Session> persist(Object entity) {
		return delegate.reactivePersist( entity ).thenApply( v -> this );
	}

	@Override
	public CompletionStage<Stage.Session> persist(Object... entity) {
		return applyToAll( delegate::reactivePersist, entity ).thenApply( v -> this );
	}

	@Override
	public CompletionStage<Stage.Session> remove(Object entity) {
		return delegate.reactiveRemove( entity ).thenApply( v -> this );
	}

	@Override
	public CompletionStage<Stage.Session> remove(Object... entity) {
		return applyToAll( delegate::reactiveRemove, entity ).thenApply( v -> this );
	}

	@Override
	public <T> CompletionStage<T> merge(T entity) {
		return delegate.reactiveMerge( entity );
	}

	@Override
	public <T> CompletionStage<Void> merge(T... entity) {
		return applyToAll( delegate::reactiveMerge, entity ).thenApply( v -> null );
	}

	@Override
	public CompletionStage<Stage.Session> refresh(Object entity) {
		return delegate.reactiveRefresh( entity ).thenApply( v -> this );
	}

	@Override
	public CompletionStage<Stage.Session> refresh(Object... entity) {
		return applyToAll( delegate::reactiveRefresh, entity ).thenApply( v -> this );
	}

	@Override
	public <R> Stage.Query<R> createQuery(String jpql, Class<R> resultType) {
		return new StageQueryImpl<>( delegate.createReactiveQuery( jpql, resultType ) );
	}

	@Override
	public <R> Stage.Query<R> createQuery(String jpql) {
		return new StageQueryImpl<>( delegate.createReactiveQuery( jpql ) );
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
	public Stage.Session setFlushMode(FlushMode flushMode) {
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
	public Stage.Session setDefaultReadOnly(boolean readOnly) {
		delegate.setDefaultReadOnly(readOnly);
		return this;
	}

	@Override
	public Stage.Session setReadOnly(Object entityOrProxy, boolean readOnly) {
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

	public Stage.Session setCacheMode(CacheMode cacheMode) {
		delegate.setCacheMode(cacheMode);
		return this;
	}

	@Override
	public Stage.Session detach(Object entity) {
		delegate.detach(entity);
		return this;
	}

	@Override
	public Stage.Session clear() {
		delegate.clear();
		return this;
	}

	@Override
	public Stage.Session enableFetchProfile(String name) {
		delegate.enableFetchProfile(name);
		return this;
	}

	@Override
	public Stage.Session disableFetchProfile(String name) {
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

	private <T> CompletionStage<T> applyToAll(
			Function<Object, CompletionStage<T>> op,
			Object[] entity) {
		if ( entity.length==0 ) {
			return CompletionStages.nullFuture();
		}
		else if ( entity.length==1 ) {
			return op.apply( entity[0] );
		}

		CompletionStage<T> stage = CompletionStages.nullFuture();
		for (Object e: entity) {
			stage = stage.thenCompose( v -> op.apply(e) );
		}
		return stage;
	}

}
