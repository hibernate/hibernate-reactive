package org.hibernate.rx;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Depending of what we want to test, this session can be configured using the {@link Builder} to
 * simulate the operations in {@link RxSession}.
 * <p>
 * By default, if no operation is specified the methods will return null or do nothing.
 * </p>
 */
public class MockRxSession implements RxSession {

	private BiFunction loadFunction = (type, id) -> null;

	private Consumer persistFunction = obj -> {};
	private Consumer removeFunction = obj -> {};

		/**
		 * Assign the functions that simulate the access to a source for CRUD.
		 *
		 * @param <T> the type of the entity
		 */
		public static class Builder<T> {
			MockRxSession session = new MockRxSession();

			public Builder find(BiFunction<Class<T>, ?, T> load) {
				session.loadFunction = load;
				return this;
			}

			public Builder persist(Consumer<T> persist) {
				session.persistFunction = persist;
				return this;
			}

			public Builder remove(Consumer<T> remove) {
				session.removeFunction = remove;
				return this;
			}

			public RxSession build() {
				return session;
			}
		}

	@Override
	public <T> CompletionStage<T> find(Class<T> entityClass, final Object id) {
		return CompletableFuture.supplyAsync(() -> (T) loadFunction.apply( entityClass, id ));
	}

	@Override
	public <T> CompletionStage<List<T>> find(Class<T> entityClass, Object... ids) {
		return null;
	}

	@Override
	public <T> T getReference(Class<T> entityClass, Object id) {
		return null;
	}

	@Override
	public CompletionStage<RxSession> persist(Object entity) {
		return CompletableFuture.runAsync( () -> persistFunction.accept( entity ) ).thenApply( v-> this );
	}

	@Override
	public CompletionStage<RxSession> remove(Object entity) {
		return CompletableFuture.runAsync( () -> removeFunction.accept( entity ) ).thenApply( v-> this );
	}

	@Override
	public <T> CompletionStage<T> merge(T object) {
		return null;
	}

	@Override
	public CompletionStage<RxSession> refresh(Object entity) {
		return null;
	}

	@Override
	public RxSession setFlushMode(FlushMode flushMode) {
		throw new UnsupportedOperationException();
	}

	@Override
	public FlushMode getFlushMode() {
		return FlushMode.AUTO;
	}

	@Override
	public CompletionStage<RxSession> flush() {
		return CompletableFuture.completedFuture( this );
	}

	@Override
	public <T> CompletionStage<T> fetch(T association) {
		return CompletableFuture.completedFuture( association );
	}

	@Override
	public <R> RxQuery<R> createQuery(String queryString) {
		throw new UnsupportedOperationException( "not implemented" );
	}

	@Override
	public <R> RxQuery<R> createQuery(String queryString, Class<R> resultType) {
		throw new UnsupportedOperationException( "not implemented" );
	}

	@Override
	public RxSession clear() {
		return this;
	}

	@Override
	public RxSession detach(Object entity) {
		return this;
	}

	@Override
	public RxSession enableFetchProfile(String name) {
		return this;
	}

	@Override
	public RxSession disableFetchProfile(String name) {
		return this;
	}

	@Override
	public boolean isFetchProfileEnabled(String name) {
		return false;
	}

	@Override
	public RxSession setCacheMode(CacheMode cacheMode) {
		return this;
	}

	@Override
	public CacheMode getCacheMode() {
		return null;
	}

	@Override
	public void close() {}

}
