/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.mutiny.impl;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

import org.hibernate.CacheMode;
import org.hibernate.Filter;
import org.hibernate.FlushMode;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.reactive.common.AffectedEntities;
import org.hibernate.reactive.common.Identifier;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.query.sqm.iternal.ReactiveQuerySqmImpl;
import org.hibernate.reactive.session.ReactiveSession;

import io.smallrye.mutiny.Uni;
import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.EntityGraph;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.CriteriaUpdate;
import jakarta.persistence.metamodel.Attribute;

import static org.hibernate.reactive.util.impl.CompletionStages.applyToAll;


/**
 * Implements the {@link Mutiny.Session} API. This delegating class is
 * needed to avoid name clashes when implementing both
 * {@code Session} and {@link org.hibernate.Session}.
 */
public class MutinySessionImpl implements Mutiny.Session {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final ReactiveSession delegate;
	private final MutinySessionFactoryImpl factory;

	public MutinySessionImpl(ReactiveSession session, MutinySessionFactoryImpl factory) {
		this.delegate = session;
		this.factory = factory;
	}

	<T> Uni<T> uni(Supplier<CompletionStage<T>> stageSupplier) {
		return factory.uni( stageSupplier );
	}

	@Override
	public Uni<Void> flush() {
//		checkOpen();
		return uni( delegate::reactiveFlush );
	}

	@Override
	public <T> Uni<T> fetch(T association) {
		return uni( () -> delegate.reactiveFetch( association, false ) );
	}

	@Override
	public <E, T> Uni<T> fetch(E entity, Attribute<E, T> field) {
		return uni( () -> delegate.reactiveFetch( entity, field ) );
	}

	@Override
	public <T> Uni<T> unproxy(T association) {
		return uni( () -> delegate.reactiveFetch( association, true ) );
	}

	@Override
	public <T> T getReference(Class<T> entityClass, Object id) {
		//it's important that this method does not hit the database!
		//TODO: how can we guarantee that?
		return delegate.getReference( entityClass, id );
	}

	public ReactiveConnection getReactiveConnection() {
		return delegate.getReactiveConnection();
	}

	@Override
	public <T> T getReference(T entity) {
		return delegate.getReference( delegate.getEntityClass( entity ), delegate.getEntityId( entity ) );
	}

	@Override
	public LockMode getLockMode(Object entity) {
		return delegate.getCurrentLockMode( entity );
	}

	@Override
	public boolean contains(Object entity) {
		return delegate.contains( entity );
	}

	@Override
	public <R> Mutiny.Query<R> createQuery(String queryString) {
		return new MutinyQueryImpl<>( delegate.createReactiveQuery( queryString ), factory );
	}

	@Override
	public <R> Mutiny.Query<R> createQuery(String queryString, Class<R> resultType) {
		return new MutinyQueryImpl<>( delegate.createReactiveQuery( queryString, resultType ), factory );
	}

	@Override
	public <R> Mutiny.MutationQuery<R> createQuery(CriteriaUpdate<R> criteriaUpdate) {
		return new MutinyQueryImpl<>(
				(ReactiveQuerySqmImpl<R>) delegate.createReactiveMutationQuery( criteriaUpdate ),
				factory
		);
	}

	@Override
	public <R> Mutiny.MutationQuery<R> createQuery(CriteriaDelete<R> criteriaDelete) {
		return new MutinyQueryImpl<>(
				(ReactiveQuerySqmImpl<R>) delegate.createReactiveMutationQuery( criteriaDelete ),
				factory
		);
	}

	@Override
	public <R> Mutiny.Query<R> createNamedQuery(String queryName) {
		return new MutinyQueryImpl<>( delegate.createReactiveNamedQuery( queryName, null ), factory );
	}

	@Override
	public <R> Mutiny.Query<R> createNamedQuery(String queryName, Class<R> resultType) {
		return new MutinyQueryImpl<>( delegate.createReactiveNamedQuery( queryName, resultType ), factory );
	}

	@Override
	public <R> Mutiny.NativeQuery<R> createNativeQuery(String queryString) {
		return new MutinyNativeQueryImpl<>( delegate.createReactiveNativeQuery( queryString ), factory );
	}

	@Override
	public <R> Mutiny.NativeQuery<R> createNativeQuery(String queryString, AffectedEntities affectedEntities) {
		return new MutinyNativeQueryImpl<>( delegate.createReactiveNativeQuery( queryString, affectedEntities ), factory );
	}

	@Override
	public <R> Mutiny.NativeQuery<R> createNativeQuery(String queryString, Class<R> resultType) {
		return new MutinyNativeQueryImpl<>( delegate.createReactiveNativeQuery( queryString, resultType ), factory );
	}

	@Override
	public <R> Mutiny.NativeQuery<R> createNativeQuery(String queryString, Class<R> resultType, AffectedEntities affectedEntities) {
		return new MutinyNativeQueryImpl<>( delegate.createReactiveNativeQuery( queryString, resultType, affectedEntities ), factory );
	}

	@Override
	public <R> Mutiny.NativeQuery<R> createNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping) {
		return new MutinyNativeQueryImpl<>( delegate.createReactiveNativeQuery( queryString, resultSetMapping ), factory );
	}

	@Override
	public <R> Mutiny.NativeQuery<R> createNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping, AffectedEntities affectedEntities) {
		return new MutinyNativeQueryImpl<>( delegate.createReactiveNativeQuery( queryString, resultSetMapping, affectedEntities ), factory );
	}

	@Override
	public <T> Uni<T> find(Class<T> entityClass, Object primaryKey) {
		return uni( () -> delegate.reactiveFind( entityClass, primaryKey, null, null ) );
	}

	@Override
	public <T> Uni<List<T>> find(Class<T> entityClass, Object... ids) {
		return uni( () -> delegate.reactiveFind( entityClass, ids ) );
	}

	@Override
	public <T> Uni<T> find(Class<T> entityClass, Identifier<T> id) {
		return uni( () -> delegate.reactiveFind( entityClass, id.namedValues() ) );
	}

	@Override
	public <T> Uni<T> find(Class<T> entityClass, Object primaryKey, LockMode lockMode) {
		return uni( () -> delegate.reactiveFind( entityClass, primaryKey, new LockOptions( lockMode ), null ) );
	}

	@Override
	public <T> Uni<T> find(Class<T> entityClass, Object id, LockModeType lockModeType) {
		return Mutiny.Session.super.find( entityClass, id, lockModeType );
	}

	//	@Override
	public <T> Uni<T> find(Class<T> entityClass, Object primaryKey, LockOptions lockOptions) {
		return uni( () -> delegate.reactiveFind( entityClass, primaryKey, lockOptions, null ) );
	}

	@Override
	public <T> Uni<T> find(EntityGraph<T> entityGraph, Object id) {
		Class<T> entityClass = ( (RootGraphImplementor<T>) entityGraph ).getGraphedType().getJavaType();
		return uni( () -> delegate.reactiveFind( entityClass, id, null, entityGraph ) );
	}

	@Override
	public Uni<Void> persist(Object entity) {
		return uni( () -> delegate.reactivePersist( entity ) );
	}

	@Override
	public Uni<Void> persistAll(Object... entity) {
		return uni( () -> applyToAll( delegate::reactivePersist, entity ) );
	}

	@Override
	public Uni<Void> remove(Object entity) {
		return uni( () -> delegate.reactiveRemove( entity ) );
	}

	@Override
	public Uni<Void> removeAll(Object... entity) {
		return uni( () -> applyToAll( delegate::reactiveRemove, entity ) );
	}

	@Override
	public <T> Uni<T> merge(T entity) {
		return uni( () -> delegate.reactiveMerge( entity ) );
	}

	@Override
	@SafeVarargs
	public final <T> Uni<Void> mergeAll(T... entity) {
		return uni( () -> applyToAll( delegate::reactiveMerge, entity ) );
	}

	@Override
	public Uni<Void> refresh(Object entity) {
		return uni( () -> delegate.reactiveRefresh( entity, LockOptions.NONE ) );
	}

	@Override
	public Uni<Void> refresh(Object entity, LockMode lockMode) {
		return uni( () -> delegate.reactiveRefresh( entity, new LockOptions( lockMode ) ) );
	}

	@Override
	public Uni<Void> refresh(Object entity, LockModeType lockModeType) {
		return Mutiny.Session.super.refresh( entity, lockModeType );
	}

	//	@Override
	public Uni<Void> refresh(Object entity, LockOptions lockOptions) {
		return uni( () -> delegate.reactiveRefresh( entity, lockOptions ) );
	}

	@Override
	public Uni<Void> refreshAll(Object... entity) {
		return uni( () -> applyToAll( e -> delegate.reactiveRefresh( e, LockOptions.NONE ), entity ) );
	}

	@Override
	public Uni<Void> lock(Object entity, LockMode lockMode) {
		return uni( () -> delegate.reactiveLock( entity, new LockOptions( lockMode ) ) );
	}

	@Override
	public Uni<Void> lock(Object entity, LockModeType lockModeType) {
		return Mutiny.Session.super.lock( entity, lockModeType );
	}

	//	@Override
	public Uni<Void> lock(Object entity, LockOptions lockOptions) {
		return uni( () -> delegate.reactiveLock( entity, lockOptions ) );
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
				throw LOG.impossibleFlushModeIllegalState();
		}
	}

	@Override
	public Mutiny.Session setFlushMode(FlushMode flushMode) {
		switch ( flushMode ) {
			case COMMIT:
				delegate.setHibernateFlushMode( FlushMode.COMMIT );
				break;
			case AUTO:
				delegate.setHibernateFlushMode( FlushMode.AUTO );
				break;
			case MANUAL:
				delegate.setHibernateFlushMode( FlushMode.MANUAL );
				break;
			case ALWAYS:
				delegate.setHibernateFlushMode( FlushMode.ALWAYS );
				break;
		}
		return this;
	}

	@Override
	public Mutiny.Session setFlushMode(FlushModeType flushModeType) {
		return Mutiny.Session.super.setFlushMode( flushModeType );
	}

	@Override
	public Mutiny.Session setDefaultReadOnly(boolean readOnly) {
		delegate.setDefaultReadOnly( readOnly );
		return this;
	}

	@Override
	public boolean isDefaultReadOnly() {
		return delegate.isDefaultReadOnly();
	}

	@Override
	public Mutiny.Session setReadOnly(Object entityOrProxy, boolean readOnly) {
		delegate.setReadOnly( entityOrProxy, readOnly );
		return this;
	}

	@Override
	public boolean isReadOnly(Object entityOrProxy) {
		return delegate.isReadOnly( entityOrProxy );
	}

	public CacheMode getCacheMode() {
		return delegate.getCacheMode();
	}

	public Mutiny.Session setCacheMode(CacheMode cacheMode) {
		delegate.setCacheMode( cacheMode );
		return this;
	}

	@Override
	public Mutiny.Session setCacheStoreMode(CacheStoreMode cacheStoreMode) {
		return Mutiny.Session.super.setCacheStoreMode( cacheStoreMode );
	}

	@Override
	public Mutiny.Session setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode) {
		return Mutiny.Session.super.setCacheRetrieveMode( cacheRetrieveMode );
	}

	@Override
	public Mutiny.Session setBatchSize(Integer batchSize) {
		delegate.setBatchSize( batchSize );
		return this;
	}

	@Override
	public Integer getBatchSize() {
		return delegate.getBatchSize();
	}

	@Override
	public Mutiny.Session detach(Object entity) {
		delegate.detach( entity );
		return this;
	}

	@Override
	public Mutiny.Session clear() {
		delegate.clear();
		return this;
	}

	@Override
	public Mutiny.Session enableFetchProfile(String name) {
		delegate.enableFetchProfile( name );
		return this;
	}

	@Override
	public Mutiny.Session disableFetchProfile(String name) {
		delegate.disableFetchProfile( name );
		return this;
	}

	@Override
	public boolean isFetchProfileEnabled(String name) {
		return delegate.isFetchProfileEnabled( name );
	}

	@Override
	public Filter enableFilter(String filterName) {
		return delegate.enableFilter( filterName );
	}

	@Override
	public void disableFilter(String filterName) {
		delegate.disableFilter( filterName );
	}

	@Override
	public Filter getEnabledFilter(String filterName) {
		return delegate.getEnabledFilter( filterName );
	}

	@Override
	public <T> Uni<T> withTransaction(Function<Mutiny.Transaction, Uni<T>> work) {
		return currentTransaction == null
				? new Transaction<T>().execute( work )
				: work.apply( currentTransaction );
	}

	private Transaction<?> currentTransaction;

	@Override
	public Mutiny.Transaction currentTransaction() {
		return currentTransaction;
	}

	private class Transaction<T> implements Mutiny.Transaction {
		boolean rollback;

		Uni<T> execute(Function<Mutiny.Transaction, Uni<T>> work) {
			currentTransaction = this;
			return begin().chain( () -> executeInTransaction( work ) ).eventually( () -> currentTransaction = null );
		}

		/**
		 * Run the code assuming that a transaction has already started so that we can
		 * differentiate an error starting a transaction (and therefore doesn't need to rollback)
		 * and an error thrown by the work.
		 */
		Uni<T> executeInTransaction(Function<Mutiny.Transaction, Uni<T>> work) {
			return work.apply( this )
					// only flush() if the work completed with no exception
					.call( this::flush ).call( this::beforeCompletion )
					// in the case of an exception or cancellation
					// we need to rollback the transaction
					.onFailure().call( this::rollback ).onCancellation().call( this::rollback )
					// finally, when there was no exception,
					// commit or rollback the transaction
					.call( () -> rollback ? rollback() : commit() ).call( this::afterCompletion );
		}

		Uni<Void> flush() {
			return Uni.createFrom().completionStage( delegate.reactiveAutoflush() );
		}

		Uni<Void> begin() {
			return Uni.createFrom().completionStage( delegate.getReactiveConnection().beginTransaction() );
		}

		Uni<Void> rollback() {
			return Uni.createFrom().completionStage( delegate.getReactiveConnection().rollbackTransaction() );
		}

		Uni<Void> commit() {
			return Uni.createFrom().completionStage( delegate.getReactiveConnection().commitTransaction() );
		}

		private Uni<Void> beforeCompletion() {
			return Uni.createFrom().completionStage( delegate.getReactiveActionQueue().beforeTransactionCompletion() );
		}

		private Uni<Void> afterCompletion() {
			return Uni.createFrom().completionStage( delegate.getReactiveActionQueue().afterTransactionCompletion( !rollback ) );
		}

		@Override
		public void markForRollback() {
			rollback = true;
		}

		@Override
		public boolean isMarkedForRollback() {
			return rollback;
		}
	}

	@Override
	public Uni<Void> close() {
		return uni( delegate::reactiveClose );
	}

	@Override
	public boolean isOpen() {
		return delegate.isOpen();
	}

	@Override
	public <T> ResultSetMapping<T> getResultSetMapping(Class<T> resultType, String mappingName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> EntityGraph<T> createEntityGraph(Class<T> rootType, String graphName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> EntityGraph<T> getEntityGraph(Class<T> rootType, String graphName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> EntityGraph<T> createEntityGraph(Class<T> rootType) {
		throw new UnsupportedOperationException();
	}
}
