/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.mutiny.impl;

import io.smallrye.mutiny.Uni;
import org.hibernate.CacheMode;
import org.hibernate.Filter;
import org.hibernate.FlushMode;
import org.hibernate.LockMode;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.metamodel.spi.MetamodelImplementor;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.session.Criteria;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.util.impl.CompletionStages;

import javax.persistence.EntityGraph;
import javax.persistence.criteria.CriteriaDelete;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.CriteriaUpdate;
import javax.persistence.metamodel.Attribute;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import static java.util.Collections.singletonMap;
import static org.hibernate.reactive.util.impl.CompletionStages.returnOrRethrow;

/**
 * Implements the {@link Mutiny.Session} API. This delegating class is
 * needed to avoid name clashes when implementing both
 * {@code Session} and {@link org.hibernate.Session}.
 */
public class MutinySessionImpl implements Mutiny.Session {

	private final ReactiveSession delegate;

	public MutinySessionImpl(ReactiveSession session) {
		this.delegate = session;
	}

	@Override
	public Uni<Mutiny.Session> flush() {
//		checkOpen();
		return Uni.createFrom().completionStage( delegate.reactiveFlush() )
				.map( v-> this );
	}

	@Override
	public <T> Uni<T> fetch(T association) {
		return Uni.createFrom().completionStage( delegate.reactiveFetch(association, false) );
	}

	@Override
	public <E, T> Uni<T> fetch(E entity, Attribute<E, T> field) {
		return Uni.createFrom().completionStage( delegate.reactiveFetch(entity, field) );
	}

	@Override
	public <T> Uni<T> unproxy(T association) {
		return Uni.createFrom().completionStage( delegate.reactiveFetch(association, true) );
	}

	@Override
	public <T> T getReference(Class<T> entityClass, Object id) {
		//it's important that this method does not hit the database!
		//TODO: how can we guarantee that?
		return delegate.getReference( entityClass, id );
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
	public <T> Uni<T> find(Class<T> entityClass, Object primaryKey) {
		return Uni.createFrom().completionStage( delegate.reactiveFind( entityClass, primaryKey, null, null ) );
	}

	@Override
	public <T> Uni<List<T>> find(Class<T> entityClass, Object... ids) {
		return Uni.createFrom().completionStage( delegate.reactiveFind( entityClass, ids ) );
	}

	public <T> Uni<T> find(
			Class<T> entityClass,
			Object primaryKey,
			Map<String, Object> properties) {
		return Uni.createFrom().completionStage( delegate.reactiveFind( entityClass, primaryKey, null, properties ) );
	}

	public <T> Uni<T> find(Class<T> entityClass, Object primaryKey, LockMode lockMode) {
		return Uni.createFrom().completionStage( delegate.reactiveFind( entityClass, primaryKey, lockMode, null ) );
	}

	@Override
	public <T> Uni<T> find(EntityGraph<T> entityGraph, Object id) {
		Class<T> entityClass = ((RootGraphImplementor<T>) entityGraph).getGraphedType().getJavaType();
		return Uni.createFrom().completionStage( delegate.reactiveFind( entityClass, id, null,
				singletonMap( GraphSemantic.FETCH.getJpaHintName(), entityGraph ) ) );
	}

	@Override
	public Uni<Mutiny.Session> persist(Object entity) {
		return Uni.createFrom().completionStage( delegate.reactivePersist( entity ) )
				.map( v-> this );
	}

	@Override
	public Uni<Mutiny.Session> persist(Object... entity) {
		return Uni.createFrom().completionStage( applyToAll( delegate::reactivePersist, entity ) )
				.map( v-> this );
	}

	@Override
	public Uni<Mutiny.Session> remove(Object entity) {
		return Uni.createFrom().completionStage( delegate.reactiveRemove( entity ) )
				.map( v-> this );
	}

	@Override
	public Uni<Mutiny.Session> remove(Object... entity) {
		return Uni.createFrom().completionStage( applyToAll( delegate::reactiveRemove, entity ) )
				.map( v-> this );
	}

	@Override
	public <T> Uni<T> merge(T entity) {
		return Uni.createFrom().completionStage( delegate.reactiveMerge( entity ) );
	}

	@Override
	public <T> Uni<Void> merge(T... entity) {
		return Uni.createFrom().completionStage( applyToAll( delegate::reactiveMerge, entity ) )
				.map( v-> null );
	}

	@Override
	public Uni<Mutiny.Session> refresh(Object entity) {
		return Uni.createFrom().completionStage( delegate.reactiveRefresh( entity, LockMode.NONE ) )
				.map( v-> this );
	}

	@Override
	public Uni<Mutiny.Session> refresh(Object entity, LockMode lockMode) {
		return Uni.createFrom().completionStage( delegate.reactiveRefresh( entity, lockMode ) )
				.map( v-> this );
	}

	@Override
	public Uni<Mutiny.Session> refresh(Object... entity) {
		return Uni.createFrom().completionStage( applyToAll( e -> delegate.reactiveRefresh( e, LockMode.NONE ), entity ) )
				.map( v-> this );
	}

	@Override
	public Uni<Mutiny.Session> lock(Object entity, LockMode lockMode) {
		return Uni.createFrom().completionStage( delegate.reactiveLock( entity, lockMode ) )
				.map( v -> this );
	}

	@Override
	public <R> Mutiny.Query<R> createQuery(String jpql, Class<R> resultType) {
		return new MutinyQueryImpl<>( delegate.createReactiveQuery( jpql, resultType ) );
	}

	@Override
	public <R> Mutiny.Query<R> createQuery(String jpql) {
		return new MutinyQueryImpl<>( delegate.createReactiveQuery( jpql ) );
	}

	@Override
	public <R> Mutiny.Query<R> createNativeQuery(String sql, Class<R> resultType) {
		final String typeName = resultType.getName();
		final MetamodelImplementor metamodel = delegate.getFactory().getMetamodel();
		final boolean knownType = metamodel.entityPersisters().containsKey( typeName );
		if ( knownType ) {
			return new MutinyQueryImpl<>( delegate.createReactiveNativeQuery( sql, resultType ) );
		}
		else {
			return new MutinyQueryImpl<>( delegate.createReactiveNativeQuery( sql ) );
		}
	}

	@Override
	public <R> Mutiny.Query<R> createNativeQuery(String sql, ResultSetMapping<R> resultSetMapping) {
		return new MutinyQueryImpl<>( delegate.createReactiveNativeQuery( sql, resultSetMapping.getName() ) );
	}

	@Override
	public <R> Mutiny.Query<R> createNativeQuery(String sql) {
		return new MutinyQueryImpl<>( delegate.createReactiveNativeQuery( sql ) );
	}

	@Override
	public <R> Mutiny.Query<R> createNamedQuery(String name) {
		return new MutinyQueryImpl<>( delegate.createReactiveNamedQuery( name ) );
	}

	@Override
	public <R> Mutiny.Query<R> createNamedQuery(String name, Class<R> resultType) {
		return new MutinyQueryImpl<>( delegate.createReactiveNamedQuery( name, resultType ) );
	}

	@Override @SuppressWarnings("unchecked")
	public <R> Mutiny.Query<R> createQuery(CriteriaQuery<R> criteriaQuery) {
		return new MutinyQueryImpl<>( delegate.createReactiveQuery( (Criteria<R>) criteriaQuery) );
	}

	@Override @SuppressWarnings("unchecked")
	public <R> Mutiny.Query<R> createQuery(CriteriaUpdate<R> criteriaUpdate) {
		return new MutinyQueryImpl<>( delegate.createReactiveQuery( (Criteria<R>) criteriaUpdate) );
	}

	@Override @SuppressWarnings("unchecked")
	public <R> Mutiny.Query<R> createQuery(CriteriaDelete<R> criteriaDelete) {
		return new MutinyQueryImpl<>( delegate.createReactiveQuery( (Criteria<R>) criteriaDelete) );
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
	public Mutiny.Session setFlushMode(FlushMode flushMode) {
		switch (flushMode) {
			case COMMIT:
				delegate.setHibernateFlushMode(FlushMode.COMMIT);
				break;
			case AUTO:
				delegate.setHibernateFlushMode(FlushMode.AUTO);
				break;
			case MANUAL:
				delegate.setHibernateFlushMode(FlushMode.MANUAL);
				break;
			case ALWAYS:
				delegate.setHibernateFlushMode(FlushMode.ALWAYS);
				break;
		}
		return this;
	}

	@Override
	public Mutiny.Session setDefaultReadOnly(boolean readOnly) {
		delegate.setDefaultReadOnly(readOnly);
		return this;
	}

	@Override
	public boolean isDefaultReadOnly() {
		return delegate.isDefaultReadOnly();
	}

	@Override
	public Mutiny.Session setReadOnly(Object entityOrProxy, boolean readOnly) {
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

	public Mutiny.Session setCacheMode(CacheMode cacheMode) {
		delegate.setCacheMode(cacheMode);
		return this;
	}

	@Override
	public Mutiny.Session detach(Object entity) {
		delegate.detach(entity);
		return this;
	}

	@Override
	public Mutiny.Session clear() {
		delegate.clear();
		return this;
	}

	@Override
	public Mutiny.Session enableFetchProfile(String name) {
		delegate.enableFetchProfile(name);
		return this;
	}

	@Override
	public Mutiny.Session disableFetchProfile(String name) {
		delegate.disableFetchProfile(name);
		return this;
	}

	@Override
	public boolean isFetchProfileEnabled(String name) {
		return delegate.isFetchProfileEnabled(name);
	}

	@Override
	public <T> ResultSetMapping<T> getResultSetMapping(Class<T> resultType, String mappingName) {
		return delegate.getResultSetMapping(resultType, mappingName);
	}

	@Override
	public <T> EntityGraph<T> getEntityGraph(Class<T> entity, String name) {
		return delegate.getEntityGraph(entity, name);
	}

	@Override
	public <T> EntityGraph<T> createEntityGraph(Class<T> entity) {
		return delegate.createEntityGraph(entity);
	}

	@Override
	public <T> EntityGraph<T> createEntityGraph(Class<T> entity, String name) {
		return delegate.createEntityGraph(entity, name);
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
	public <T> Uni<T> withTransaction(Function<Mutiny.Transaction, Uni<T>> work) {
		return new Transaction<T>().execute( work );
	}

	private class Transaction<T> implements Mutiny.Transaction {
		boolean rollback;
		Throwable error;

		Uni<T> execute(Function<Mutiny.Transaction, Uni<T>> work) {
			return begin()
					.flatMap( v -> work.apply( this ) )
					// only flush() if the work completed with no exception
					.flatMap( result -> flush().map( v -> result ) )
					// have to capture the error here and pass it along,
					// since we can't just return a CompletionStage that
					// rolls back the transaction from the handle() function
					.onTermination().invoke( this::processError )
					// finally, commit or rollback the transaction, and
					// then rethrow the caught error if necessary
					.flatMap(
							result -> end()
									// make sure that if rollback() throws,
									// the original error doesn't get swallowed
									.onTermination().invoke( this::processError )
									// finally rethrow the original error, if any
									.map( v -> returnOrRethrow( error, result ) )
					);
		}

		Uni<Void> flush() {
			return Uni.createFrom().completionStage( delegate.reactiveAutoflush() );
		}

		Uni<Void> begin() {
			return Uni.createFrom().completionStage( delegate.getReactiveConnection().beginTransaction() );
		}

		Uni<Void> end() {
			return Uni.createFrom().completionStage( rollback
					? delegate.getReactiveConnection().rollbackTransaction()
					: delegate.getReactiveConnection().commitTransaction()
			);
		}

		<R> R processError(R result, Throwable e, boolean canceled) {
			if ( canceled ) {
				rollback = true;
			}
			if ( e!=null ) {
				rollback = true;
				if (error == null) {
					error = e;
				}
				else {
					error.addSuppressed(e);
				}
			}
			return result;
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
	public void close() {
		delegate.close();
	}

	@Override
	public boolean isOpen() {
		return delegate.isOpen();
	}

	private CompletionStage<Void> applyToAll(
			Function<Object, CompletionStage<?>> op,
			Object[] entity) {
		if ( entity.length==0 ) {
			return CompletionStages.nullFuture();
		}
		else if ( entity.length==1 ) {
			return op.apply( entity[0] ).thenApply( v -> null );
		}
		else {
			return CompletionStages.loop( entity, op );
		}
	}

}
