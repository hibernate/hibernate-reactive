/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.internal;

import jakarta.persistence.Statement;
import jakarta.persistence.StatementReference;
import jakarta.persistence.TypedQuery;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.hibernate.HibernateException;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.procedure.spi.NamedCallableQueryMemento;
import org.hibernate.query.named.NamedMutationMemento;
import org.hibernate.query.named.NamedObjectRepository;
import org.hibernate.query.named.NamedQueryMemento;
import org.hibernate.query.named.NamedResultSetMappingMemento;
import org.hibernate.query.named.NamedSelectionMemento;
import org.hibernate.query.spi.QueryEngine;
import org.hibernate.query.named.NamedNativeQueryMemento;
import org.hibernate.query.named.NamedSqmQueryMemento;
import org.hibernate.reactive.query.sql.spi.ReactiveNamedNativeQueryMemento;
import org.hibernate.reactive.query.sql.spi.ReactiveNamedSqmQueryMemento;

import jakarta.persistence.TypedQueryReference;

public class ReactiveNamedObjectRepositoryImpl implements NamedObjectRepository {

	private final NamedObjectRepository delegate;

	public ReactiveNamedObjectRepositoryImpl(NamedObjectRepository delegate) {
		this.delegate = delegate;
	}

	@Override
	public <R> Map<String, TypedQueryReference<R>> getNamedQueries(Class<R> resultType) {
		return delegate.getNamedQueries( resultType );
	}

	public void forEachNamedQuery(BiConsumer<String,? super TypedQueryReference<?>> action){
		delegate.forEachNamedQuery( action );
	}

	@Override
	public <R> TypedQueryReference<R> registerNamedQuery(String name, TypedQuery<R> query) {
		return delegate.registerNamedQuery( name, query );
	}

	@Override
	public Map<String, StatementReference> getNamedMutations() {
		return delegate.getNamedMutations();
	}

	@Override
	public void forEachNamedMutation(BiConsumer<String,? super StatementReference> action) {
		delegate.forEachNamedMutation( action );
	}

	@Override
	public StatementReference registerNamedMutation(String name, Statement statement){
		return delegate.registerNamedMutation( name, statement );
	}

	@Override
	public <R> NamedQueryMemento<R> findQueryMementoByName(String name, boolean includeProcedureCalls){
		return delegate.findQueryMementoByName( name, includeProcedureCalls );
	}

	@Override
	public <R> NamedQueryMemento<R> getQueryMementoByName(String name, boolean includeProcedureCalls){
		return delegate.findQueryMementoByName( name, includeProcedureCalls );
	}

	@Override
	public <R> NamedSelectionMemento<R> getSelectionQueryMemento(String name){
		return delegate.getSelectionQueryMemento( name );
	}

	@Override
	public <R> NamedMutationMemento<R> getMutationQueryMemento(String name){
		return delegate.getMutationQueryMemento( name );
	}

	@Override
	public NamedCallableQueryMemento getCallableQueryMemento(String name) {
		return delegate.getCallableQueryMemento( name );
	}

	@Override
	public void visitCallableQueryMementos(Consumer<NamedCallableQueryMemento> action) {
		delegate.visitCallableQueryMementos( action );
	}

	@Override
	public void registerCallableQueryMemento(String name, NamedCallableQueryMemento memento) {
		delegate.registerCallableQueryMemento( name, memento );
	}

	@Override
	public NamedResultSetMappingMemento getResultSetMappingMemento(String mappingName) {
		return delegate.getResultSetMappingMemento( mappingName );
	}

	@Override
	public void visitResultSetMappingMementos(Consumer<NamedResultSetMappingMemento> action) {
		delegate.visitResultSetMappingMementos( action );
	}

	@Override
	public void registerResultSetMappingMemento(String name, NamedResultSetMappingMemento memento) {
		delegate.registerResultSetMappingMemento( name, memento );
	}

	@Override
	public Map<String, HibernateException> checkNamedQueries(QueryEngine queryPlanCache) {
		return delegate.checkNamedQueries( queryPlanCache );
	}

	@Override
	public void validateNamedQueries(QueryEngine queryEngine) {
		delegate.validateNamedQueries( queryEngine );
	}

	@Override
	public NamedQueryMemento<?> resolve(
			SessionFactoryImplementor sessionFactory,
			MetadataImplementor bootMetamodel,
			String registrationName) {
		return wrap( delegate.resolve( sessionFactory, bootMetamodel, registrationName ) );
	}

	@Override
	public void prepare(SessionFactoryImplementor sessionFactory, Metadata bootMetamodel) {
		delegate.prepare( sessionFactory, bootMetamodel );
	}

	@Override
	public void close() {
		delegate.close();
	}

	private static NamedQueryMemento<?> wrap(final NamedQueryMemento<?> namedQueryMemento) {
		if ( namedQueryMemento == null ) {
			return null;
		}
		else if ( namedQueryMemento instanceof NamedSqmQueryMemento ) {
			return wrapSqmQueryMemento( (NamedSqmQueryMemento<?>) namedQueryMemento );
		}
		else {
			return wrapNativeQueryMemento( (NamedNativeQueryMemento<?>) namedQueryMemento );
		}
	}

	private static NamedSqmQueryMemento<?> wrapSqmQueryMemento(final NamedSqmQueryMemento<?> sqmQueryMemento) {
		if ( sqmQueryMemento == null ) {
			return null;
		}
		//Avoid nested wrapping!
		else if ( sqmQueryMemento instanceof ReactiveNamedSqmQueryMemento ) {
			return sqmQueryMemento;
		}
		else {
			return new ReactiveNamedSqmQueryMemento( sqmQueryMemento );
		}
	}

	private static NamedNativeQueryMemento<?> wrapNativeQueryMemento(final NamedNativeQueryMemento<?> nativeQueryMemento) {
		if ( nativeQueryMemento == null ) {
			return null;
		}
		//Avoid nested wrapping!
		else if ( nativeQueryMemento instanceof ReactiveNamedNativeQueryMemento ) {
			return nativeQueryMemento;
		}
		else {
			return new ReactiveNamedNativeQueryMemento( nativeQueryMemento );
		}
	}

}
