/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.internal;

import java.util.Map;
import java.util.function.Consumer;

import org.hibernate.HibernateException;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.procedure.spi.NamedCallableQueryMemento;
import org.hibernate.query.named.NamedObjectRepository;
import org.hibernate.query.named.NamedQueryMemento;
import org.hibernate.query.named.NamedResultSetMappingMemento;
import org.hibernate.query.spi.QueryEngine;
import org.hibernate.query.sql.spi.NamedNativeQueryMemento;
import org.hibernate.query.sqm.spi.NamedSqmQueryMemento;
import org.hibernate.reactive.query.sql.spi.ReactiveNamedNativeQueryMemento;
import org.hibernate.reactive.query.sql.spi.ReactiveNamedSqmQueryMemento;

public class ReactiveNamedObjectRepositoryImpl implements NamedObjectRepository {

	private final NamedObjectRepository delegate;

	public ReactiveNamedObjectRepositoryImpl(NamedObjectRepository delegate) {
		this.delegate = delegate;
	}

	@Override
	public NamedSqmQueryMemento getSqmQueryMemento(String queryName) {
		NamedSqmQueryMemento sqmQueryMemento = delegate.getSqmQueryMemento( queryName );
		return sqmQueryMemento == null
				? null
				: new ReactiveNamedSqmQueryMemento( sqmQueryMemento );
	}

	@Override
	public void visitSqmQueryMementos(Consumer<NamedSqmQueryMemento> action) {
		delegate.visitSqmQueryMementos( action );
	}

	@Override
	public void registerSqmQueryMemento(String name, NamedSqmQueryMemento descriptor) {
		delegate.registerSqmQueryMemento( name, descriptor );
	}

	@Override
	public NamedNativeQueryMemento getNativeQueryMemento(String queryName) {
		NamedNativeQueryMemento nativeQueryMemento = delegate.getNativeQueryMemento( queryName );
		return nativeQueryMemento == null
				? null
				: new ReactiveNamedNativeQueryMemento( nativeQueryMemento );
	}

	@Override
	public void visitNativeQueryMementos(Consumer<NamedNativeQueryMemento> action) {
		delegate.visitNativeQueryMementos( action );
	}

	@Override
	public void registerNativeQueryMemento(String name, NamedNativeQueryMemento descriptor) {
		delegate.registerNativeQueryMemento( name, descriptor );
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
	public NamedQueryMemento resolve(
			SessionFactoryImplementor sessionFactory,
			MetadataImplementor bootMetamodel,
			String registrationName) {
		return delegate.resolve( sessionFactory, bootMetamodel, registrationName );
	}

	@Override
	public void prepare(SessionFactoryImplementor sessionFactory, Metadata bootMetamodel) {
		delegate.prepare( sessionFactory, bootMetamodel );
	}

	@Override
	public void close() {
		delegate.close();
	}
}
