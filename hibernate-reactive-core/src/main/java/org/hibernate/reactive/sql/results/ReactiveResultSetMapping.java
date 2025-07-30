/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.query.named.NamedResultSetMappingMemento;
import org.hibernate.query.results.LegacyFetchBuilder;
import org.hibernate.query.results.ResultBuilder;
import org.hibernate.query.results.ResultSetMapping;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.sql.results.internal.ReactiveResultSetAccess;
import org.hibernate.reactive.sql.results.spi.ReactiveValuesMappingProducer;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMapping;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMetadata;

/**
 * @see org.hibernate.query.results.internal.ResultSetMappingImpl
 */
public class ReactiveResultSetMapping implements ResultSetMapping, ReactiveValuesMappingProducer {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );
	private final ResultSetMapping delegate;

	public ReactiveResultSetMapping(ResultSetMapping resultSetMapping) {
		this.delegate = Objects.requireNonNull( resultSetMapping );
		assert !(resultSetMapping instanceof ReactiveResultSetMapping) : "double wrapping detected!";
	}

	@Override
	public JdbcValuesMapping resolve(
			JdbcValuesMetadata jdbcResultsMetadata,
			LoadQueryInfluencers loadQueryInfluencers,
			SessionFactoryImplementor sessionFactory) {
		throw LOG.nonReactiveMethodCall( "reactiveResolve" );
	}

	@Override
	public void addAffectedTableNames(Set<String> affectedTableNames, SessionFactoryImplementor sessionFactory) {
		delegate.addAffectedTableNames( affectedTableNames, sessionFactory );
	}

	@Override
	public CompletionStage<JdbcValuesMapping> reactiveResolve(
			JdbcValuesMetadata jdbcResultsMetadata,
			LoadQueryInfluencers loadQueryInfluencers,
			SessionFactoryImplementor sessionFactory) {
		return ( (ReactiveResultSetAccess) jdbcResultsMetadata )
				.getReactiveResultSet()
				.thenApply( columnCount -> delegate.resolve( jdbcResultsMetadata, loadQueryInfluencers, sessionFactory ) );
	}

	@Override
	public ResultSetMapping cacheKeyInstance() {
		return new ReactiveResultSetMapping( delegate.cacheKeyInstance() );
	}

	@Override
	public String getMappingIdentifier() {
		return delegate.getMappingIdentifier();
	}

	@Override
	public boolean isDynamic() {
		return delegate.isDynamic();
	}

	@Override
	public int getNumberOfResultBuilders() {
		return delegate.getNumberOfResultBuilders();
	}

	@Override
	public List<ResultBuilder> getResultBuilders() {
		return delegate.getResultBuilders();
	}

	@Override
	public void visitResultBuilders(BiConsumer<Integer, ResultBuilder> resultBuilderConsumer) {
		delegate.visitResultBuilders( resultBuilderConsumer );
	}

	@Override
	public void visitLegacyFetchBuilders(Consumer<LegacyFetchBuilder> resultBuilderConsumer) {
		delegate.visitLegacyFetchBuilders( resultBuilderConsumer );
	}

	@Override
	public void addResultBuilder(ResultBuilder resultBuilder) {
		delegate.addResultBuilder( resultBuilder );
	}

	@Override
	public void addLegacyFetchBuilder(LegacyFetchBuilder fetchBuilder) {
		delegate.addLegacyFetchBuilder( fetchBuilder );
	}

	@Override
	public NamedResultSetMappingMemento toMemento(String name) {
		return delegate.toMemento( name );
	}
}
