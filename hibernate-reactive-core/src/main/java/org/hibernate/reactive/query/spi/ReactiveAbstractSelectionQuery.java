/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.spi;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.hibernate.HibernateException;
import org.hibernate.LockOptions;
import org.hibernate.TypeMismatchException;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.IllegalQueryOperationException;
import org.hibernate.query.hql.internal.QuerySplitter;
import org.hibernate.query.internal.DelegatingDomainQueryExecutionContext;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.QueryInterpretationCache;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.internal.SqmInterpretationsKey;
import org.hibernate.query.sqm.internal.SqmInterpretationsKey.InterpretationsKeySource;
import org.hibernate.query.sqm.tree.SqmStatement;
import org.hibernate.query.sqm.tree.select.SqmSelectStatement;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.sqm.internal.AggregatedSelectReactiveQueryPlan;
import org.hibernate.reactive.query.sqm.internal.ConcreteSqmSelectReactiveQueryPlan;
import org.hibernate.reactive.query.sqm.spi.ReactiveSelectQueryPlan;
import org.hibernate.reactive.sql.results.spi.ReactiveSingleResultConsumer;
import org.hibernate.sql.results.internal.TupleMetadata;

import jakarta.persistence.NoResultException;

import static java.util.Collections.emptySet;

/**
 * Emulate {@link org.hibernate.query.spi.AbstractSelectionQuery}.
 * <p>
 *     Hibernate Reactive implementations already extend another class,
 *     they cannot extends {@link org.hibernate.query.spi.AbstractSelectionQuery too}.
 *     This approach allows us to avoid duplicating code.
 * </p>
 * @param <R>
 */
public class ReactiveAbstractSelectionQuery<R> {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final Supplier<QueryOptions> queryOptionsSupplier;

	private final SharedSessionContractImplementor session;
	private final Supplier<CompletionStage<List<R>>> doList;
	private final Supplier<SqmStatement<?>> getStatement;

	private final Supplier<TupleMetadata> getTupleMetadata;

	private final Supplier<DomainParameterXref> getDomainParameterXref;

	private final Supplier<Class<?>> getResultType;
	private final Supplier<String> getQueryString;

	private Set<String> fetchProfiles;

	private final Runnable beforeQuery;

	private final Consumer<Boolean> afterQuery;
	private final Function<List<R>, R> uniqueElement;
	private final InterpretationsKeySource interpretationsKeySource;

	// I'm sure we can avoid some of this by making some methods public in ORM,
	// but this allows me to prototype faster. We can refactor the code later.
	public ReactiveAbstractSelectionQuery(
			InterpretationsKeySource interpretationKeySource,
			SharedSessionContractImplementor session,
			Supplier<CompletionStage<List<R>>> doList,
			Supplier<SqmStatement<?>> getStatement,
			Supplier<TupleMetadata> getTupleMetadata,
			Supplier<DomainParameterXref> getDomainParameterXref,
			Supplier<Class<?>> getResultType,
			Supplier<String> getQueryString,
			Runnable beforeQuery,
			Consumer<Boolean> afterQuery,
			Function<List<R>, R> uniqueElement) {
		this(
				interpretationKeySource::getQueryOptions,
				session,
				doList,
				getStatement,
				getTupleMetadata,
				getDomainParameterXref,
				getResultType,
				getQueryString,
				beforeQuery,
				afterQuery,
				uniqueElement,
				interpretationKeySource
		);
	}

	public ReactiveAbstractSelectionQuery(
			Supplier<QueryOptions> queryOptionsSupplier,
			SharedSessionContractImplementor session,
			Supplier<CompletionStage<List<R>>> doList,
			Supplier<SqmStatement<?>> getStatement,
			Supplier<TupleMetadata> getTupleMetadata,
			Supplier<DomainParameterXref> getDomainParameterXref,
			Supplier<Class<?>> getResultType,
			Supplier<String> getQueryString,
			Runnable beforeQuery,
			Consumer<Boolean> afterQuery,
			Function<List<R>, R> uniqueElement,
			InterpretationsKeySource interpretationsKeySource) {
		this.queryOptionsSupplier = queryOptionsSupplier;
		this.session = session;
		this.doList = doList;
		this.getStatement = getStatement;
		this.getTupleMetadata = getTupleMetadata;
		this.getDomainParameterXref = getDomainParameterXref;
		this.getResultType = getResultType;
		this.getQueryString = getQueryString;
		this.beforeQuery = beforeQuery;
		this.afterQuery = afterQuery;
		this.uniqueElement = uniqueElement;
		this.interpretationsKeySource = interpretationsKeySource;
	}

	public CompletionStage<R> reactiveUnique() {
		return reactiveList()
				.thenApply( uniqueElement );
	}

	public CompletionStage<Optional<R>> reactiveUniqueResultOptional() {
		return reactiveUnique()
				.thenApply( Optional::ofNullable );
	}

	public CompletionStage<R> getReactiveSingleResult() {
		return reactiveList()
				.thenApply( this::reactiveSingleResult )
				.exceptionally( this::convertException );
	}

	public CompletionStage<Long> getReactiveResultsCount(SqmSelectStatement<?> sqmStatement, DomainQueryExecutionContext domainQueryExecutionContext) {
		final DelegatingDomainQueryExecutionContext context = new DelegatingDomainQueryExecutionContext( domainQueryExecutionContext ) {
			@Override
			public QueryOptions getQueryOptions() {
				return QueryOptions.NONE;
			}
		};
		return buildConcreteSelectQueryPlan( sqmStatement.createCountQuery(), Long.class, getQueryOptions() )
				.reactiveExecuteQuery( context, new ReactiveSingleResultConsumer<>() );
	}

	private R reactiveSingleResult(List<R> list) {
		if ( list.isEmpty() ) {
			throw new NoResultException( String.format( "No result found for query [%s]", getQueryString() ) );
		}
		return uniqueElement.apply( list );
	}

	public CompletionStage<R> getReactiveSingleResultOrNull() {
		return reactiveList()
				.thenApply( uniqueElement )
				.exceptionally( this::convertException );
	}

	private R convertException(Throwable t) {
		if ( t instanceof CompletionException && t.getCause() != null ) {
			return convertException( t.getCause() );
		}
		else if ( t instanceof HibernateException ) {
			throw getSession().getExceptionConverter().convert( (HibernateException) t, getLockOptions() );
		}
		else if ( t instanceof RuntimeException ) {
			throw getSession().getExceptionConverter().convert( (RuntimeException) t, getLockOptions() );
		}
		else {
			throw new CompletionException( t );
		}
	}

	private LockOptions getLockOptions() {
		return getQueryOptions().getLockOptions();
	}

	public CompletionStage<List<R>> reactiveList() {
		final Set<String> profiles = applyProfiles();
		beforeQuery.run();
		return doReactiveList()
				.handle( (list, error) -> {
					handleException( error );
					return list;
				} )
				.whenComplete( (rs, throwable) -> {
					afterQuery.accept( throwable == null );
					unapplyProfiles( profiles );
				} );
	}

	private void unapplyProfiles(Set<String> profiles) {
		for ( String profile : profiles) {
			session.getLoadQueryInfluencers().disableFetchProfile( profile );
		}
	}

	private Set<String> applyProfiles() {
		final LoadQueryInfluencers loadQueryInfluencers = session.getLoadQueryInfluencers();
		if ( fetchProfiles != null ) {
			final Set<String> profiles = new HashSet<>( fetchProfiles.size() );
			for ( String profile : fetchProfiles ) {
				if ( !loadQueryInfluencers.isFetchProfileEnabled( profile ) ) {
					loadQueryInfluencers.enableFetchProfile( profile );
					profiles.add( profile );
				}
			}
			return profiles;
		}
		else {
			return emptySet();
		}
	}

	private void handleException(Throwable e) {
		if ( e != null ) {
			if ( e instanceof IllegalQueryOperationException ) {
				throw new IllegalStateException( e );
			}
			if ( e instanceof TypeMismatchException ) {
				throw new IllegalStateException( e );
			}
			if ( e instanceof HibernateException ) {
				throw getSession().getExceptionConverter()
						.convert( (HibernateException) e, getLockOptions() );
			}
			if ( e instanceof RuntimeException ) {
				throw (RuntimeException) e;
			}

			throw new HibernateException( e );
		}
	}

	public ReactiveSelectQueryPlan<R> resolveSelectReactiveQueryPlan() {
		final QueryInterpretationCache.Key cacheKey = SqmInterpretationsKey.createInterpretationsKey( interpretationsKeySource );
		if ( cacheKey != null ) {
			return (ReactiveSelectQueryPlan<R>) getSession().getFactory()
					.getQueryEngine()
					.getInterpretationCache()
					.resolveSelectQueryPlan( cacheKey, this::buildSelectQueryPlan );
		}
		else {
			return buildSelectQueryPlan();
		}
	}

	private ReactiveSelectQueryPlan<R> buildSelectQueryPlan() {
		final SqmSelectStatement<R>[] concreteSqmStatements = QuerySplitter
				.split( (SqmSelectStatement<R>) getSqmStatement() );

		return concreteSqmStatements.length > 1
				? buildAggregatedSelectQueryPlan( concreteSqmStatements )
				: buildConcreteSelectQueryPlan( concreteSqmStatements[0], getResultType(), getQueryOptions() );
	}

	private ReactiveSelectQueryPlan<R> buildAggregatedSelectQueryPlan(SqmSelectStatement<?>[] concreteSqmStatements) {
		final ReactiveSelectQueryPlan<R>[] aggregatedQueryPlans = new ReactiveSelectQueryPlan[ concreteSqmStatements.length ];

		// todo (6.0) : we want to make sure that certain thing (ResultListTransformer, etc) only get applied at the aggregator-level

		for ( int i = 0, x = concreteSqmStatements.length; i < x; i++ ) {
			aggregatedQueryPlans[i] = buildConcreteSelectQueryPlan( concreteSqmStatements[i], getResultType(), getQueryOptions() );
		}

		return new AggregatedSelectReactiveQueryPlan<>(  aggregatedQueryPlans );
	}

	public  <T> ReactiveSelectQueryPlan<T> buildConcreteSelectQueryPlan(
			SqmSelectStatement<?> concreteSqmStatement,
			Class<T> resultType,
			QueryOptions queryOptions) {
		return new ConcreteSqmSelectReactiveQueryPlan<>(
				concreteSqmStatement,
				getQueryString(),
				getDomainParameterXref(),
				resultType,
				getTupleMetadata(),
				queryOptions
		);
	}

	private QueryOptions getQueryOptions() {
		return queryOptionsSupplier.get();
	}

	private SharedSessionContractImplementor getSession() {
		return session;
	}

	private CompletionStage<List<R>> doReactiveList() {
		return doList.get();
	}

	public SqmStatement<R> getSqmStatement() {
		return (SqmStatement<R>) getStatement.get();
	}

	public TupleMetadata getTupleMetadata() {
		return getTupleMetadata.get();
	}

	public Class<R> getResultType() {
		return (Class<R>) getResultType.get();
	}

	public DomainParameterXref getDomainParameterXref() {
		return getDomainParameterXref.get();
	}

	public String getQueryString() {
		return getQueryString.get();
	}

	public R getSingleResult() {
		throw LOG.nonReactiveMethodCall( "getReactiveSingleResult" );
	}

	public R getSingleResultOrNull() {
		throw LOG.nonReactiveMethodCall( "getReactiveSingleResultOrNull" );
	}

	public List<R> getResultList() {
		throw LOG.nonReactiveMethodCall( "getReactiveResultList" );
	}

	public List<R> list() {
		throw LOG.nonReactiveMethodCall( "reactiveList" );
	}

	public Stream<R> getResultStream() {
		throw LOG.nonReactiveMethodCall( "<no alternative>" );
	}

	public R uniqueResult() {
		throw LOG.nonReactiveMethodCall( "reactiveUniqueResult" );
	}

	public Optional<R> uniqueResultOptional() {
		throw LOG.nonReactiveMethodCall( "reactiveUniqueResultOptional" );
	}

	public void enableFetchProfile(String profileName) {
		if ( fetchProfiles == null ) {
			fetchProfiles = new HashSet<>();
		}
		fetchProfiles.add( profileName );
	}
}
