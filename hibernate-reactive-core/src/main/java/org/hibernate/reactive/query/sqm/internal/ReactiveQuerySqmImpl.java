/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.internal;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.TypeMismatchException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.Generator;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.RootGraph;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.id.BulkInsertionCapableIdentifierGenerator;
import org.hibernate.id.OptimizableGenerator;
import org.hibernate.id.enhanced.Optimizer;
import org.hibernate.metamodel.model.domain.EntityDomainType;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.query.IllegalQueryOperationException;
import org.hibernate.query.QueryParameter;
import org.hibernate.query.ResultListTransformer;
import org.hibernate.query.TupleTransformer;
import org.hibernate.query.criteria.internal.NamedCriteriaQueryMementoImpl;
import org.hibernate.query.hql.internal.NamedHqlQueryMementoImpl;
import org.hibernate.query.hql.internal.QuerySplitter;
import org.hibernate.query.spi.AbstractSelectionQuery;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.HqlInterpretation;
import org.hibernate.query.spi.QueryInterpretationCache;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.sqm.internal.SqmInterpretationsKey;
import org.hibernate.query.sqm.internal.SqmQueryImpl;
import org.hibernate.query.sqm.tree.SqmStatement;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.query.sqm.tree.insert.SqmInsertStatement;
import org.hibernate.query.sqm.tree.select.SqmSelectStatement;
import org.hibernate.query.sqm.tree.update.SqmUpdateStatement;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.spi.ReactiveAbstractSelectionQuery;
import org.hibernate.reactive.query.sql.spi.ReactiveNonSelectQueryPlan;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveSqmMultiTableInsertStrategy;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveSqmMultiTableMutationStrategy;
import org.hibernate.reactive.query.sqm.spi.ReactiveSelectQueryPlan;
import org.hibernate.reactive.session.ReactiveSqmQueryImplementor;
import org.hibernate.sql.exec.spi.Callback;
import org.hibernate.transform.ResultTransformer;

import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;
import jakarta.persistence.metamodel.Type;

/**
 * A reactive {@link SqmQueryImpl}
 */
public class ReactiveQuerySqmImpl<R> extends SqmQueryImpl<R> implements ReactiveSqmQueryImplementor<R> {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final ReactiveAbstractSelectionQuery<R> selectionQueryDelegate;

	public ReactiveQuerySqmImpl(
			NamedHqlQueryMementoImpl memento,
			Class<R> expectedResultType,
			SharedSessionContractImplementor session) {
		super( memento, expectedResultType, session );
		this.selectionQueryDelegate = createSelectionQueryDelegate( session );
	}

	public ReactiveQuerySqmImpl(
			NamedCriteriaQueryMementoImpl memento,
			Class<R> resultType,
			SharedSessionContractImplementor session) {
		super( memento, resultType, session );
		this.selectionQueryDelegate = createSelectionQueryDelegate( session );
	}

	public ReactiveQuerySqmImpl(
			String hql,
			HqlInterpretation hqlInterpretation,
			Class<R> resultType,
			SharedSessionContractImplementor session) {
		super( hql, hqlInterpretation, resultType, session );
		this.selectionQueryDelegate = createSelectionQueryDelegate( session );
	}

	public ReactiveQuerySqmImpl(
			SqmStatement<R> criteria,
			Class<R> resultType,
			SharedSessionContractImplementor session) {
		super( criteria, resultType, session );
		this.selectionQueryDelegate = createSelectionQueryDelegate( session );
	}

	private ReactiveAbstractSelectionQuery<R> createSelectionQueryDelegate(SharedSessionContractImplementor session) {
		return new ReactiveAbstractSelectionQuery<>(
				this,
				session,
				this::doReactiveList,
				this::getSqmStatement,
				this::getTupleMetadata,
				this::getDomainParameterXref,
				this::getResultType,
				this::getQueryString,
				this::beforeQuery,
				this::afterQuery,
				AbstractSelectionQuery::uniqueElement
			);
	}

	@Override
	public CompletionStage<R> reactiveUnique() {
		return selectionQueryDelegate.reactiveUnique();
	}

	@Override
	public CompletionStage<R> getReactiveSingleResult() {
		return selectionQueryDelegate.getReactiveSingleResult();
	}

	@Override
	public long getResultCount() {
		throw LOG.nonReactiveMethodCall( "getReactiveResultCount()" );
	}

	@Override
	public CompletionStage<Long> getReactiveResultCount() {
		return selectionQueryDelegate
				.getReactiveResultsCount( ( (SqmSelectStatement<?>) getSqmStatement() ).createCountQuery(), this );
	}

	@Override
	public CompletionStage<R> getReactiveSingleResultOrNull() {
		return selectionQueryDelegate.getReactiveSingleResultOrNull();
	}

	@Override
	public CompletionStage<Optional<R>> reactiveUniqueResultOptional() {
		return selectionQueryDelegate.reactiveUniqueResultOptional();
	}

	@Override
	public CompletionStage<List<R>> reactiveList() {
		return selectionQueryDelegate.reactiveList();
	}

	@Override
	public R getSingleResult() {
		return selectionQueryDelegate.getSingleResult();
	}

	@Override
	public R getSingleResultOrNull() {
		return selectionQueryDelegate.getSingleResultOrNull();
	}

	@Override
	public Callback getCallback() {
		return selectionQueryDelegate.getCallback();
	}

	@Override
	public R uniqueResult() {
		return selectionQueryDelegate.uniqueResult();
	}

	@Override
	public Optional<R> uniqueResultOptional() {
		return selectionQueryDelegate.uniqueResultOptional();
	}

	@Override
	public List<R> getResultList() {
		return selectionQueryDelegate.getResultList();
	}

	@Override
	public Stream<R> getResultStream() {
		return selectionQueryDelegate.getResultStream();
	}

	private CompletionStage<List<R>> doReactiveList() {
		verifySelect();
		getSession().prepareForQueryExecution( requiresTxn( getQueryOptions().getLockOptions().findGreatestLockMode() ) );

		final SqmSelectStatement<?> sqmStatement = (SqmSelectStatement<?>) getSqmStatement();
		final boolean containsCollectionFetches = sqmStatement.containsCollectionFetches();
		final boolean hasLimit = hasLimit( sqmStatement, getQueryOptions() );
		final boolean needsDistinct = containsCollectionFetches
				&& ( sqmStatement.usesDistinct() || hasAppliedGraph( getQueryOptions() ) || hasLimit );

		final DomainQueryExecutionContext executionContextToUse = executionContextForDoList( containsCollectionFetches, hasLimit, needsDistinct );

		return resolveSelectReactiveQueryPlan()
				.reactivePerformList( executionContextToUse )
				.thenApply( (List<R> list) -> needsDistinct
						? applyDistinct( sqmStatement, hasLimit, list )
						: list
				);
	}

	private List<R> applyDistinct(SqmSelectStatement<?> sqmStatement, boolean hasLimit, List<R> list) {
		final int first = !hasLimit || getQueryOptions().getLimit().getFirstRow() == null
				? getIntegerLiteral( sqmStatement.getOffset(), 0 )
				: getQueryOptions().getLimit().getFirstRow();
		final int max = !hasLimit || getQueryOptions().getLimit().getMaxRows() == null
				? getMaxRows( sqmStatement, list.size() )
				: getQueryOptions().getLimit().getMaxRows();
		if ( first > 0 || max != -1 ) {
			final int resultSize = list.size();
			final int toIndex = max != -1
					? first + max
					: resultSize;
			return list.subList( first, Math.min( toIndex, resultSize ) );
		}
		return list;
	}

	private ReactiveSelectQueryPlan<R> resolveSelectReactiveQueryPlan() {
		final QueryInterpretationCache.Key cacheKey = SqmInterpretationsKey.createInterpretationsKey( this );
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

	@Override
	protected ReactiveSelectQueryPlan<R> buildSelectQueryPlan() {
		final SqmSelectStatement<R>[] concreteSqmStatements =
				QuerySplitter.split( (SqmSelectStatement<R>) getSqmStatement() );

		return concreteSqmStatements.length > 1
			? buildAggregatedSelectQueryPlan( concreteSqmStatements )
			: buildConcreteSelectQueryPlan( concreteSqmStatements[0], getResultType(), getQueryOptions() );
	}

	private ReactiveSelectQueryPlan<R> buildAggregatedSelectQueryPlan(SqmSelectStatement<?>[] concreteSqmStatements) {
		@SuppressWarnings("unchecked")
		final ReactiveSelectQueryPlan<R>[] aggregatedQueryPlans = new ReactiveSelectQueryPlan[ concreteSqmStatements.length ];

		// todo (6.0) : we want to make sure that certain thing (ResultListTransformer, etc) only get applied at the aggregator-level

		for ( int i = 0, x = concreteSqmStatements.length; i < x; i++ ) {
			aggregatedQueryPlans[i] = buildConcreteSelectQueryPlan( concreteSqmStatements[i], getResultType(), getQueryOptions() );
		}

		return new AggregatedSelectReactiveQueryPlan<>( aggregatedQueryPlans );
	}

	private <T> ReactiveSelectQueryPlan<T> buildConcreteSelectQueryPlan(
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


	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// Update / delete / insert query execution

	@Override
	public int executeUpdate() {
		throw LOG.nonReactiveMethodCall( "executeReactiveUpdate" );
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate() {
		verifyUpdate();
		getSession().checkTransactionNeededForUpdateOperation( "Executing an update/delete query" );
		beforeQuery();
		return doExecuteReactiveUpdate()
				.handle( (count, error) -> {
					handleException( error );
					return count;
				} )
				.whenComplete( (rs, throwable) -> afterQuery( throwable == null ) );
	}

	public CompletionStage<Integer> doExecuteReactiveUpdate() {
		getSession().prepareForQueryExecution( true );
		return resolveNonSelectQueryPlan().executeReactiveUpdate( this );
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

	private ReactiveNonSelectQueryPlan resolveNonSelectQueryPlan() {
		// resolve (or make) the QueryPlan.

		ReactiveNonSelectQueryPlan queryPlan = null;

		final QueryInterpretationCache.Key cacheKey = SqmInterpretationsKey.generateNonSelectKey( this );
		if ( cacheKey != null ) {
			queryPlan = (ReactiveNonSelectQueryPlan) getSession().getFactory().getQueryEngine()
					.getInterpretationCache().getNonSelectQueryPlan( cacheKey );
		}

		if ( queryPlan == null ) {
			queryPlan = buildNonSelectQueryPlan();
			if ( cacheKey != null ) {
				getSession().getFactory().getQueryEngine().getInterpretationCache().cacheNonSelectQueryPlan( cacheKey, queryPlan );
			}
		}

		return queryPlan;
	}

	private ReactiveNonSelectQueryPlan buildNonSelectQueryPlan() {
		// to get here the SQM statement has already been validated to be
		// a non-select variety...
		if ( getSqmStatement() instanceof SqmDeleteStatement<?> ) {
			return buildDeleteQueryPlan();
		}

		if ( getSqmStatement() instanceof SqmUpdateStatement<?> ) {
			return buildUpdateQueryPlan();
		}

		if ( getSqmStatement() instanceof SqmInsertStatement<?> ) {
			return buildInsertQueryPlan();
		}

		throw new UnsupportedOperationException( "Query#executeUpdate for Statements of type [" + getSqmStatement() + "not yet supported" );
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private ReactiveNonSelectQueryPlan buildDeleteQueryPlan() {
		final SqmDeleteStatement[] concreteSqmStatements = QuerySplitter
				.split( (SqmDeleteStatement) getSqmStatement() );

		return concreteSqmStatements.length > 1
				? buildAggregatedDeleteQueryPlan( concreteSqmStatements )
				: buildConcreteDeleteQueryPlan( concreteSqmStatements[0] );
	}

	private ReactiveNonSelectQueryPlan buildConcreteDeleteQueryPlan(@SuppressWarnings("rawtypes") SqmDeleteStatement sqmDelete) {
		final EntityDomainType<?> entityDomainType = sqmDelete.getTarget().getModel();
		final String entityNameToDelete = entityDomainType.getHibernateEntityName();
		final EntityPersister entityDescriptor = getSessionFactory().getRuntimeMetamodels()
				.getMappingMetamodel()
				.getEntityDescriptor( entityNameToDelete );
		final ReactiveSqmMultiTableMutationStrategy multiTableStrategy = (ReactiveSqmMultiTableMutationStrategy) entityDescriptor.getSqmMultiTableMutationStrategy();
		return multiTableStrategy == null
			? new ReactiveSimpleDeleteQueryPlan( entityDescriptor, sqmDelete, getDomainParameterXref() )
			: new ReactiveMultiTableDeleteQueryPlan( sqmDelete, getDomainParameterXref(), multiTableStrategy );
	}

	private ReactiveNonSelectQueryPlan buildAggregatedDeleteQueryPlan(@SuppressWarnings("rawtypes") SqmDeleteStatement[] concreteSqmStatements) {
		final ReactiveNonSelectQueryPlan[] aggregatedQueryPlans = new ReactiveNonSelectQueryPlan[ concreteSqmStatements.length ];

		for ( int i = 0, x = concreteSqmStatements.length; i < x; i++ ) {
			aggregatedQueryPlans[i] = buildConcreteDeleteQueryPlan( concreteSqmStatements[i] );
		}

		return new ReactiveAggregatedNonSelectQueryPlan( aggregatedQueryPlans );
	}

	private ReactiveNonSelectQueryPlan buildUpdateQueryPlan() {
		//noinspection rawtypes
		final SqmUpdateStatement sqmUpdate = (SqmUpdateStatement) getSqmStatement();

		final String entityNameToUpdate = sqmUpdate.getTarget().getModel().getHibernateEntityName();
		final EntityPersister entityDescriptor = getSessionFactory().getRuntimeMetamodels()
				.getMappingMetamodel()
				.getEntityDescriptor( entityNameToUpdate );

		final ReactiveSqmMultiTableMutationStrategy multiTableStrategy = (ReactiveSqmMultiTableMutationStrategy) entityDescriptor.getSqmMultiTableMutationStrategy();
		return multiTableStrategy == null
			? new ReactiveSimpleUpdateQueryPlan( sqmUpdate, getDomainParameterXref() )
			: new ReactiveMultiTableUpdateQueryPlan( sqmUpdate, getDomainParameterXref(), multiTableStrategy );
	}

	private ReactiveNonSelectQueryPlan buildInsertQueryPlan() {
		//noinspection rawtypes
		final SqmInsertStatement sqmInsert = (SqmInsertStatement) getSqmStatement();

		final String entityNameToInsert = sqmInsert.getTarget().getModel().getHibernateEntityName();
		final EntityPersister persister = getSessionFactory()
				.getMappingMetamodel().getEntityDescriptor( entityNameToInsert );

		boolean useMultiTableInsert = persister.hasMultipleTables();
		if ( !useMultiTableInsert && !isSimpleValuesInsert( sqmInsert, persister ) ) {
			final Generator identifierGenerator = persister.getGenerator();

			if ( identifierGenerator instanceof BulkInsertionCapableIdentifierGenerator && identifierGenerator instanceof OptimizableGenerator ) {
				final Optimizer optimizer = ( (OptimizableGenerator) identifierGenerator ).getOptimizer();
				if ( optimizer != null && optimizer.getIncrementSize() > 1 ) {
					useMultiTableInsert = !hasIdentifierAssigned( sqmInsert, persister );
				}
			}
		}
		if ( !useMultiTableInsert ) {
			return new ReactiveSimpleInsertQueryPlan( sqmInsert, getDomainParameterXref() );
		}
		else {
			return new ReactiveMultiTableInsertQueryPlan(
					sqmInsert,
					getDomainParameterXref(),
					(ReactiveSqmMultiTableInsertStrategy) persister.getSqmMultiTableInsertStrategy()
			);
		}
	}

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// QueryOptions

	@Override
	public ReactiveQuerySqmImpl<R> setHint(String hintName, Object value) {
		super.setHint( hintName, value );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> addQueryHint(String hint) {
		super.addQueryHint( hint );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setLockOptions(LockOptions lockOptions) {
		super.setLockOptions( lockOptions );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setLockMode(String alias, LockMode lockMode) {
		super.setLockMode( alias, lockMode );
		return this;
	}

	@Override
	public <T> ReactiveQuerySqmImpl<T> setTupleTransformer(TupleTransformer<T> transformer) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ReactiveQuerySqmImpl<R> setResultListTransformer(ResultListTransformer transformer) {
		super.setResultListTransformer( transformer );
		return this;
	}

	@Override @Deprecated
	public <T> ReactiveQuerySqmImpl<T> setResultTransformer(ResultTransformer<T> transformer) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ReactiveQuerySqmImpl<R> setMaxResults(int maxResult) {
		super.setMaxResults( maxResult );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setFirstResult(int startPosition) {
		super.setFirstResult( startPosition );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setHibernateFlushMode(FlushMode flushMode) {
		super.setHibernateFlushMode( flushMode );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setFlushMode(FlushModeType flushMode) {
		super.setFlushMode( flushMode );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setLockMode(LockModeType lockMode) {
		super.setLockMode( lockMode );
		return this;
	}

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// covariance

	@Override
	public ReactiveQuerySqmImpl<R> applyGraph(RootGraph graph, GraphSemantic semantic) {
		super.applyGraph( graph, semantic );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> applyLoadGraph(RootGraph graph) {
		super.applyLoadGraph( graph );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> applyFetchGraph(RootGraph graph) {
		super.applyFetchGraph( graph );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setComment(String comment) {
		super.setComment( comment );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setCacheMode(CacheMode cacheMode) {
		super.setCacheMode( cacheMode );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode) {
		super.setCacheRetrieveMode( cacheRetrieveMode );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setCacheStoreMode(CacheStoreMode cacheStoreMode) {
		super.setCacheStoreMode( cacheStoreMode );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setCacheable(boolean cacheable) {
		super.setCacheable( cacheable );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setCacheRegion(String cacheRegion) {
		super.setCacheRegion( cacheRegion );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setHibernateLockMode(LockMode lockMode) {
		super.setHibernateLockMode( lockMode );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setTimeout(int timeout) {
		super.setTimeout( timeout );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setFetchSize(int fetchSize) {
		super.setFetchSize( fetchSize );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setReadOnly(boolean readOnly) {
		super.setReadOnly( readOnly );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setProperties(Object bean) {
		super.setProperties( bean );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setProperties(Map bean) {
		super.setProperties( bean );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameter(String name, Object value) {
		super.setParameter( name, value );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameter(String name, P value, Class<P> javaType) {
		super.setParameter( name, value, javaType );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameter(String name, P value, Type<P> type) {
		super.setParameter( name, value, type );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameter(String name, Instant value, TemporalType temporalType) {
		super.setParameter( name, value, temporalType );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameter(int position, Object value) {
		super.setParameter( position, value );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameter(int position, P value, Class<P> javaType) {
		super.setParameter( position, value, javaType );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameter(int position, P value, Type<P> type) {
		super.setParameter( position, value, type );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameter(int position, Instant value, TemporalType temporalType) {
		super.setParameter( position, value, temporalType );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameter(QueryParameter<P> parameter, P value) {
		super.setParameter( parameter, value );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameter(QueryParameter<P> parameter, P value, Class<P> javaType) {
		super.setParameter( parameter, value, javaType );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameter(QueryParameter<P> parameter, P value, Type<P> type) {
		super.setParameter( parameter, value, type );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameter(Parameter<P> parameter, P value) {
		super.setParameter( parameter, value );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameter(Parameter<Calendar> param, Calendar value, TemporalType temporalType) {
		super.setParameter( param, value, temporalType );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameter(Parameter<Date> param, Date value, TemporalType temporalType) {
		super.setParameter( param, value, temporalType );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameter(String name, Calendar value, TemporalType temporalType) {
		super.setParameter( name, value, temporalType );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameter(String name, Date value, TemporalType temporalType) {
		super.setParameter( name, value, temporalType );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameter(int position, Calendar value, TemporalType temporalType) {
		super.setParameter( position, value, temporalType );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameter(int position, Date value, TemporalType temporalType) {
		super.setParameter( position, value, temporalType );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameterList(String name, Collection values) {
		super.setParameterList( name, values );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(String name, Collection<? extends P> values, Class<P> javaType) {
		super.setParameterList( name, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(String name, Collection<? extends P> values, Type<P> type) {
		super.setParameterList( name, values, type );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameterList(String name, Object[] values) {
		super.setParameterList( name, values );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(String name, P[] values, Class<P> javaType) {
		super.setParameterList( name, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(String name, P[] values, Type<P> type) {
		super.setParameterList( name, values, type );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameterList(int position, Collection values) {
		super.setParameterList( position, values );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(int position, Collection<? extends P> values, Class<P> javaType) {
		super.setParameterList( position, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(int position, Collection<? extends P> values, Type<P> type) {
		super.setParameterList( position, values, type );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setParameterList(int position, Object[] values) {
		super.setParameterList( position, values );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(int position, P[] values, Class<P> javaType) {
		super.setParameterList( position, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(int position, P[] values, Type<P> type) {
		super.setParameterList( position, values, type );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values) {
		super.setParameterList( parameter, values );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, Class<P> javaType) {
		super.setParameterList( parameter, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, Type<P> type) {
		super.setParameterList( parameter, values, type );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(QueryParameter<P> parameter, P[] values) {
		super.setParameterList( parameter, values );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(QueryParameter<P> parameter, P[] values, Class<P> javaType) {
		super.setParameterList( parameter, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveQuerySqmImpl<R> setParameterList(QueryParameter<P> parameter, P[] values, Type<P> type) {
		super.setParameterList( parameter, values, type );
		return this;
	}

	@Override
	public ReactiveQuerySqmImpl<R> setFollowOnLocking(boolean enable) {
		super.setFollowOnLocking( enable );
		return this;
	}

	@Override
	public void applyGraph(RootGraphImplementor<?> graph, GraphSemantic semantic) {
		super.applyGraph( graph, semantic );
	}

	@Override
	public ReactiveQuerySqmImpl<R> enableFetchProfile(String profileName) {
		selectionQueryDelegate.enableFetchProfile( profileName );
		return this;
	}
}
