/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
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
import org.hibernate.metamodel.model.domain.EntityDomainType;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.query.IllegalQueryOperationException;
import org.hibernate.query.QueryFlushMode;
import org.hibernate.query.QueryParameter;
import org.hibernate.query.ResultListTransformer;
import org.hibernate.query.TupleTransformer;
import org.hibernate.query.internal.MutationQueryImpl;
import org.hibernate.query.named.NamedSqmQueryMemento;
import org.hibernate.query.spi.HqlInterpretation;
import org.hibernate.query.sqm.tree.SqmDmlStatement;
import org.hibernate.query.sqm.tree.SqmStatement;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.query.sqm.tree.insert.SqmInsertStatement;
import org.hibernate.query.sqm.tree.insert.SqmInsertValuesStatement;
import org.hibernate.query.sqm.tree.insert.SqmValues;
import org.hibernate.query.sqm.tree.update.SqmUpdateStatement;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.sql.spi.ReactiveNonSelectQueryPlan;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveSqmMultiTableInsertStrategy;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveSqmMultiTableMutationStrategy;
import org.hibernate.reactive.session.ReactiveSqmQueryImplementor;
import org.hibernate.generator.Generator;
import org.hibernate.id.BulkInsertionCapableIdentifierGenerator;
import org.hibernate.id.OptimizableGenerator;
import org.hibernate.id.enhanced.Optimizer;
import org.hibernate.query.hql.internal.QuerySplitter;
import org.hibernate.query.spi.QueryInterpretationCache;
import org.hibernate.query.sqm.internal.SqmInterpretationsKey;
import org.hibernate.query.sqm.tree.SqmCopyContext;

import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;
import jakarta.persistence.metamodel.Type;


/**
 * A reactive mutation query backed by HQL/JPQL or criteria.
 * Mirrors {@link MutationQueryImpl} with reactive execution support.
 *
 * @param <R> the result type (target entity type for DML)
 */
public class ReactiveSqmQueryImpl<R> extends MutationQueryImpl<R> implements ReactiveSqmQueryImplementor<R> {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveSqmQueryImpl(
			String hql,
			HqlInterpretation<R> hqlInterpretation,
			Class<R> expectedResultType,
			SharedSessionContractImplementor session) {
		super( hql, hqlInterpretation, expectedResultType, session );
	}

	public ReactiveSqmQueryImpl(
			NamedSqmQueryMemento<R> memento,
			Class<R> resultType,
			SharedSessionContractImplementor session) {
		this( (SqmDmlStatement<R>) memento.getSqmStatement(), session );
	}

	public ReactiveSqmQueryImpl(
			SqmDmlStatement<R> criteria,
			SharedSessionContractImplementor session) {
		super( criteria, session );
	}

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// Reactive mutation execution

	@Override
	public int executeUpdate() {
		throw LOG.nonReactiveMethodCall( "executeReactiveUpdate" );
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate() {
		getSession().checkTransactionNeededForUpdateOperation( "Executing an update/delete query" );
		return doExecuteReactiveUpdate()
				.handle( (count, error) -> {
					handleException( error );
					return count;
				} );
	}

	private CompletionStage<Integer> doExecuteReactiveUpdate() {
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
						.convert( (HibernateException) e, getQueryOptions().getLockOptions() );
			}
			if ( e instanceof RuntimeException ) {
				throw (RuntimeException) e;
			}
			throw new HibernateException( e );
		}
	}

	private ReactiveNonSelectQueryPlan resolveNonSelectQueryPlan() {
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
		final SqmStatement<?> sqmStatement = getSqmStatement();
		if ( sqmStatement instanceof SqmDeleteStatement<?> ) {
			return buildDeleteQueryPlan();
		}
		if ( sqmStatement instanceof SqmUpdateStatement<?> ) {
			return buildUpdateQueryPlan();
		}
		if ( sqmStatement instanceof SqmInsertStatement<?> ) {
			return buildInsertQueryPlan();
		}
		throw new UnsupportedOperationException( "Query#executeUpdate for Statements of type [" + sqmStatement + "] not yet supported" );
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
		final ReactiveSqmMultiTableMutationStrategy multiTableStrategy =
				(ReactiveSqmMultiTableMutationStrategy) entityDescriptor.getSqmMultiTableMutationStrategy();
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
		final ReactiveSqmMultiTableMutationStrategy multiTableStrategy =
				(ReactiveSqmMultiTableMutationStrategy) entityDescriptor.getSqmMultiTableMutationStrategy();
		return multiTableStrategy == null
				? new ReactiveSimpleNonSelectQueryPlan( sqmUpdate, getDomainParameterXref() )
				: new ReactiveMultiTableUpdateQueryPlan( sqmUpdate, getDomainParameterXref(), multiTableStrategy );
	}

	private ReactiveNonSelectQueryPlan buildInsertQueryPlan() {
		//noinspection rawtypes
		final SqmInsertStatement<R> sqmInsert = (SqmInsertStatement<R>) getSqmStatement();
		final String entityNameToInsert = sqmInsert.getTarget().getModel().getHibernateEntityName();
		final EntityPersister persister = getSessionFactory()
				.getMappingMetamodel().getEntityDescriptor( entityNameToInsert );

		boolean useMultiTableInsert = persister.hasMultipleTables();
		if ( !useMultiTableInsert && !isSimpleValuesInsert( sqmInsert, persister ) ) {
			final Generator identifierGenerator = persister.getGenerator();
			if ( identifierGenerator instanceof BulkInsertionCapableIdentifierGenerator
					&& identifierGenerator instanceof OptimizableGenerator ) {
				final Optimizer optimizer = ( (OptimizableGenerator) identifierGenerator ).getOptimizer();
				if ( optimizer != null && optimizer.getIncrementSize() > 1 ) {
					useMultiTableInsert = !hasIdentifierAssigned( sqmInsert, persister );
				}
			}
		}
		if ( useMultiTableInsert ) {
			return new ReactiveMultiTableInsertQueryPlan(
					sqmInsert,
					getDomainParameterXref(),
					(ReactiveSqmMultiTableInsertStrategy) persister.getSqmMultiTableInsertStrategy()
			);
		}
		else if ( sqmInsert instanceof SqmInsertValuesStatement<R> insertValues
				&& insertValues.getValuesList().size() != 1
				&& !getSessionFactory().getJdbcServices().getDialect().supportsValuesListForInsert() ) {
			final List<SqmValues> valuesList = insertValues.getValuesList();
			final ReactiveNonSelectQueryPlan[] planParts = new ReactiveNonSelectQueryPlan[valuesList.size()];
			for ( int i = 0; i < valuesList.size(); i++ ) {
				final SqmInsertValuesStatement<?> subInsert =
						insertValues.copyWithoutValues( SqmCopyContext.simpleContext() );
				subInsert.values( valuesList.get( i ) );
				planParts[i] = new ReactiveSimpleNonSelectQueryPlan( subInsert, getDomainParameterXref() );
			}
			return new ReactiveAggregatedNonSelectQueryPlan( planParts );
		}

		return new ReactiveSimpleNonSelectQueryPlan( sqmInsert, getDomainParameterXref() );
	}

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// ReactiveSelectionQuery — stubs and delegation (not applicable for DML queries)

	@Override
	public LockOptions getLockOptions() {
		return getQueryOptions().getLockOptions();
	}

	@Override
	public LockModeType getLockMode() {
		return super.getLockMode();
	}

	@Override
	public LockMode getHibernateLockMode() {
		return getQueryOptions().getLockOptions().getLockMode();
	}

	@Override
	public String getQueryString() {
		return super.getQueryString();
	}

	@Override
	public Integer getFetchSize() {
		return null;
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	public int getFirstResult() {
		return 0;
	}

	@Override
	public int getMaxResults() {
		return Integer.MAX_VALUE;
	}

	@Override
	public CacheMode getCacheMode() {
		return super.getCacheMode();
	}

	@Override
	public CacheStoreMode getCacheStoreMode() {
		return super.getCacheStoreMode();
	}

	@Override
	public CacheRetrieveMode getCacheRetrieveMode() {
		return super.getCacheRetrieveMode();
	}

	@Override
	public boolean isCacheable() {
		return super.isCacheable();
	}

	@Override
	public String getCacheRegion() {
		return super.getCacheRegion();
	}

	@Override
	public void applyGraph(org.hibernate.graph.spi.RootGraphImplementor<?> graph, org.hibernate.graph.GraphSemantic semantic) {
		// not applicable for mutation queries
	}

	@Override
	public ReactiveSqmQueryImpl<R> enableFetchProfile(String profileName) {
		return this;
	}

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// ReactiveSelectionQuery — stubs (not applicable for DML queries)

	@Override
	public CompletionStage<List<R>> reactiveList() {
		throw LOG.nonReactiveMethodCall( "executeReactiveUpdate" );
	}

	@Override
	public CompletionStage<R> getReactiveSingleResult() {
		throw LOG.nonReactiveMethodCall( "executeReactiveUpdate" );
	}

	@Override
	public CompletionStage<R> getReactiveSingleResultOrNull() {
		throw LOG.nonReactiveMethodCall( "executeReactiveUpdate" );
	}

	@Override
	public CompletionStage<Long> getReactiveResultCount() {
		throw LOG.nonReactiveMethodCall( "executeReactiveUpdate" );
	}

	@Override
	public CompletionStage<R> reactiveUnique() {
		throw LOG.nonReactiveMethodCall( "executeReactiveUpdate" );
	}

	@Override
	public CompletionStage<Optional<R>> reactiveUniqueResultOptional() {
		throw LOG.nonReactiveMethodCall( "executeReactiveUpdate" );
	}

	@Override
	public List<R> getResultList() {
		throw LOG.nonReactiveMethodCall( "executeReactiveUpdate" );
	}

	@Override
	public Stream<R> getResultStream() {
		throw LOG.nonReactiveMethodCall( "executeReactiveUpdate" );
	}

	@Override
	public R getSingleResult() {
		throw LOG.nonReactiveMethodCall( "executeReactiveUpdate" );
	}

	@Override
	public R getSingleResultOrNull() {
		throw LOG.nonReactiveMethodCall( "executeReactiveUpdate" );
	}

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// Covariant overrides

	@Override
	public ReactiveSqmQueryImpl<R> setHint(String hintName, Object value) {
		super.setHint( hintName, value );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setComment(String comment) {
		super.setComment( comment );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> addQueryHint(String hint) {
		super.addQueryHint( hint );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setLockOptions(LockOptions lockOptions) {
		// MutationQueryImpl doesn't support lock options directly
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setLockMode(String alias, LockMode lockMode) {
		// Not applicable for mutation queries
		return this;
	}

	@Override
	public <T> ReactiveSqmQueryImpl<T> setTupleTransformer(TupleTransformer<T> transformer) {
		throw new UnsupportedOperationException( "Not supported for mutation queries" );
	}

	@Override
	public ReactiveSqmQueryImpl<R> setResultListTransformer(ResultListTransformer<R> transformer) {
		// Not applicable for mutation queries
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setMaxResults(int maxResult) {
		super.setMaxResults( maxResult );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setFirstResult(int startPosition) {
		super.setFirstResult( startPosition );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setHibernateFlushMode(FlushMode flushMode) {
		setQueryFlushMode( QueryFlushMode.fromHibernateMode( flushMode ) );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setFlushMode(FlushModeType flushMode) {
		super.setFlushMode( flushMode );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setLockMode(LockModeType lockMode) {
		// Mutation queries have limited lock mode support
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setFollowOnLocking(boolean enable) {
		// Not applicable to mutation queries
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setCacheMode(CacheMode cacheMode) {
		super.setCacheMode( cacheMode );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode) {
		super.setCacheRetrieveMode( cacheRetrieveMode );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setCacheStoreMode(CacheStoreMode cacheStoreMode) {
		super.setCacheStoreMode( cacheStoreMode );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setCacheable(boolean cacheable) {
		super.setCacheable( cacheable );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setCacheRegion(String cacheRegion) {
		super.setCacheRegion( cacheRegion );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setHibernateLockMode(LockMode lockMode) {
		// Not applicable for mutation queries
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setTimeout(int timeout) {
		super.setTimeout( timeout );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setFetchSize(int fetchSize) {
		// Not applicable for mutation queries
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setReadOnly(boolean readOnly) {
		// Not applicable for mutation queries
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setProperties(Object bean) {
		super.setProperties( bean );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setProperties(Map bean) {
		super.setProperties( bean );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setParameter(String name, Object value) {
		super.setParameter( name, value );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameter(String name, P value, Class<P> javaType) {
		super.setParameter( name, value, javaType );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameter(String name, P value, Type<P> type) {
		super.setParameter( name, value, type );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setParameter(String name, Instant value, TemporalType temporalType) {
		super.setParameter( name, value, temporalType );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setParameter(String name, Calendar value, TemporalType temporalType) {
		super.setParameter( name, value, temporalType );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setParameter(String name, Date value, TemporalType temporalType) {
		super.setParameter( name, value, temporalType );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setParameter(int position, Object value) {
		super.setParameter( position, value );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameter(int position, P value, Class<P> javaType) {
		super.setParameter( position, value, javaType );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameter(int position, P value, Type<P> type) {
		super.setParameter( position, value, type );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setParameter(int position, Instant value, TemporalType temporalType) {
		super.setParameter( position, value, temporalType );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setParameter(int position, Calendar value, TemporalType temporalType) {
		super.setParameter( position, value, temporalType );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setParameter(int position, Date value, TemporalType temporalType) {
		super.setParameter( position, value, temporalType );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameter(QueryParameter<P> parameter, P value) {
		super.setParameter( parameter, value );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameter(QueryParameter<P> parameter, P value, Class<P> javaType) {
		super.setParameter( parameter, value, javaType );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameter(QueryParameter<P> parameter, P value, Type<P> type) {
		super.setParameter( parameter, value, type );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameter(Parameter<P> parameter, P value) {
		super.setParameter( parameter, value );
		return this;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public ReactiveSqmQueryImpl<R> setParameter(Parameter param, Calendar value, TemporalType temporalType) {
		super.setParameter( param, value, temporalType );
		return this;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public ReactiveSqmQueryImpl<R> setParameter(Parameter param, Date value, TemporalType temporalType) {
		super.setParameter( param, value, temporalType );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setParameterList(String name, Collection values) {
		super.setParameterList( name, values );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameterList(String name, Collection<? extends P> values, Class<P> javaType) {
		super.setParameterList( name, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameterList(String name, Collection<? extends P> values, Type<P> type) {
		super.setParameterList( name, values, type );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setParameterList(String name, Object[] values) {
		super.setParameterList( name, values );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameterList(String name, P[] values, Class<P> javaType) {
		super.setParameterList( name, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameterList(String name, P[] values, Type<P> type) {
		super.setParameterList( name, values, type );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setParameterList(int position, Collection values) {
		super.setParameterList( position, values );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameterList(int position, Collection<? extends P> values, Class<P> javaType) {
		super.setParameterList( position, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameterList(int position, Collection<? extends P> values, Type<P> type) {
		super.setParameterList( position, values, type );
		return this;
	}

	@Override
	public ReactiveSqmQueryImpl<R> setParameterList(int position, Object[] values) {
		super.setParameterList( position, values );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameterList(int position, P[] values, Class<P> javaType) {
		super.setParameterList( position, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameterList(int position, P[] values, Type<P> type) {
		super.setParameterList( position, values, type );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values) {
		super.setParameterList( parameter, values );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, Class<P> javaType) {
		super.setParameterList( parameter, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, Type<P> type) {
		super.setParameterList( parameter, values, type );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameterList(QueryParameter<P> parameter, P[] values) {
		super.setParameterList( parameter, values );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameterList(QueryParameter<P> parameter, P[] values, Class<P> javaType) {
		super.setParameterList( parameter, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveSqmQueryImpl<R> setParameterList(QueryParameter<P> parameter, P[] values, Type<P> type) {
		super.setParameterList( parameter, values, type );
		return this;
	}

	@Override
	public org.hibernate.query.named.NamedQueryMemento<?> toMemento(String name) {
		return toMutationMemento( name );
	}
}
