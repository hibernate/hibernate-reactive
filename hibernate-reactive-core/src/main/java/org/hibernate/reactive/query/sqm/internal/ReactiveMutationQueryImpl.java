/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.internal;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.dialect.sql.ast.spi.ValuesListSupport;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.id.BulkInsertionCapableIdentifierGenerator;
import org.hibernate.id.OptimizableGenerator;
import org.hibernate.metamodel.model.domain.EntityDomainType;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.query.internal.MutationQueryImpl;
import org.hibernate.query.named.internal.CriteriaMutationMementoImpl;
import org.hibernate.query.named.internal.HqlMutationMementoImpl;
import org.hibernate.query.named.spi.NamedQueryMemento;
import org.hibernate.query.spi.HqlInterpretation;
import org.hibernate.query.sqm.internal.SqmInterpretationsKey;
import org.hibernate.query.sqm.tree.spi.SqmCopyContext;
import org.hibernate.query.sqm.tree.spi.SqmDmlStatement;
import org.hibernate.query.sqm.tree.spi.SqmStatement;
import org.hibernate.query.sqm.tree.spi.delete.SqmDeleteStatement;
import org.hibernate.query.sqm.tree.spi.insert.SqmInsertStatement;
import org.hibernate.query.sqm.tree.spi.insert.SqmInsertValuesStatement;
import org.hibernate.query.sqm.tree.spi.insert.SqmValues;
import org.hibernate.query.sqm.tree.spi.update.SqmUpdateStatement;
import org.hibernate.reactive.logging.internal.Log;
import org.hibernate.reactive.logging.internal.LoggerFactory;
import org.hibernate.reactive.query.ReactiveSelectionQuery;
import org.hibernate.reactive.query.sql.spi.ReactiveNonSelectQueryPlan;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveSqmMultiTableInsertStrategy;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveSqmMultiTableMutationStrategy;
import org.hibernate.reactive.session.ReactiveSqmQueryImplementor;

import static org.hibernate.query.hql.internal.QuerySplitter.split;

/**
 * A reactive mutation query backed by HQL/JPQL or criteria.
 * Mirrors {@link MutationQueryImpl} with reactive execution support.
 *
 * @param <R> the result type (target entity type for DML)
 */
public class ReactiveMutationQueryImpl<R> extends MutationQueryImpl<R> implements ReactiveSqmQueryImplementor<R> {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveMutationQueryImpl(
			String hql,
			HqlInterpretation<R> hqlInterpretation,
			Class<R> expectedResultType,
			SharedSessionContractImplementor session) {
		super( hql, hqlInterpretation, expectedResultType, session );
	}

	public ReactiveMutationQueryImpl(
			HqlMutationMementoImpl<R> memento,
			HqlInterpretation<R> interpretation,
			Class<R> resultType,
			SharedSessionContractImplementor session) {
		super( memento, interpretation, resultType, session );
	}

	public ReactiveMutationQueryImpl(
			CriteriaMutationMementoImpl<R> memento,
			SqmDmlStatement<R> criteria,
			SharedSessionContractImplementor session) {
		super( memento, criteria, session );
	}

	public ReactiveMutationQueryImpl(
			SqmDmlStatement<R> criteria,
			SharedSessionContractImplementor session) {
		super( criteria, session );
	}

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// Reactive mutation execution

	@Override
	public int execute() {
		throw LOG.nonReactiveMethodCall( "reactiveExecute" );
	}

	@Override
	public CompletionStage<Integer> reactiveExecute() {
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
		final var cacheKey = SqmInterpretationsKey.generateNonSelectKey( this );
		if ( cacheKey != null ) {
			final var cached = (ReactiveNonSelectQueryPlan) getInterpretationCache().getNonSelectQueryPlan( cacheKey );
			if ( cached != null ) {
				return cached;
			}
		}
		final var queryPlan = buildNonSelectQueryPlan();
		if ( cacheKey != null ) {
			getInterpretationCache().cacheNonSelectQueryPlan( cacheKey, queryPlan );
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
		final SqmDeleteStatement[] concreteSqmStatements = split( (SqmDeleteStatement) getSqmStatement() );
		return concreteSqmStatements.length > 1
				? buildAggregatedDeleteQueryPlan( concreteSqmStatements )
				: buildConcreteDeleteQueryPlan( concreteSqmStatements[0] );
	}

	private ReactiveNonSelectQueryPlan buildConcreteDeleteQueryPlan(@SuppressWarnings("rawtypes") SqmDeleteStatement sqmDelete) {
		final EntityDomainType<?> entityDomainType = sqmDelete.getTarget().getModel();
		final String entityNameToDelete = entityDomainType.getHibernateEntityName();
		final EntityPersister entityDescriptor = getMappingMetamodel().getEntityDescriptor( entityNameToDelete );
		final ReactiveSqmMultiTableMutationStrategy multiTableStrategy =
				(ReactiveSqmMultiTableMutationStrategy) entityDescriptor.getSqmMultiTableMutationStrategy();
		return multiTableStrategy == null
				? new ReactiveSimpleDeleteQueryPlan( entityDescriptor, sqmDelete, getDomainParameterXref() )
				: new ReactiveMultiTableDeleteQueryPlan( sqmDelete, getDomainParameterXref(), multiTableStrategy );
	}

	private ReactiveNonSelectQueryPlan buildAggregatedDeleteQueryPlan(@SuppressWarnings("rawtypes") SqmDeleteStatement[] concreteSqmStatements) {
		final ReactiveNonSelectQueryPlan[] aggregatedQueryPlans = new ReactiveNonSelectQueryPlan[concreteSqmStatements.length];
		for ( int i = 0, x = concreteSqmStatements.length; i < x; i++ ) {
			aggregatedQueryPlans[i] = buildConcreteDeleteQueryPlan( concreteSqmStatements[i] );
		}
		return new ReactiveAggregatedNonSelectQueryPlan( aggregatedQueryPlans );
	}

	private ReactiveNonSelectQueryPlan buildUpdateQueryPlan() {
		//noinspection rawtypes
		final SqmUpdateStatement sqmUpdate = (SqmUpdateStatement) getSqmStatement();
		final String entityNameToUpdate = sqmUpdate.getTarget().getModel().getHibernateEntityName();
		final EntityPersister entityDescriptor = getMappingMetamodel().getEntityDescriptor( entityNameToUpdate );
		final ReactiveSqmMultiTableMutationStrategy multiTableStrategy =
				(ReactiveSqmMultiTableMutationStrategy) entityDescriptor.getSqmMultiTableMutationStrategy();
		return multiTableStrategy == null
				? new ReactiveSimpleNonSelectQueryPlan( sqmUpdate, getDomainParameterXref() )
				: new ReactiveMultiTableUpdateQueryPlan( sqmUpdate, getDomainParameterXref(), multiTableStrategy );
	}

	private ReactiveNonSelectQueryPlan buildInsertQueryPlan() {
		@SuppressWarnings("unchecked")
		final SqmInsertStatement<R> sqmInsert = (SqmInsertStatement<R>) getSqmStatement();
		final String entityNameToInsert = sqmInsert.getTarget().getModel().getHibernateEntityName();
		final EntityPersister persister = getMappingMetamodel().getEntityDescriptor( entityNameToInsert );

		if ( useMultiTableInsert( persister, sqmInsert ) ) {
			return new ReactiveMultiTableInsertQueryPlan(
					sqmInsert,
					getDomainParameterXref(),
					(ReactiveSqmMultiTableInsertStrategy) persister.getSqmMultiTableInsertStrategy()
			);
		}
		else if ( sqmInsert instanceof SqmInsertValuesStatement<R> insertValues
				&& insertValues.getValuesList().size() != 1
				&& !getSessionFactory().getJdbcServices().getDialect().getValuesListSupport().supports( ValuesListSupport.Context.INSERT ) ) {
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

	private boolean useMultiTableInsert(EntityPersister persister, SqmInsertStatement<R> sqmInsert) {
		boolean useMultiTableInsert = persister.hasMultipleTables();
		if ( !useMultiTableInsert && !isSimpleValuesInsert( sqmInsert, persister ) ) {
			final var identifierGenerator = persister.getGenerator();
			if ( identifierGenerator instanceof BulkInsertionCapableIdentifierGenerator bulkGenerator
					&& identifierGenerator instanceof OptimizableGenerator optimizableGenerator ) {
				final var optimizer = optimizableGenerator.getOptimizer();
				if ( ( optimizer != null && optimizer.getIncrementSize() > 1 )
						|| !bulkGenerator.supportsBulkInsertionIdentifierGeneration() ) {
					useMultiTableInsert = !hasIdentifierAssigned( sqmInsert, persister );
				}
			}
		}
		return useMultiTableInsert;
	}

	@Override
	public NamedQueryMemento<?> toMemento(String name) {
		return null;
	}

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
	public ReactiveMutationQueryImpl<R> setHint(String hintName, Object value) {
		super.setHint( hintName, value );
		return this;
	}

	@Override
	public ReactiveMutationQueryImpl<R> setComment(String comment) {
		super.setComment( comment );
		return this;
	}

	@Override
	public ReactiveMutationQueryImpl<R> addQueryHint(String hint) {
		super.addQueryHint( hint );
		return this;
	}

	@Override
	public ReactiveMutationQueryImpl<R> setMaxResults(int maxResult) {
		super.setMaxResults( maxResult );
		return this;
	}

	@Override
	public ReactiveMutationQueryImpl<R> setFirstResult(int startPosition) {
		super.setFirstResult( startPosition );
		return this;
	}

	@Override
	public ReactiveMutationQueryImpl<R> setCacheable(boolean cacheable) {
		super.setCacheable( cacheable );
		return this;
	}

	@Override
	public ReactiveMutationQueryImpl<R> setCacheRegion(String cacheRegion) {
		super.setCacheRegion( cacheRegion );
		return this;
	}

	@Override
	public ReactiveMutationQueryImpl<R> setHibernateLockMode(LockMode lockMode) {
		// Not applicable for mutation queries
		return this;
	}

	@Override
	public ReactiveMutationQueryImpl<R> setTimeout(int timeout) {
		super.setTimeout( timeout );
		return this;
	}

	@Override
	public ReactiveMutationQueryImpl<R> setFetchSize(int fetchSize) {
		// Not applicable for mutation queries
		return this;
	}

	@Override
	public ReactiveMutationQueryImpl<R> setReadOnly(boolean readOnly) {
		// Not applicable for mutation queries
		return this;
	}

	@Override
	public ReactiveMutationQueryImpl<R> setProperties(Object bean) {
		super.setProperties( bean );
		return this;
	}

	@Override
	public ReactiveMutationQueryImpl<R> setProperties(Map bean) {
		super.setProperties( bean );
		return this;
	}

	@Override
	public LockOptions getLockOptions() {
		return null;
	}

	@Override
	public ReactiveSqmQueryImplementor<R> setLockOptions(LockOptions lockOptions) {
		return null;
	}

	@Override
	public ReactiveSqmQueryImplementor<R> setLockMode(String alias, LockMode lockMode) {
		return null;
	}

	@Override
	public ReactiveSelectionQuery<R> setFollowOnLocking(boolean enable) {
		return null;
	}

	@Override
	public void applyGraph(RootGraphImplementor<?> graph, GraphSemantic semantic) {

	}
}
