/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.internal;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.action.internal.BulkOperationCleanupAction;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.MappingModelExpressible;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.QueryEngine;
import org.hibernate.query.spi.QueryParameterImplementor;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter;
import org.hibernate.query.sqm.internal.SqmUtil;
import org.hibernate.query.sqm.spi.SqmParameterMappingModelResolutionAccess;
import org.hibernate.query.sqm.sql.SqmTranslation;
import org.hibernate.query.sqm.sql.SqmTranslator;
import org.hibernate.query.sqm.sql.SqmTranslatorFactory;
import org.hibernate.query.sqm.tree.expression.SqmParameter;
import org.hibernate.query.sqm.tree.update.SqmUpdateStatement;
import org.hibernate.reactive.query.sql.spi.ReactiveNonSelectQueryPlan;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveJdbcMutationExecutor;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.spi.FromClauseAccess;
import org.hibernate.sql.ast.tree.MutationStatement;
import org.hibernate.sql.exec.spi.JdbcOperationQueryMutation;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.exec.spi.JdbcParametersList;


/**
 * @see org.hibernate.query.sqm.internal.SimpleUpdateQueryPlan
 */
public class ReactiveSimpleUpdateQueryPlan implements ReactiveNonSelectQueryPlan {

	private final SqmUpdateStatement<?> sqmUpdate;
	private final DomainParameterXref domainParameterXref;

	private JdbcOperationQueryMutation jdbcUpdate;
	private FromClauseAccess tableGroupAccess;
	private Map<QueryParameterImplementor<?>, Map<SqmParameter<?>, List<JdbcParametersList>>> jdbcParamsXref;
	private Map<SqmParameter<?>, MappingModelExpressible<?>> sqmParamMappingTypeResolutions;

	public ReactiveSimpleUpdateQueryPlan(SqmUpdateStatement<?> sqmUpdate, DomainParameterXref domainParameterXref) {
		this.sqmUpdate = sqmUpdate;
		this.domainParameterXref = domainParameterXref;
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate(DomainQueryExecutionContext executionContext) {
		BulkOperationCleanupAction.schedule( executionContext.getSession(), sqmUpdate );
		final SharedSessionContractImplementor session = executionContext.getSession();
		SqlAstTranslator<? extends JdbcOperationQueryMutation> updateTranslator = jdbcUpdate == null
				? createUpdateTranslator( executionContext )
				: null;
		final JdbcParameterBindings jdbcParameterBindings = SqmUtil.createJdbcParameterBindings(
				executionContext.getQueryParameterBindings(),
				domainParameterXref,
				jdbcParamsXref,
				new SqmParameterMappingModelResolutionAccess() {
					@Override @SuppressWarnings("unchecked")
					public <T> MappingModelExpressible<T> getResolvedMappingModelType(SqmParameter<T> parameter) {
						return (MappingModelExpressible<T>) sqmParamMappingTypeResolutions.get(parameter);
					}
				},
				session
		);

		if ( jdbcUpdate != null && !jdbcUpdate.isCompatibleWith( jdbcParameterBindings, executionContext.getQueryOptions() ) ) {
			updateTranslator = createUpdateTranslator( executionContext );
		}

		if ( updateTranslator != null ) {
			jdbcUpdate = updateTranslator.translate( jdbcParameterBindings, executionContext.getQueryOptions() );
		}

		return StandardReactiveJdbcMutationExecutor.INSTANCE
				.executeReactive(
						jdbcUpdate,
						jdbcParameterBindings,
						session.getJdbcCoordinator().getStatementPreparer()::prepareStatement,
						(i, ps) -> {},
						SqmJdbcExecutionContextAdapter.omittingLockingAndPaging( executionContext )
				);
	}

	// I can probably change ORM to reuse this
	private SqlAstTranslator<? extends JdbcOperationQueryMutation> createUpdateTranslator(DomainQueryExecutionContext executionContext) {
		final SessionFactoryImplementor factory = executionContext.getSession().getFactory();
		final QueryEngine queryEngine = factory.getQueryEngine();

		final SqmTranslatorFactory translatorFactory = queryEngine.getSqmTranslatorFactory();
		final SqmTranslator<? extends MutationStatement> translator = translatorFactory.createMutationTranslator(
				sqmUpdate,
				executionContext.getQueryOptions(),
				domainParameterXref,
				executionContext.getQueryParameterBindings(),
				executionContext.getSession().getLoadQueryInfluencers(),
				factory
		);

		final SqmTranslation<? extends MutationStatement> sqmInterpretation = translator.translate();
		tableGroupAccess = sqmInterpretation.getFromClauseAccess();
		this.jdbcParamsXref = SqmUtil
				.generateJdbcParamsXref( domainParameterXref, sqmInterpretation::getJdbcParamsBySqmParam );
		this.sqmParamMappingTypeResolutions = sqmInterpretation.getSqmParameterMappingModelTypeResolutions();
		return factory.getJdbcServices().getJdbcEnvironment().getSqlAstTranslatorFactory()
				.buildMutationTranslator( factory, sqmInterpretation.getSqlAst() );
	}
}
