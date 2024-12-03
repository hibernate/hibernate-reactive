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
import org.hibernate.query.spi.QueryParameterImplementor;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter;
import org.hibernate.query.sqm.internal.SqmUtil;
import org.hibernate.query.sqm.spi.SqmParameterMappingModelResolutionAccess;
import org.hibernate.query.sqm.sql.SqmTranslation;
import org.hibernate.query.sqm.tree.expression.SqmParameter;
import org.hibernate.query.sqm.tree.insert.SqmInsertStatement;
import org.hibernate.reactive.query.sql.spi.ReactiveNonSelectQueryPlan;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveJdbcMutationExecutor;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.tree.MutationStatement;
import org.hibernate.sql.exec.spi.JdbcOperationQueryMutation;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.exec.spi.JdbcParametersList;

/**
 * @see org.hibernate.query.sqm.internal.SimpleInsertQueryPlan
 */
public class ReactiveSimpleInsertQueryPlan implements ReactiveNonSelectQueryPlan {

	private final SqmInsertStatement<?> sqmInsert;

	private final DomainParameterXref domainParameterXref;

	private Map<SqmParameter<?>, MappingModelExpressible<?>> paramTypeResolutions;

	private JdbcOperationQueryMutation jdbcInsert;
	private Map<QueryParameterImplementor<?>, Map<SqmParameter<?>, List<JdbcParametersList>>> jdbcParamsXref;

	public ReactiveSimpleInsertQueryPlan(
			SqmInsertStatement<?> sqmInsert,
			DomainParameterXref domainParameterXref) {
		this.sqmInsert = sqmInsert;
		this.domainParameterXref = domainParameterXref;
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate(DomainQueryExecutionContext executionContext) {
		BulkOperationCleanupAction.schedule( executionContext.getSession(), sqmInsert );
		final SharedSessionContractImplementor session = executionContext.getSession();
		SqlAstTranslator<? extends JdbcOperationQueryMutation> insertTranslator = jdbcInsert == null
				? createInsertTranslator( executionContext )
				: null;

		final JdbcParameterBindings jdbcParameterBindings = SqmUtil.createJdbcParameterBindings(
				executionContext.getQueryParameterBindings(),
				domainParameterXref,
				jdbcParamsXref,
				new SqmParameterMappingModelResolutionAccess() {
					@Override @SuppressWarnings("unchecked")
					public <T> MappingModelExpressible<T> getResolvedMappingModelType(SqmParameter<T> parameter) {
						return (MappingModelExpressible<T>) paramTypeResolutions.get(parameter);
					}
				},
				session
		);

		if ( jdbcInsert != null && !jdbcInsert.isCompatibleWith( jdbcParameterBindings, executionContext.getQueryOptions() ) ) {
			insertTranslator = createInsertTranslator( executionContext );
		}

		if ( insertTranslator != null ) {
			jdbcInsert = insertTranslator.translate( jdbcParameterBindings, executionContext.getQueryOptions() );
		}

		return StandardReactiveJdbcMutationExecutor.INSTANCE
				.executeReactive(
						jdbcInsert,
						jdbcParameterBindings,
						session.getJdbcCoordinator().getStatementPreparer()::prepareStatement,
						(i, ps) -> {},
						SqmJdbcExecutionContextAdapter.omittingLockingAndPaging( executionContext )
				);
	}

	// Copied from Hibernate ORM SimpleInsertQueryPlan#createInsertTranslator
	private SqlAstTranslator<? extends JdbcOperationQueryMutation> createInsertTranslator(DomainQueryExecutionContext executionContext) {
		final SessionFactoryImplementor factory = executionContext.getSession().getFactory();

		final SqmTranslation<? extends MutationStatement> sqmInterpretation = factory.getQueryEngine().getSqmTranslatorFactory()
				.createMutationTranslator(
						sqmInsert,
						executionContext.getQueryOptions(),
						domainParameterXref,
						executionContext.getQueryParameterBindings(),
						executionContext.getSession().getLoadQueryInfluencers(),
						factory
				)
				.translate();

		this.jdbcParamsXref = SqmUtil.generateJdbcParamsXref(
				domainParameterXref,
				sqmInterpretation::getJdbcParamsBySqmParam
		);

		this.paramTypeResolutions = sqmInterpretation.getSqmParameterMappingModelTypeResolutions();

		return factory.getJdbcServices()
				.getJdbcEnvironment()
				.getSqlAstTranslatorFactory()
				.buildMutationTranslator( factory, sqmInterpretation.getSqlAst() );
	}
}
