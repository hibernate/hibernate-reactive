/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.iternal;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.action.internal.BulkOperationCleanupAction;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.ForeignKeyDescriptor;
import org.hibernate.metamodel.mapping.MappingModelExpressible;
import org.hibernate.metamodel.mapping.internal.MappingModelCreationHelper;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.QueryEngine;
import org.hibernate.query.spi.QueryParameterImplementor;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.internal.SimpleDeleteQueryPlan;
import org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter;
import org.hibernate.query.sqm.internal.SqmUtil;
import org.hibernate.query.sqm.spi.SqmParameterMappingModelResolutionAccess;
import org.hibernate.query.sqm.sql.SqmTranslation;
import org.hibernate.query.sqm.sql.SqmTranslator;
import org.hibernate.query.sqm.sql.SqmTranslatorFactory;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.query.sqm.tree.expression.SqmParameter;
import org.hibernate.reactive.query.sql.spi.ReactiveNonSelectQueryPlan;
import org.hibernate.reactive.query.sqm.mutation.internal.ReactiveSqmMutationStrategyHelper;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveJdbcMutationExecutor;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.tree.delete.DeleteStatement;
import org.hibernate.sql.ast.tree.expression.Expression;
import org.hibernate.sql.ast.tree.from.MutatingTableReferenceGroupWrapper;
import org.hibernate.sql.ast.tree.from.NamedTableReference;
import org.hibernate.sql.ast.tree.predicate.InSubQueryPredicate;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.exec.spi.JdbcOperationQueryDelete;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.exec.spi.JdbcParametersList;
import org.hibernate.sql.results.internal.SqlSelectionImpl;

import static org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter.usingLockingAndPaging;

public class ReactiveSimpleDeleteQueryPlan extends SimpleDeleteQueryPlan implements ReactiveNonSelectQueryPlan {

	private JdbcOperationQueryDelete jdbcDelete;
	private final EntityMappingType entityDescriptor;
	private final SqmDeleteStatement<?> sqmDelete;
	private final DomainParameterXref domainParameterXref;

	private SqmTranslation<DeleteStatement> sqmInterpretation;

	private Map<QueryParameterImplementor<?>, Map<SqmParameter<?>, List<JdbcParametersList>>> jdbcParamsXref;

	public ReactiveSimpleDeleteQueryPlan(
			EntityMappingType entityDescriptor,
			SqmDeleteStatement<?> sqmDelete,
			DomainParameterXref domainParameterXref) {
		super( entityDescriptor, sqmDelete, domainParameterXref );
		this.entityDescriptor = entityDescriptor;
		this.sqmDelete = sqmDelete;
		this.domainParameterXref = domainParameterXref;
	}

	@Override
	protected SqlAstTranslator<JdbcOperationQueryDelete> createDeleteTranslator(DomainQueryExecutionContext executionContext) {
		final SessionFactoryImplementor factory = executionContext.getSession().getFactory();
		final QueryEngine queryEngine = factory.getQueryEngine();

		final SqmTranslatorFactory translatorFactory = queryEngine.getSqmTranslatorFactory();
		final SqmTranslator<DeleteStatement> translator = translatorFactory.createSimpleDeleteTranslator(
				sqmDelete,
				executionContext.getQueryOptions(),
				domainParameterXref,
				executionContext.getQueryParameterBindings(),
				executionContext.getSession().getLoadQueryInfluencers(),
				factory
		);

		sqmInterpretation = translator.translate();

		this.jdbcParamsXref = SqmUtil.generateJdbcParamsXref(
				domainParameterXref,
				sqmInterpretation::getJdbcParamsBySqmParam
		);

		return factory.getJdbcServices().getJdbcEnvironment().getSqlAstTranslatorFactory()
				.buildDeleteTranslator( factory, sqmInterpretation.getSqlAst() );
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate(DomainQueryExecutionContext executionContext) {
		BulkOperationCleanupAction.schedule( executionContext.getSession(), sqmDelete );
		final SharedSessionContractImplementor session = executionContext.getSession();
		final SessionFactoryImplementor factory = session.getFactory();
		SqlAstTranslator<JdbcOperationQueryDelete> deleteTranslator = null;
		if ( jdbcDelete == null ) {
			deleteTranslator = createDeleteTranslator( executionContext );
		}

		final JdbcParameterBindings jdbcParameterBindings = SqmUtil.createJdbcParameterBindings(
				executionContext.getQueryParameterBindings(),
				domainParameterXref,
				jdbcParamsXref,
				factory.getRuntimeMetamodels().getMappingMetamodel(),
				sqmInterpretation.getFromClauseAccess()::findTableGroup,
				new SqmParameterMappingModelResolutionAccess() {
					@Override
					@SuppressWarnings("unchecked")
					public <T> MappingModelExpressible<T> getResolvedMappingModelType(SqmParameter<T> parameter) {
						return (MappingModelExpressible<T>) sqmInterpretation.getSqmParameterMappingModelTypeResolutions()
								.get( parameter );
					}
				},
				session
		);

		if ( jdbcDelete != null
				&& !jdbcDelete.isCompatibleWith( jdbcParameterBindings, executionContext.getQueryOptions() ) ) {
			deleteTranslator = createDeleteTranslator( executionContext );
		}

		if ( deleteTranslator != null ) {
			jdbcDelete = deleteTranslator.translate( jdbcParameterBindings, executionContext.getQueryOptions() );
		}

		final boolean missingRestriction = sqmDelete.getWhereClause() == null
				|| sqmDelete.getWhereClause().getPredicate() == null;
		if ( missingRestriction ) {
			assert domainParameterXref.getSqmParameterCount() == 0;
			assert jdbcParamsXref.isEmpty();
		}

		final SqmJdbcExecutionContextAdapter executionContextAdapter = usingLockingAndPaging( executionContext );

		return ReactiveSqmMutationStrategyHelper.cleanUpCollectionTables(
						entityDescriptor,
						(tableReference, attributeMapping) -> {
							if ( missingRestriction ) {
								return null;
							}

							final ForeignKeyDescriptor fkDescriptor = attributeMapping.getKeyDescriptor();
							final Expression fkColumnExpression = MappingModelCreationHelper.buildColumnReferenceExpression(
									new MutatingTableReferenceGroupWrapper(
											new NavigablePath( attributeMapping.getRootPathName() ),
											attributeMapping,
											(NamedTableReference) tableReference
									),
									fkDescriptor.getKeyPart(),
									null,
									factory
							);

							final QuerySpec matchingIdSubQuery = new QuerySpec( false );

							final MutatingTableReferenceGroupWrapper tableGroup = new MutatingTableReferenceGroupWrapper(
									new NavigablePath( attributeMapping.getRootPathName() ),
									attributeMapping,
									sqmInterpretation.getSqlAst().getTargetTable()
							);
							final Expression fkTargetColumnExpression = MappingModelCreationHelper.buildColumnReferenceExpression(
									tableGroup,
									fkDescriptor.getTargetPart(),
									sqmInterpretation.getSqlExpressionResolver(),
									factory
							);
							matchingIdSubQuery.getSelectClause().addSqlSelection( new SqlSelectionImpl(
									1,
									0,
									fkTargetColumnExpression
							) );

							matchingIdSubQuery.getFromClause().addRoot(
									tableGroup
							);

							matchingIdSubQuery.applyPredicate( sqmInterpretation.getSqlAst().getRestriction() );

							return new InSubQueryPredicate( fkColumnExpression, matchingIdSubQuery, false );
						},
						( missingRestriction ? JdbcParameterBindings.NO_BINDINGS : jdbcParameterBindings ),
						executionContextAdapter
				)
				.thenCompose( unused -> StandardReactiveJdbcMutationExecutor.INSTANCE
						.executeReactive(
								jdbcDelete,
								jdbcParameterBindings,
								session.getJdbcCoordinator().getStatementPreparer()::prepareStatement,
								ReactiveSimpleDeleteQueryPlan::doNothing,
								executionContextAdapter )
				);
	}

	private static void doNothing(Integer i, PreparedStatement ps) {
	}
}
