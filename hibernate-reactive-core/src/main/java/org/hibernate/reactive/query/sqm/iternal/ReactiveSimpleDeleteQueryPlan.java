/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.iternal;

import java.lang.invoke.MethodHandles;
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
import org.hibernate.metamodel.mapping.MappingModelHelper;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.QueryParameterImplementor;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.internal.SimpleDeleteQueryPlan;
import org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter;
import org.hibernate.query.sqm.internal.SqmUtil;
import org.hibernate.query.sqm.mutation.internal.SqmMutationStrategyHelper;
import org.hibernate.query.sqm.spi.SqmParameterMappingModelResolutionAccess;
import org.hibernate.query.sqm.sql.SqmTranslation;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.query.sqm.tree.expression.SqmParameter;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.sql.spi.ReactiveNonSelectQueryPlan;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveMutationExecutor;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.tree.delete.DeleteStatement;
import org.hibernate.sql.ast.tree.expression.Expression;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.from.MutatingTableReferenceGroupWrapper;
import org.hibernate.sql.ast.tree.predicate.InSubQueryPredicate;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.exec.spi.JdbcDelete;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.results.internal.SqlSelectionImpl;

/**
 * @see
 */
public class ReactiveSimpleDeleteQueryPlan extends SimpleDeleteQueryPlan implements ReactiveNonSelectQueryPlan {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private JdbcDelete jdbcDelete;
	private EntityMappingType entityDescriptor;
	private SqmDeleteStatement<?> sqmDelete;
	private DomainParameterXref domainParameterXref;

	private Map<QueryParameterImplementor<?>, Map<SqmParameter<?>, List<List<JdbcParameter>>>> jdbcParamsXref;

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
	public CompletionStage<Integer> executeReactiveUpdate(DomainQueryExecutionContext executionContext) {
		BulkOperationCleanupAction.schedule( executionContext.getSession(), sqmDelete );
		final SharedSessionContractImplementor session = executionContext.getSession();
		final SessionFactoryImplementor factory = session.getFactory();
		SqlAstTranslator<JdbcDelete> deleteTranslator = null;
		if ( jdbcDelete == null ) {
			deleteTranslator = createDeleteTranslator( executionContext );
		}

		// FIXME: These is ugly, but they are both initialized in `super.createDeleteTranslator`
		SqmTranslation<DeleteStatement> sqmInterpretation = getSqmInterpretation();
		Map<QueryParameterImplementor<?>, Map<SqmParameter<?>, List<List<JdbcParameter>>>> jdbcParamsXref1 = getJdbcParamsXref();

		final JdbcParameterBindings jdbcParameterBindings = SqmUtil.createJdbcParameterBindings(
				executionContext.getQueryParameterBindings(),
				domainParameterXref,
				jdbcParamsXref,
				factory.getRuntimeMetamodels().getMappingMetamodel(),
				sqmInterpretation.getFromClauseAccess()::findTableGroup,
				new SqmParameterMappingModelResolutionAccess() {
					@Override @SuppressWarnings("unchecked")
					public <T> MappingModelExpressible<T> getResolvedMappingModelType(SqmParameter<T> parameter) {
						return (MappingModelExpressible<T>) sqmInterpretation.getSqmParameterMappingModelTypeResolutions().get(parameter);
					}
				},
				session
		);

		if ( jdbcDelete != null
				&& ! jdbcDelete.isCompatibleWith( jdbcParameterBindings, executionContext.getQueryOptions() ) ) {
			deleteTranslator = createDeleteTranslator( executionContext );
		}

		if ( deleteTranslator != null ) {
			jdbcDelete = deleteTranslator.translate( jdbcParameterBindings, executionContext.getQueryOptions() );
		}
		else {
			jdbcDelete.bindFilterJdbcParameters( jdbcParameterBindings );
		}

		final boolean missingRestriction = sqmDelete.getWhereClause() == null
				|| sqmDelete.getWhereClause().getPredicate() == null;
		if ( missingRestriction ) {
			assert domainParameterXref.getSqmParameterCount() == 0;
			assert jdbcParamsXref.isEmpty();
		}

		final SqmJdbcExecutionContextAdapter executionContextAdapter = SqmJdbcExecutionContextAdapter
				.usingLockingAndPaging( executionContext );

		SqmMutationStrategyHelper.cleanUpCollectionTables(
				entityDescriptor,
				(tableReference, attributeMapping) -> {
					if ( missingRestriction ) {
						return null;
					}

					final ForeignKeyDescriptor fkDescriptor = attributeMapping.getKeyDescriptor();
					final Expression fkColumnExpression = MappingModelHelper.buildColumnReferenceExpression(
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
					final Expression fkTargetColumnExpression = MappingModelHelper.buildColumnReferenceExpression(
							tableGroup,
							fkDescriptor.getTargetPart(),
							sqmInterpretation.getSqlExpressionResolver(),
							factory
					);
					matchingIdSubQuery.getSelectClause().addSqlSelection( new SqlSelectionImpl( 1, 0, fkTargetColumnExpression ) );

					matchingIdSubQuery.getFromClause().addRoot(
							tableGroup
					);

					matchingIdSubQuery.applyPredicate( sqmInterpretation.getSqlAst().getRestriction() );

					return new InSubQueryPredicate( fkColumnExpression, matchingIdSubQuery, false );
				},
				( missingRestriction ? JdbcParameterBindings.NO_BINDINGS : jdbcParameterBindings ),
				executionContextAdapter
		);

		// FIXME: Should we get this from the service registry like ORM?
		return StandardReactiveMutationExecutor.INSTANCE
				.executeReactiveUpdate(
						jdbcDelete,
						jdbcParameterBindings,
						session.getJdbcCoordinator().getStatementPreparer()::prepareStatement,
						ReactiveSimpleDeleteQueryPlan::expectationCheck,
						executionContextAdapter
				);
	}

	private static void expectationCheck(Integer integer, PreparedStatement preparedStatement) {
	}

}
