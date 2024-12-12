/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.internal;

import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.action.internal.BulkOperationCleanupAction;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.ForeignKeyDescriptor;
import org.hibernate.metamodel.mapping.MappingModelExpressible;
import org.hibernate.metamodel.mapping.SoftDeleteMapping;
import org.hibernate.metamodel.mapping.internal.MappingModelCreationHelper;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.QueryParameterImplementor;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.internal.SimpleDeleteQueryPlan;
import org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter;
import org.hibernate.query.sqm.internal.SqmUtil;
import org.hibernate.query.sqm.spi.SqmParameterMappingModelResolutionAccess;
import org.hibernate.query.sqm.sql.SqmTranslation;
import org.hibernate.query.sqm.sql.SqmTranslator;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.query.sqm.tree.expression.SqmParameter;
import org.hibernate.reactive.query.sql.spi.ReactiveNonSelectQueryPlan;
import org.hibernate.reactive.query.sqm.mutation.internal.ReactiveSqmMutationStrategyHelper;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveJdbcMutationExecutor;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.tree.AbstractUpdateOrDeleteStatement;
import org.hibernate.sql.ast.tree.MutationStatement;
import org.hibernate.sql.ast.tree.expression.ColumnReference;
import org.hibernate.sql.ast.tree.expression.Expression;
import org.hibernate.sql.ast.tree.expression.JdbcLiteral;
import org.hibernate.sql.ast.tree.from.MutatingTableReferenceGroupWrapper;
import org.hibernate.sql.ast.tree.from.NamedTableReference;
import org.hibernate.sql.ast.tree.predicate.InSubQueryPredicate;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.ast.tree.update.Assignment;
import org.hibernate.sql.ast.tree.update.UpdateStatement;
import org.hibernate.sql.exec.spi.JdbcOperationQueryMutation;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.exec.spi.JdbcParametersList;
import org.hibernate.sql.results.internal.SqlSelectionImpl;

import static org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter.usingLockingAndPaging;

public class ReactiveSimpleDeleteQueryPlan extends SimpleDeleteQueryPlan implements ReactiveNonSelectQueryPlan {
	private final EntityMappingType entityDescriptor;
	private final SqmDeleteStatement<?> sqmDelete;
	private final DomainParameterXref domainParameterXref;

	private JdbcOperationQueryMutation jdbcOperation;

	private SqmTranslation<? extends AbstractUpdateOrDeleteStatement> sqmInterpretation;
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
	protected SqlAstTranslator<? extends JdbcOperationQueryMutation> createTranslator(DomainQueryExecutionContext executionContext) {
		final SessionFactoryImplementor factory = executionContext.getSession().getFactory();
		final SqmTranslator<? extends MutationStatement> translator = factory.getQueryEngine().getSqmTranslatorFactory().createMutationTranslator(
				sqmDelete,
				executionContext.getQueryOptions(),
				domainParameterXref,
				executionContext.getQueryParameterBindings(),
				executionContext.getSession().getLoadQueryInfluencers(),
				factory
		);

		sqmInterpretation = (SqmTranslation<? extends AbstractUpdateOrDeleteStatement>) translator.translate();

		this.jdbcParamsXref = SqmUtil.generateJdbcParamsXref(
				domainParameterXref,
				sqmInterpretation::getJdbcParamsBySqmParam
		);

		return factory.getJdbcServices()
				.getJdbcEnvironment()
				.getSqlAstTranslatorFactory()
				.buildMutationTranslator( factory, mutationStatement() );
	}

	private MutationStatement mutationStatement() {
		if ( entityDescriptor.getSoftDeleteMapping() == null ) {
			return sqmInterpretation.getSqlAst();
		}
		final AbstractUpdateOrDeleteStatement sqlDeleteAst = sqmInterpretation.getSqlAst();
		final NamedTableReference targetTable = sqlDeleteAst.getTargetTable();
		final SoftDeleteMapping columnMapping = getEntityDescriptor().getSoftDeleteMapping();
		final ColumnReference columnReference = new ColumnReference( targetTable, columnMapping );
		//noinspection rawtypes,unchecked
		final JdbcLiteral jdbcLiteral = new JdbcLiteral(
				columnMapping.getDeletedLiteralValue(),
				columnMapping.getJdbcMapping()
		);
		final Assignment assignment = new Assignment( columnReference, jdbcLiteral );

		return new UpdateStatement(
				targetTable,
				Collections.singletonList( assignment ),
				sqlDeleteAst.getRestriction()
		);
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate(DomainQueryExecutionContext executionContext) {
		BulkOperationCleanupAction.schedule( executionContext.getSession(), sqmDelete );
		final SharedSessionContractImplementor session = executionContext.getSession();
		final SessionFactoryImplementor factory = session.getFactory();
		SqlAstTranslator<? extends JdbcOperationQueryMutation> sqlAstTranslator = null;
		if ( jdbcOperation == null ) {
			sqlAstTranslator = createTranslator( executionContext );
		}

		final JdbcParameterBindings jdbcParameterBindings = SqmUtil.createJdbcParameterBindings(
				executionContext.getQueryParameterBindings(),
				domainParameterXref,
				jdbcParamsXref,
				new SqmParameterMappingModelResolutionAccess() {
					@Override @SuppressWarnings("unchecked")
					public <T> MappingModelExpressible<T> getResolvedMappingModelType(SqmParameter<T> parameter) {
						return (MappingModelExpressible<T>) sqmInterpretation.getSqmParameterMappingModelTypeResolutions().get(parameter);
					}
				},
				session
		);

		if ( jdbcOperation != null
				&& !jdbcOperation.isCompatibleWith( jdbcParameterBindings, executionContext.getQueryOptions() ) ) {
			sqlAstTranslator = createTranslator( executionContext );
		}

		if ( sqlAstTranslator != null ) {
			jdbcOperation = sqlAstTranslator.translate( jdbcParameterBindings, executionContext.getQueryOptions() );
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
							matchingIdSubQuery.getSelectClause()
									.addSqlSelection( new SqlSelectionImpl( 0, fkTargetColumnExpression ) );

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
								jdbcOperation,
								jdbcParameterBindings,
								session.getJdbcCoordinator().getStatementPreparer()::prepareStatement,
								ReactiveSimpleDeleteQueryPlan::doNothing,
								executionContextAdapter )
				);
	}

	private static void doNothing(Integer i, PreparedStatement ps) {
	}
}
