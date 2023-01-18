/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.cte;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.MappingModelExpressible;
import org.hibernate.metamodel.mapping.SqlExpressible;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter;
import org.hibernate.query.sqm.internal.SqmUtil;
import org.hibernate.query.sqm.mutation.internal.MatchingIdSelectionHelper;
import org.hibernate.query.sqm.mutation.internal.MultiTableSqmMutationConverter;
import org.hibernate.query.sqm.mutation.internal.cte.AbstractCteMutationHandler;
import org.hibernate.query.sqm.mutation.internal.cte.CteMutationStrategy;
import org.hibernate.query.sqm.tree.SqmDeleteOrUpdateStatement;
import org.hibernate.query.sqm.tree.expression.SqmExpression;
import org.hibernate.query.sqm.tree.expression.SqmParameter;
import org.hibernate.query.sqm.tree.expression.SqmStar;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveAbstractMutationHandler;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.tree.cte.CteContainer;
import org.hibernate.sql.ast.tree.cte.CteMaterialization;
import org.hibernate.sql.ast.tree.cte.CteStatement;
import org.hibernate.sql.ast.tree.cte.CteTable;
import org.hibernate.sql.ast.tree.cte.CteTableGroup;
import org.hibernate.sql.ast.tree.expression.Expression;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.from.NamedTableReference;
import org.hibernate.sql.ast.tree.predicate.Predicate;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.basic.BasicResult;
import org.hibernate.sql.results.internal.SqlSelectionImpl;
import org.hibernate.type.spi.TypeConfiguration;

/**
 * @see org.hibernate.query.sqm.mutation.internal.cte.AbstractCteMutationHandler
 */
public interface ReactiveAbstractCteMutationHandler extends ReactiveAbstractMutationHandler {

	CteTable getCteTable();

	DomainParameterXref getDomainParameterXref();

	CteMutationStrategy getStrategy();

	void addDmlCtes(
			CteContainer statement,
			CteStatement idSelectCte,
			MultiTableSqmMutationConverter sqmConverter,
			Map<SqmParameter<?>, List<JdbcParameter>> parameterResolutions,
			SessionFactoryImplementor factory);

	/**
	 * @see org.hibernate.query.sqm.mutation.internal.cte.AbstractCteMutationHandler#execute(DomainQueryExecutionContext)
	 */
	@Override
	default CompletionStage<Integer> reactiveExecute(DomainQueryExecutionContext executionContext) {
		final SqmDeleteOrUpdateStatement sqmMutationStatement = getSqmDeleteOrUpdateStatement();
		final SessionFactoryImplementor factory = executionContext.getSession().getFactory();
		final EntityMappingType entityDescriptor = getEntityDescriptor();
		final String explicitDmlTargetAlias;
		// We need an alias because we try to acquire a WRITE lock for these rows in the CTE
		if ( sqmMutationStatement.getTarget().getExplicitAlias() == null ) {
			explicitDmlTargetAlias = "dml_target";
		}
		else {
			explicitDmlTargetAlias = sqmMutationStatement.getTarget().getExplicitAlias();
		}

		final MultiTableSqmMutationConverter sqmConverter = new MultiTableSqmMutationConverter(
				entityDescriptor,
				sqmMutationStatement,
				sqmMutationStatement.getTarget(),
				explicitDmlTargetAlias,
				getDomainParameterXref(),
				executionContext.getQueryOptions(),
				executionContext.getSession().getLoadQueryInfluencers(),
				executionContext.getQueryParameterBindings(),
				factory
		);
		final Map<SqmParameter<?>, List<JdbcParameter>> parameterResolutions;
		if ( getDomainParameterXref().getSqmParameterCount() == 0 ) {
			parameterResolutions = Collections.emptyMap();
		}
		else {
			parameterResolutions = new IdentityHashMap<>();
		}

		//noinspection rawtypes
		final Map<SqmParameter, MappingModelExpressible> paramTypeResolutions = new LinkedHashMap<>();

		final Predicate restriction = sqmConverter.visitWhereClause(
				sqmMutationStatement.getWhereClause(),
				columnReference -> {
				},
				(sqmParam, mappingType, jdbcParameters) -> paramTypeResolutions.put( sqmParam, mappingType )
		);
		sqmConverter.pruneTableGroupJoins();

		final CteStatement idSelectCte = new CteStatement(
				getCteTable(),
				MatchingIdSelectionHelper.generateMatchingIdSelectStatement(
						entityDescriptor,
						sqmMutationStatement,
						true,
						restriction,
						sqmConverter,
						executionContext,
						factory
				),
				// The id-select cte will be reused multiple times
				CteMaterialization.MATERIALIZED
		);

		// Create the main query spec that will return the count of
		final QuerySpec querySpec = new QuerySpec( true, 1 );
		final List<DomainResult<?>> domainResults = new ArrayList<>( 1 );
		final SelectStatement statement = new SelectStatement( querySpec, domainResults );
		final JdbcServices jdbcServices = factory.getJdbcServices();
		final SqlAstTranslator<JdbcOperationQuerySelect> translator = jdbcServices.getJdbcEnvironment()
				.getSqlAstTranslatorFactory()
				.buildSelectTranslator( factory, statement );

		final Expression count = createCountStar( factory, sqmConverter );
		domainResults.add(
				new BasicResult<>(
						0,
						null,
						( (SqlExpressible) count ).getJdbcMapping()
				)
		);
		querySpec.getSelectClause().addSqlSelection( new SqlSelectionImpl( 1, 0, count ) );
		querySpec.getFromClause().addRoot(
				new CteTableGroup(
						new NamedTableReference(
								idSelectCte.getCteTable().getTableExpression(),
								AbstractCteMutationHandler.CTE_TABLE_IDENTIFIER
						)
				)
		);

		// Add all CTEs
		statement.addCteStatement( idSelectCte );
		addDmlCtes( statement, idSelectCte, sqmConverter, parameterResolutions, factory );

		final JdbcParameterBindings jdbcParameterBindings = SqmUtil.createJdbcParameterBindings(
				executionContext.getQueryParameterBindings(),
				getDomainParameterXref(),
				SqmUtil.generateJdbcParamsXref( getDomainParameterXref(), sqmConverter ),
				factory.getRuntimeMetamodels().getMappingMetamodel(),
				navigablePath -> sqmConverter.getMutatingTableGroup(),
				paramTypeResolutions::get,
				executionContext.getSession()
		);
		final LockOptions lockOptions = executionContext.getQueryOptions().getLockOptions();
		final LockMode lockMode = lockOptions.getAliasSpecificLockMode( explicitDmlTargetAlias );
		// Acquire a WRITE lock for the rows that are about to be modified
		lockOptions.setAliasSpecificLockMode( explicitDmlTargetAlias, LockMode.WRITE );
		final JdbcOperationQuerySelect select = translator.translate(
				jdbcParameterBindings,
				executionContext.getQueryOptions()
		);
		lockOptions.setAliasSpecificLockMode( explicitDmlTargetAlias, lockMode );
		executionContext.getSession().autoFlushIfRequired( select.getAffectedTableNames() );

		return StandardReactiveSelectExecutor.INSTANCE.list(
						select,
						jdbcParameterBindings,
						SqmJdbcExecutionContextAdapter.omittingLockingAndPaging( executionContext ),
						row -> row[0],
						ReactiveListResultsConsumer.UniqueSemantic.NONE
				)
				.thenApply( list -> ( (Number) list.get( 0 ) ).intValue() );
	}

	/**
	 * Copy and paste of the one in the super class
	 */
	default Expression createCountStar(
			SessionFactoryImplementor factory,
			MultiTableSqmMutationConverter sqmConverter) {
		final SqmExpression<?> arg = new SqmStar( factory.getNodeBuilder() );
		final TypeConfiguration typeConfiguration = factory.getJpaMetamodel().getTypeConfiguration();
		return factory.getQueryEngine()
				.getSqmFunctionRegistry()
				.findFunctionDescriptor( "count" )
				.generateSqmExpression( arg, null, factory.getQueryEngine(), typeConfiguration )
				.convertToSqlAst( sqmConverter );
	}
}
