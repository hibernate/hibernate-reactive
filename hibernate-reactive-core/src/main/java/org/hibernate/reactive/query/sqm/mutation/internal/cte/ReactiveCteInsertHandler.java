/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.cte;

import java.lang.invoke.MethodHandles;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.dialect.temptable.TemporaryTable;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.id.BulkInsertionCapableIdentifierGenerator;
import org.hibernate.id.OptimizableGenerator;
import org.hibernate.id.enhanced.Optimizer;
import org.hibernate.internal.util.collections.Stack;
import org.hibernate.metamodel.mapping.BasicValuedMapping;
import org.hibernate.metamodel.mapping.MappingModelExpressible;
import org.hibernate.metamodel.mapping.SqlExpressible;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.query.results.internal.TableGroupImpl;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.BinaryArithmeticOperator;
import org.hibernate.query.sqm.ComparisonOperator;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter;
import org.hibernate.query.sqm.internal.SqmUtil;
import org.hibernate.query.sqm.mutation.internal.MultiTableSqmMutationConverter;
import org.hibernate.query.sqm.mutation.internal.SqmInsertStrategyHelper;
import org.hibernate.query.sqm.mutation.internal.cte.CteInsertHandler;
import org.hibernate.query.sqm.spi.SqmParameterMappingModelResolutionAccess;
import org.hibernate.query.sqm.sql.BaseSqmToSqlAstConverter;
import org.hibernate.query.sqm.sql.internal.SqmPathInterpretation;
import org.hibernate.query.sqm.tree.expression.SqmParameter;
import org.hibernate.query.sqm.tree.insert.SqmInsertSelectStatement;
import org.hibernate.query.sqm.tree.insert.SqmInsertStatement;
import org.hibernate.query.sqm.tree.insert.SqmInsertValuesStatement;
import org.hibernate.query.sqm.tree.insert.SqmValues;
import org.hibernate.reactive.engine.spi.ReactiveSharedSessionContractImplementor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.sqm.mutation.internal.ReactiveHandler;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.ast.SqlAstJoinType;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.spi.SqlAstProcessingState;
import org.hibernate.sql.ast.tree.Statement;
import org.hibernate.sql.ast.tree.cte.CteColumn;
import org.hibernate.sql.ast.tree.cte.CteMaterialization;
import org.hibernate.sql.ast.tree.cte.CteStatement;
import org.hibernate.sql.ast.tree.cte.CteTable;
import org.hibernate.sql.ast.tree.cte.CteTableGroup;
import org.hibernate.sql.ast.tree.expression.BinaryArithmeticExpression;
import org.hibernate.sql.ast.tree.expression.ColumnReference;
import org.hibernate.sql.ast.tree.expression.Expression;
import org.hibernate.sql.ast.tree.expression.QueryLiteral;
import org.hibernate.sql.ast.tree.expression.SelfRenderingSqlFragmentExpression;
import org.hibernate.sql.ast.tree.from.NamedTableReference;
import org.hibernate.sql.ast.tree.from.TableGroup;
import org.hibernate.sql.ast.tree.from.TableGroupJoin;
import org.hibernate.sql.ast.tree.from.ValuesTableGroup;
import org.hibernate.sql.ast.tree.insert.InsertSelectStatement;
import org.hibernate.sql.ast.tree.insert.Values;
import org.hibernate.sql.ast.tree.predicate.ComparisonPredicate;
import org.hibernate.sql.ast.tree.select.QueryPart;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.ast.tree.update.Assignment;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.basic.BasicResult;
import org.hibernate.sql.results.internal.SqlSelectionImpl;

public class ReactiveCteInsertHandler extends CteInsertHandler implements ReactiveHandler {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final SessionFactoryImplementor sessionFactory;

	public ReactiveCteInsertHandler(
			CteTable cteTable,
			SqmInsertStatement<?> sqmStatement,
			DomainParameterXref domainParameterXref,
			SessionFactoryImplementor sessionFactory) {
		super( cteTable, sqmStatement, domainParameterXref, sessionFactory );
		this.sessionFactory = sessionFactory;
	}

	@Override
	public int execute(DomainQueryExecutionContext executionContext) {
		throw LOG.nonReactiveMethodCall( "reactiveExecute" );
	}

	// Pretty much a copy and paste of the method in the super class
	// We should refactor this
	@Override
	public CompletionStage<Integer> reactiveExecute(DomainQueryExecutionContext executionContext) {
		final SqmInsertStatement<?> sqmInsertStatement = getSqmStatement();
		final SessionFactoryImplementor factory = executionContext.getSession().getFactory();
		final EntityPersister entityDescriptor = getEntityDescriptor().getEntityPersister();
		final String explicitDmlTargetAlias;
		if ( sqmInsertStatement.getTarget().getExplicitAlias() == null ) {
			explicitDmlTargetAlias = "dml_target";
		}
		else {
			explicitDmlTargetAlias = sqmInsertStatement.getTarget().getExplicitAlias();
		}

		final MultiTableSqmMutationConverter sqmConverter = new MultiTableSqmMutationConverter(
				entityDescriptor,
				sqmInsertStatement,
				sqmInsertStatement.getTarget(),
				explicitDmlTargetAlias,
				getDomainParameterXref(),
				executionContext.getQueryOptions(),
				executionContext.getSession().getLoadQueryInfluencers(),
				executionContext.getQueryParameterBindings(),
				factory.getSqlTranslationEngine()
		);
		final TableGroup insertingTableGroup = sqmConverter.getMutatingTableGroup();

		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		// visit the insertion target using our special converter, collecting
		// information about the target paths

		final int size = getSqmStatement().getInsertionTargetPaths().size();
		final List<Map.Entry<List<CteColumn>, Assignment>> targetPathColumns = new ArrayList<>( size );
		final List<CteColumn> targetPathCteColumns = new ArrayList<>( size );
		final NamedTableReference entityTableReference = new NamedTableReference(
				getCteTable().getTableExpression(),
				TemporaryTable.DEFAULT_ALIAS,
				true
		);
		final InsertSelectStatement insertStatement = new InsertSelectStatement( entityTableReference );

		final BaseSqmToSqlAstConverter.AdditionalInsertValues additionalInsertValues = sqmConverter.visitInsertionTargetPaths(
				(assignable, columnReferences) -> {
					final SqmPathInterpretation<?> pathInterpretation = (SqmPathInterpretation<?>) assignable;
					final List<CteColumn> columns = getCteTable().findCteColumns( pathInterpretation.getExpressionType() );
					insertStatement.addTargetColumnReferences( columnReferences );
					targetPathCteColumns.addAll( columns );
					targetPathColumns.add( new AbstractMap.SimpleEntry<>( columns, new Assignment( assignable, (Expression) assignable ) ) );
				},
				sqmInsertStatement,
				entityDescriptor,
				insertingTableGroup
		);

		final boolean assignsId = targetPathCteColumns.contains( getCteTable().getCteColumns().get( 0 ) );

		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		// Create the statement that represent the source for the entity cte

		final Stack<SqlAstProcessingState> processingStateStack = sqmConverter.getProcessingStateStack();
		final SqlAstProcessingState oldState = processingStateStack.pop();
		final Statement queryStatement;
		if ( sqmInsertStatement instanceof SqmInsertSelectStatement ) {
			final QueryPart queryPart = sqmConverter.visitQueryPart( ( (SqmInsertSelectStatement<?>) sqmInsertStatement ).getSelectQueryPart() );
			queryPart.visitQuerySpecs(
					querySpec -> {
						// This returns true if the insertion target uses a sequence with an optimizer
						// in which case we will fill the row_number column instead of the id column
						if ( additionalInsertValues.applySelections( querySpec, sessionFactory ) ) {
							final CteColumn rowNumberColumn = getCteTable().getCteColumns()
									.get( getCteTable().getCteColumns().size() - 1 );
							final ColumnReference columnReference = new ColumnReference(
									(String) null,
									rowNumberColumn.getColumnExpression(),
									false,
									null,
									rowNumberColumn.getJdbcMapping()
							);
							insertStatement.getTargetColumns().set(
									insertStatement.getTargetColumns().size() - 1,
									columnReference
							);
							targetPathCteColumns.set(
									targetPathCteColumns.size() - 1,
									rowNumberColumn
							);
						}
						if ( !assignsId && entityDescriptor.getGenerator().generatedOnExecution() ) {
							querySpec.getSelectClause()
									.addSqlSelection( new SqlSelectionImpl( 0, SqmInsertStrategyHelper.createRowNumberingExpression( querySpec, sessionFactory ) ) );
						}
					}
			);
			queryStatement = new SelectStatement( queryPart );
		}
		else {
			final List<SqmValues> sqmValuesList = ( (SqmInsertValuesStatement<?>) sqmInsertStatement ).getValuesList();
			final List<Values> valuesList = new ArrayList<>( sqmValuesList.size() );
			for ( SqmValues sqmValues : sqmValuesList ) {
				final Values values = sqmConverter.visitValues( sqmValues );
				additionalInsertValues.applyValues( values );
				valuesList.add( values );
			}
			final QuerySpec querySpec = new QuerySpec( true );
			final NavigablePath navigablePath = new NavigablePath( entityDescriptor.getRootPathName() );
			final List<String> columnNames = new ArrayList<>( targetPathColumns.size() );
			final String valuesAlias = insertingTableGroup.getPrimaryTableReference().getIdentificationVariable();
			for ( Map.Entry<List<CteColumn>, Assignment> entry : targetPathColumns ) {
				for ( ColumnReference columnReference : entry.getValue().getAssignable().getColumnReferences() ) {
					columnNames.add( columnReference.getColumnExpression() );
					querySpec.getSelectClause().addSqlSelection(
							new SqlSelectionImpl(
									0,
									columnReference.getQualifier().equals( valuesAlias )
											? columnReference
											: new ColumnReference(
											valuesAlias,
											columnReference.getColumnExpression(),
											false,
											null,
											columnReference.getJdbcMapping()
									)
							)
					);
				}
			}
			if ( !assignsId && entityDescriptor.getGenerator().generatedOnExecution() ) {
				querySpec.getSelectClause().addSqlSelection(
						new SqlSelectionImpl(
								0,
								SqmInsertStrategyHelper.createRowNumberingExpression(
										querySpec,
										sessionFactory
								)
						)
				);
			}
			final ValuesTableGroup valuesTableGroup = new ValuesTableGroup(
					navigablePath,
					entityDescriptor.getEntityPersister(),
					valuesList,
					insertingTableGroup.getPrimaryTableReference().getIdentificationVariable(),
					columnNames,
					true,
					factory
			);
			querySpec.getFromClause().addRoot( valuesTableGroup );
			queryStatement = new SelectStatement( querySpec );
		}
		processingStateStack.push( oldState );
		sqmConverter.pruneTableGroupJoins();

		if ( !assignsId && entityDescriptor.getGenerator().generatedOnExecution() ) {
			// Add the row number to the assignments
			final CteColumn rowNumberColumn = getCteTable().getCteColumns()
					.get( getCteTable().getCteColumns().size() - 1 );
			final ColumnReference columnReference = new ColumnReference(
					(String) null,
					rowNumberColumn.getColumnExpression(),
					false,
					null,
					rowNumberColumn.getJdbcMapping()
			);
			insertStatement.getTargetColumns().add( columnReference );
			targetPathCteColumns.add( rowNumberColumn );
		}

		final CteTable entityCteTable = createCteTable( getCteTable(), targetPathCteColumns );

		// Create the main query spec that will return the count of rows
		final QuerySpec querySpec = new QuerySpec( true, 1 );
		final List<DomainResult<?>> domainResults = new ArrayList<>( 1 );
		final SelectStatement statement = new SelectStatement( querySpec, domainResults );

		final CteStatement entityCte;
		if ( additionalInsertValues.requiresRowNumberIntermediate() ) {
			final CteTable fullEntityCteTable = getCteTable();
			final String baseTableName = "base_" + entityCteTable.getTableExpression();
			final CteStatement baseEntityCte = new CteStatement(
					entityCteTable.withName( baseTableName ),
					queryStatement,
					// The query cte will be reused multiple times
					CteMaterialization.MATERIALIZED
			);
			statement.addCteStatement( baseEntityCte );

			final CteColumn rowNumberColumn = fullEntityCteTable.getCteColumns().get(
					fullEntityCteTable.getCteColumns().size() - 1
			);
			final ColumnReference rowNumberColumnReference = new ColumnReference(
					"e",
					rowNumberColumn.getColumnExpression(),
					false,
					null,
					rowNumberColumn.getJdbcMapping()
			);
			final CteColumn idColumn = fullEntityCteTable.getCteColumns().get( 0 );
			final BasicValuedMapping idType = (BasicValuedMapping) idColumn.getJdbcMapping();
			final Optimizer optimizer = ( (OptimizableGenerator) entityDescriptor.getGenerator() ).getOptimizer();
			final BasicValuedMapping integerType = (BasicValuedMapping) rowNumberColumn.getJdbcMapping();
			final Expression rowNumberMinusOneModuloIncrement = new BinaryArithmeticExpression(
					new BinaryArithmeticExpression(
							rowNumberColumnReference,
							BinaryArithmeticOperator.SUBTRACT,
							new QueryLiteral<>(
									1,
									(BasicValuedMapping) rowNumberColumn.getJdbcMapping()
							),
							integerType
					),
					BinaryArithmeticOperator.MODULO,
					new QueryLiteral<>(
							optimizer.getIncrementSize(),
							integerType
					),
					integerType
			);

			// Create the CTE that fetches a new sequence value for the row numbers that need it
			{
				final QuerySpec rowsWithSequenceQuery = new QuerySpec( true );
				rowsWithSequenceQuery.getFromClause().addRoot(
						new CteTableGroup( new NamedTableReference( baseTableName, "e" ) )
				);
				rowsWithSequenceQuery.getSelectClause().addSqlSelection(
						new SqlSelectionImpl(
								0,
								rowNumberColumnReference
						)
				);
				final String fragment = ( (BulkInsertionCapableIdentifierGenerator) entityDescriptor.getGenerator() )
						.determineBulkInsertionIdentifierGenerationSelectFragment(
								sessionFactory.getSqlStringGenerationContext()
						);
				rowsWithSequenceQuery.getSelectClause().addSqlSelection(
						new SqlSelectionImpl(
								1,
								new SelfRenderingSqlFragmentExpression( fragment )
						)
				);
				rowsWithSequenceQuery.applyPredicate(
						new ComparisonPredicate(
								rowNumberMinusOneModuloIncrement,
								ComparisonOperator.EQUAL,
								new QueryLiteral<>(
										0,
										integerType
								)
						)
				);
				final CteTable rowsWithSequenceCteTable = new CteTable(
						ROW_NUMBERS_WITH_SEQUENCE_VALUE,
						List.of( rowNumberColumn, idColumn )
				);
				final SelectStatement rowsWithSequenceStatement = new SelectStatement( rowsWithSequenceQuery );
				final CteStatement rowsWithSequenceCte = new CteStatement(
						rowsWithSequenceCteTable,
						rowsWithSequenceStatement,
						// The query cte will be reused multiple times
						CteMaterialization.MATERIALIZED
				);
				statement.addCteStatement( rowsWithSequenceCte );
			}

			// Create the CTE that represents the entity cte
			{
				final QuerySpec entityQuery = new QuerySpec( true );
				final NavigablePath navigablePath = new NavigablePath( baseTableName );
				final TableGroup baseTableGroup = new TableGroupImpl(
						navigablePath,
						null,
						new NamedTableReference( baseTableName, "e" ),
						null
				);
				final TableGroup rowsWithSequenceTableGroup = new CteTableGroup(
						new NamedTableReference(
								ROW_NUMBERS_WITH_SEQUENCE_VALUE,
								"t"
						)
				);
				baseTableGroup.addTableGroupJoin(
						new TableGroupJoin(
								rowsWithSequenceTableGroup.getNavigablePath(),
								SqlAstJoinType.LEFT,
								rowsWithSequenceTableGroup,
								new ComparisonPredicate(
										new BinaryArithmeticExpression(
												rowNumberColumnReference,
												BinaryArithmeticOperator.SUBTRACT,
												rowNumberMinusOneModuloIncrement,
												integerType
										),
										ComparisonOperator.EQUAL,
										new ColumnReference(
												"t",
												rowNumberColumn.getColumnExpression(),
												false,
												null,
												rowNumberColumn.getJdbcMapping()
										)
								)
						)
				);
				entityQuery.getFromClause().addRoot( baseTableGroup );
				entityQuery.getSelectClause().addSqlSelection(
						new SqlSelectionImpl(
								0,
								new BinaryArithmeticExpression(
										new ColumnReference(
												"t",
												idColumn.getColumnExpression(),
												false,
												null,
												idColumn.getJdbcMapping()
										),
										BinaryArithmeticOperator.ADD,
										new BinaryArithmeticExpression(
												rowNumberColumnReference,
												BinaryArithmeticOperator.SUBTRACT,
												new ColumnReference(
														"t",
														rowNumberColumn.getColumnExpression(),
														false,
														null,
														rowNumberColumn.getJdbcMapping()
												),
												integerType
										),
										idType
								)
						)
				);
				final CteTable finalEntityCteTable;
				if ( targetPathCteColumns.contains( getCteTable().getCteColumns().get( 0 ) ) ) {
					finalEntityCteTable = entityCteTable;
				}
				else {
					targetPathCteColumns.add( 0, getCteTable().getCteColumns().get( 0 ) );
					finalEntityCteTable = createCteTable( getCteTable(), targetPathCteColumns );
				}
				final List<CteColumn> cteColumns = finalEntityCteTable.getCteColumns();
				for ( int i = 1; i < cteColumns.size(); i++ ) {
					final CteColumn cteColumn = cteColumns.get( i );
					entityQuery.getSelectClause().addSqlSelection(
							new SqlSelectionImpl(
									i,
									new ColumnReference(
											"e",
											cteColumn.getColumnExpression(),
											false,
											null,
											cteColumn.getJdbcMapping()
									)
							)
					);
				}

				final SelectStatement entityStatement = new SelectStatement( entityQuery );
				entityCte = new CteStatement(
						finalEntityCteTable,
						entityStatement,
						// The query cte will be reused multiple times
						CteMaterialization.MATERIALIZED
				);
				statement.addCteStatement( entityCte );
			}
		}
		else if ( !assignsId && entityDescriptor.getGenerator().generatedOnExecution() ) {
			final String baseTableName = "base_" + entityCteTable.getTableExpression();
			final CteStatement baseEntityCte = new CteStatement(
					entityCteTable.withName( baseTableName ),
					queryStatement,
					// The query cte will be reused multiple times
					CteMaterialization.MATERIALIZED
			);
			statement.addCteStatement( baseEntityCte );
			targetPathCteColumns.add( 0, getCteTable().getCteColumns().get( 0 ) );
			final CteTable finalEntityCteTable = createCteTable( getCteTable(), targetPathCteColumns );
			final QuerySpec finalQuerySpec = new QuerySpec( true );
			final SelectStatement finalQueryStatement = new SelectStatement( finalQuerySpec );
			entityCte = new CteStatement(
					finalEntityCteTable,
					finalQueryStatement,
					// The query cte will be reused multiple times
					CteMaterialization.MATERIALIZED
			);
		}
		else {
			entityCte = new CteStatement(
					entityCteTable,
					queryStatement,
					// The query cte will be reused multiple times
					CteMaterialization.MATERIALIZED
			);
			statement.addCteStatement( entityCte );
		}

		// Add all CTEs
		final String baseInsertCte = addDmlCtes(
				statement,
				entityCte,
				targetPathColumns,
				assignsId,
				sqmConverter,
				factory
		);

		final Expression count = createCountStar( factory, sqmConverter );
		domainResults
				.add( new BasicResult<>( 0, null, ( (SqlExpressible) count ).getJdbcMapping() ) );
		querySpec.getSelectClause().addSqlSelection( new SqlSelectionImpl( 0, count ) );
		querySpec.getFromClause().addRoot(
				new CteTableGroup(
						new NamedTableReference(
								// We want to return the insertion count of the base table
								baseInsertCte,
								CTE_TABLE_IDENTIFIER
						)
				)
		);

		// Execute the statement
		final JdbcServices jdbcServices = factory.getJdbcServices();
		final SqlAstTranslator<JdbcOperationQuerySelect> translator = jdbcServices.getJdbcEnvironment()
				.getSqlAstTranslatorFactory()
				.buildSelectTranslator( factory, statement );
		final JdbcParameterBindings jdbcParameterBindings = SqmUtil.createJdbcParameterBindings(
				executionContext.getQueryParameterBindings(),
				getDomainParameterXref(),
				SqmUtil.generateJdbcParamsXref( getDomainParameterXref(), sqmConverter ),
				new SqmParameterMappingModelResolutionAccess() {
					@Override
					@SuppressWarnings("unchecked")
					public <T> MappingModelExpressible<T> getResolvedMappingModelType(SqmParameter<T> parameter) {
						return (MappingModelExpressible<T>) sqmConverter.getSqmParameterMappingModelExpressibleResolutions()
								.get( parameter );
					}
				},
				executionContext.getSession()
		);
		final JdbcOperationQuerySelect select = translator.translate( jdbcParameterBindings, executionContext.getQueryOptions() );
		return ( (ReactiveSharedSessionContractImplementor) executionContext.getSession() )
				.reactiveAutoFlushIfRequired( select.getAffectedTableNames() )
				.thenCompose( v -> StandardReactiveSelectExecutor.INSTANCE.list(
								select,
								jdbcParameterBindings,
								SqmJdbcExecutionContextAdapter.omittingLockingAndPaging( executionContext ),
								row -> row[0],
								ReactiveListResultsConsumer.UniqueSemantic.NONE
						)
						.thenApply( list -> ( (Number) list.get( 0 ) ).intValue() ) );
	}
}
