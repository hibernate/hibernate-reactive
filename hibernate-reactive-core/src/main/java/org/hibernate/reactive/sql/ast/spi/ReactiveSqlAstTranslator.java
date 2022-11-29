/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.ast.spi;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.reactive.sql.exec.spi.ReactiveJdbcSelect;
import org.hibernate.reactive.sql.results.internal.ReactiveStandardValuesMappingProducer;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.spi.StandardSqlAstTranslator;
import org.hibernate.sql.ast.tree.Statement;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.spi.JdbcOperation;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;

import static org.hibernate.sql.ast.SqlTreePrinter.logSqlAst;
import static org.hibernate.sql.results.graph.DomainResultGraphPrinter.logDomainResultGraph;

public class ReactiveSqlAstTranslator<T extends JdbcOperation> extends StandardSqlAstTranslator<T> implements SqlAstTranslator<T> {

	public ReactiveSqlAstTranslator(SessionFactoryImplementor sessionFactory, Statement statement) {
		super( sessionFactory, statement );
	}

	@Override
	protected JdbcOperationQuerySelect translateSelect(SelectStatement selectStatement) {
		logDomainResultGraph( selectStatement.getDomainResultDescriptors() );
		logSqlAst( selectStatement );

		visitSelectStatement( selectStatement );

		final int rowsToSkip;
		return new ReactiveJdbcSelect(
				getSql(),
				getParameterBinders(),
				new ReactiveStandardValuesMappingProducer(
						selectStatement.getQuerySpec().getSelectClause().getSqlSelections(),
						selectStatement.getDomainResultDescriptors()
				),
				getAffectedTableNames(),
				getFilterJdbcParameters(),
				rowsToSkip = getRowsToSkip( selectStatement, getJdbcParameterBindings() ),
				getMaxRows( selectStatement, getJdbcParameterBindings(), rowsToSkip ),
				getAppliedParameterBindings(),
				getJdbcLockStrategy(),
				getOffsetParameter(),
				getLimitParameter()
		);
	}

	//
//	@Override
//	public T translate(JdbcParameterBindings jdbcParameterBindings, QueryOptions queryOptions) {
//		T jdbcOperation = astTranslator.translate( jdbcParameterBindings, queryOptions );
//		return jdbcOperation instanceof JdbcSelect
//			? (T) new ReactiveJdbcSelect( (JdbcSelect) jdbcOperation )
//			: jdbcOperation;
//	}
//
//	@Override
//	public SessionFactoryImplementor getSessionFactory() {
//		return astTranslator.getSessionFactory();
//	}
//
//	@Override
//	public void render(SqlAstNode sqlAstNode, SqlAstNodeRenderingMode renderingMode) {
//		astTranslator.render( sqlAstNode, renderingMode );
//	}
//
//	@Override
//	public boolean supportsFilterClause() {
//		return astTranslator.supportsFilterClause();
//	}
//
//	@Override
//	public QueryPart getCurrentQueryPart() {
//		return astTranslator.getCurrentQueryPart();
//	}
//
//	@Override
//	public Stack<Clause> getCurrentClauseStack() {
//		return astTranslator.getCurrentClauseStack();
//	}
//
//	@Override
//	public Set<String> getAffectedTableNames() {
//		return astTranslator.getAffectedTableNames();
//	}
//
//	@Override
//	public void visitSelectStatement(SelectStatement statement) {
//		astTranslator.visitSelectStatement( statement );
//	}
//
//	@Override
//	public void visitDeleteStatement(DeleteStatement statement) {
//		astTranslator.visitDeleteStatement( statement );
//	}
//
//	@Override
//	public void visitUpdateStatement(UpdateStatement statement) {
//		astTranslator.visitUpdateStatement( statement );
//	}
//
//	@Override
//	public void visitInsertStatement(InsertStatement statement) {
//		astTranslator.visitInsertStatement( statement );
//	}
//
//	@Override
//	public void visitAssignment(Assignment assignment) {
//		astTranslator.visitAssignment( assignment );
//	}
//
//	@Override
//	public void visitQueryGroup(QueryGroup queryGroup) {
//		astTranslator.visitQueryGroup( queryGroup );
//	}
//
//	@Override
//	public void visitQuerySpec(QuerySpec querySpec) {
//		astTranslator.visitQuerySpec( querySpec );
//	}
//
//	@Override
//	public void visitSortSpecification(SortSpecification sortSpecification) {
//		astTranslator.visitSortSpecification( sortSpecification );
//	}
//
//	@Override
//	public void visitOffsetFetchClause(QueryPart querySpec) {
//		astTranslator.visitOffsetFetchClause( querySpec );
//	}
//
//	@Override
//	public void visitSelectClause(SelectClause selectClause) {
//		astTranslator.visitSelectClause( selectClause );
//	}
//
//	@Override
//	public void visitSqlSelection(SqlSelection sqlSelection) {
//		astTranslator.visitSqlSelection( sqlSelection );
//	}
//
//	@Override
//	public void visitFromClause(FromClause fromClause) {
//		astTranslator.visitFromClause( fromClause );
//	}
//
//	@Override
//	public void visitTableGroup(TableGroup tableGroup) {
//		astTranslator.visitTableGroup( tableGroup );
//	}
//
//	@Override
//	public void visitTableGroupJoin(TableGroupJoin tableGroupJoin) {
//		astTranslator.visitTableGroupJoin( tableGroupJoin );
//	}
//
//	@Override
//	public void visitNamedTableReference(NamedTableReference tableReference) {
//		astTranslator.visitNamedTableReference( tableReference );
//	}
//
//	@Override
//	public void visitValuesTableReference(ValuesTableReference tableReference) {
//		astTranslator.visitValuesTableReference( tableReference );
//	}
//
//	@Override
//	public void visitQueryPartTableReference(QueryPartTableReference tableReference) {
//		astTranslator.visitQueryPartTableReference( tableReference );
//	}
//
//	@Override
//	public void visitFunctionTableReference(FunctionTableReference tableReference) {
//		astTranslator.visitFunctionTableReference( tableReference );
//	}
//
//	@Override
//	public void visitTableReferenceJoin(TableReferenceJoin tableReferenceJoin) {
//		astTranslator.visitTableReferenceJoin( tableReferenceJoin );
//	}
//
//	@Override
//	public void visitColumnReference(ColumnReference columnReference) {
//		astTranslator.visitColumnReference( columnReference );
//	}
//
//	@Override
//	public void visitExtractUnit(ExtractUnit extractUnit) {
//		astTranslator.visitExtractUnit( extractUnit );
//	}
//
//	@Override
//	public void visitFormat(Format format) {
//		astTranslator.visitFormat( format );
//	}
//
//	@Override
//	public void visitDistinct(Distinct distinct) {
//		astTranslator.visitDistinct( distinct );
//	}
//
//	@Override
//	public void visitOverflow(Overflow overflow) {
//		astTranslator.visitOverflow( overflow );
//	}
//
//	@Override
//	public void visitStar(Star star) {
//		astTranslator.visitStar( star );
//	}
//
//	@Override
//	public void visitTrimSpecification(TrimSpecification trimSpecification) {
//		astTranslator.visitTrimSpecification( trimSpecification );
//	}
//
//	@Override
//	public void visitCastTarget(CastTarget castTarget) {
//		astTranslator.visitCastTarget( castTarget );
//	}
//
//	@Override
//	public void visitBinaryArithmeticExpression(BinaryArithmeticExpression arithmeticExpression) {
//		astTranslator.visitBinaryArithmeticExpression( arithmeticExpression );
//	}
//
//	@Override
//	public void visitCaseSearchedExpression(CaseSearchedExpression caseSearchedExpression) {
//		astTranslator.visitCaseSearchedExpression( caseSearchedExpression );
//	}
//
//	@Override
//	public void visitCaseSimpleExpression(CaseSimpleExpression caseSimpleExpression) {
//		astTranslator.visitCaseSimpleExpression( caseSimpleExpression );
//	}
//
//	@Override
//	public void visitAny(Any any) {
//		astTranslator.visitAny( any );
//	}
//
//	@Override
//	public void visitEvery(Every every) {
//		astTranslator.visitEvery( every );
//	}
//
//	@Override
//	public void visitSummarization(Summarization every) {
//		astTranslator.visitSummarization( every );
//	}
//
//	@Override
//	public void visitOver(Over<?> over) {
//		astTranslator.visitOver( over );
//	}
//
//	@Override
//	public void visitSelfRenderingExpression(SelfRenderingExpression expression) {
//		astTranslator.visitSelfRenderingExpression( expression );
//	}
//
//	@Override
//	public void visitSqlSelectionExpression(SqlSelectionExpression expression) {
//		astTranslator.visitSqlSelectionExpression( expression );
//	}
//
//	@Override
//	public void visitEntityTypeLiteral(EntityTypeLiteral expression) {
//		astTranslator.visitEntityTypeLiteral( expression );
//	}
//
//	@Override
//	public void visitTuple(SqlTuple tuple) {
//		astTranslator.visitTuple( tuple );
//	}
//
//	@Override
//	public void visitCollation(Collation collation) {
//		astTranslator.visitCollation( collation );
//	}
//
//	@Override
//	public void visitParameter(JdbcParameter jdbcParameter) {
//		astTranslator.visitParameter( jdbcParameter );
//	}
//
//	@Override
//	public void visitJdbcLiteral(JdbcLiteral<?> jdbcLiteral) {
//		astTranslator.visitJdbcLiteral( jdbcLiteral );
//	}
//
//	@Override
//	public void visitQueryLiteral(QueryLiteral<?> queryLiteral) {
//		astTranslator.visitQueryLiteral( queryLiteral );
//	}
//
//	@Override
//	public void visitUnaryOperationExpression(UnaryOperation unaryOperationExpression) {
//		astTranslator.visitUnaryOperationExpression( unaryOperationExpression );
//	}
//
//	@Override
//	public void visitModifiedSubQueryExpression(ModifiedSubQueryExpression expression) {
//		astTranslator.visitModifiedSubQueryExpression( expression );
//	}
//
//	@Override
//	public void visitBooleanExpressionPredicate(BooleanExpressionPredicate booleanExpressionPredicate) {
//		astTranslator.visitBooleanExpressionPredicate( booleanExpressionPredicate );
//	}
//
//	@Override
//	public void visitBetweenPredicate(BetweenPredicate betweenPredicate) {
//		astTranslator.visitBetweenPredicate( betweenPredicate );
//	}
//
//	@Override
//	public void visitFilterPredicate(FilterPredicate filterPredicate) {
//		astTranslator.visitFilterPredicate( filterPredicate );
//	}
//
//	@Override
//	public void visitFilterFragmentPredicate(FilterPredicate.FilterFragmentPredicate fragmentPredicate) {
//		astTranslator.visitFilterFragmentPredicate( fragmentPredicate );
//	}
//
//	@Override
//	public void visitSqlFragmentPredicate(SqlFragmentPredicate predicate) {
//		astTranslator.visitSqlFragmentPredicate( predicate );
//	}
//
//	@Override
//	public void visitGroupedPredicate(GroupedPredicate groupedPredicate) {
//		astTranslator.visitGroupedPredicate( groupedPredicate );
//	}
//
//	@Override
//	public void visitInListPredicate(InListPredicate inListPredicate) {
//		astTranslator.visitInListPredicate( inListPredicate );
//	}
//
//	@Override
//	public void visitInSubQueryPredicate(InSubQueryPredicate inSubQueryPredicate) {
//		astTranslator.visitInSubQueryPredicate( inSubQueryPredicate );
//	}
//
//	@Override
//	public void visitExistsPredicate(ExistsPredicate existsPredicate) {
//		astTranslator.visitExistsPredicate( existsPredicate );
//	}
//
//	@Override
//	public void visitJunction(Junction junction) {
//		astTranslator.visitJunction( junction );
//	}
//
//	@Override
//	public void visitLikePredicate(LikePredicate likePredicate) {
//		astTranslator.visitLikePredicate( likePredicate );
//	}
//
//	@Override
//	public void visitNegatedPredicate(NegatedPredicate negatedPredicate) {
//		astTranslator.visitNegatedPredicate( negatedPredicate );
//	}
//
//	@Override
//	public void visitNullnessPredicate(NullnessPredicate nullnessPredicate) {
//		astTranslator.visitNullnessPredicate( nullnessPredicate );
//	}
//
//	@Override
//	public void visitRelationalPredicate(ComparisonPredicate comparisonPredicate) {
//		astTranslator.visitRelationalPredicate( comparisonPredicate );
//	}
//
//	@Override
//	public void visitSelfRenderingPredicate(SelfRenderingPredicate selfRenderingPredicate) {
//		astTranslator.visitSelfRenderingPredicate( selfRenderingPredicate );
//	}
//
//	@Override
//	public void visitDurationUnit(DurationUnit durationUnit) {
//		astTranslator.visitDurationUnit( durationUnit );
//	}
//
//	@Override
//	public void visitDuration(Duration duration) {
//		astTranslator.visitDuration( duration );
//	}
//
//	@Override
//	public void visitConversion(Conversion conversion) {
//		astTranslator.visitConversion( conversion );
//	}
}
