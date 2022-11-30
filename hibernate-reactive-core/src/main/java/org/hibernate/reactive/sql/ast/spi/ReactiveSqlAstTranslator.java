/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.ast.spi;

import java.util.Set;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.internal.util.collections.Stack;
import org.hibernate.persister.internal.SqlFragmentPredicate;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.sqm.tree.expression.Conversion;
import org.hibernate.sql.ast.Clause;
import org.hibernate.sql.ast.SqlAstNodeRenderingMode;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.spi.SqlSelection;
import org.hibernate.sql.ast.tree.SqlAstNode;
import org.hibernate.sql.ast.tree.delete.DeleteStatement;
import org.hibernate.sql.ast.tree.expression.Any;
import org.hibernate.sql.ast.tree.expression.BinaryArithmeticExpression;
import org.hibernate.sql.ast.tree.expression.CaseSearchedExpression;
import org.hibernate.sql.ast.tree.expression.CaseSimpleExpression;
import org.hibernate.sql.ast.tree.expression.CastTarget;
import org.hibernate.sql.ast.tree.expression.Collation;
import org.hibernate.sql.ast.tree.expression.ColumnReference;
import org.hibernate.sql.ast.tree.expression.Distinct;
import org.hibernate.sql.ast.tree.expression.Duration;
import org.hibernate.sql.ast.tree.expression.DurationUnit;
import org.hibernate.sql.ast.tree.expression.EntityTypeLiteral;
import org.hibernate.sql.ast.tree.expression.Every;
import org.hibernate.sql.ast.tree.expression.ExtractUnit;
import org.hibernate.sql.ast.tree.expression.Format;
import org.hibernate.sql.ast.tree.expression.JdbcLiteral;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.expression.ModifiedSubQueryExpression;
import org.hibernate.sql.ast.tree.expression.Over;
import org.hibernate.sql.ast.tree.expression.Overflow;
import org.hibernate.sql.ast.tree.expression.QueryLiteral;
import org.hibernate.sql.ast.tree.expression.SelfRenderingExpression;
import org.hibernate.sql.ast.tree.expression.SqlSelectionExpression;
import org.hibernate.sql.ast.tree.expression.SqlTuple;
import org.hibernate.sql.ast.tree.expression.Star;
import org.hibernate.sql.ast.tree.expression.Summarization;
import org.hibernate.sql.ast.tree.expression.TrimSpecification;
import org.hibernate.sql.ast.tree.expression.UnaryOperation;
import org.hibernate.sql.ast.tree.from.FromClause;
import org.hibernate.sql.ast.tree.from.FunctionTableReference;
import org.hibernate.sql.ast.tree.from.NamedTableReference;
import org.hibernate.sql.ast.tree.from.QueryPartTableReference;
import org.hibernate.sql.ast.tree.from.TableGroup;
import org.hibernate.sql.ast.tree.from.TableGroupJoin;
import org.hibernate.sql.ast.tree.from.TableReferenceJoin;
import org.hibernate.sql.ast.tree.from.ValuesTableReference;
import org.hibernate.sql.ast.tree.insert.InsertSelectStatement;
import org.hibernate.sql.ast.tree.predicate.BetweenPredicate;
import org.hibernate.sql.ast.tree.predicate.BooleanExpressionPredicate;
import org.hibernate.sql.ast.tree.predicate.ComparisonPredicate;
import org.hibernate.sql.ast.tree.predicate.ExistsPredicate;
import org.hibernate.sql.ast.tree.predicate.FilterPredicate;
import org.hibernate.sql.ast.tree.predicate.GroupedPredicate;
import org.hibernate.sql.ast.tree.predicate.InListPredicate;
import org.hibernate.sql.ast.tree.predicate.InSubQueryPredicate;
import org.hibernate.sql.ast.tree.predicate.Junction;
import org.hibernate.sql.ast.tree.predicate.LikePredicate;
import org.hibernate.sql.ast.tree.predicate.NegatedPredicate;
import org.hibernate.sql.ast.tree.predicate.NullnessPredicate;
import org.hibernate.sql.ast.tree.predicate.SelfRenderingPredicate;
import org.hibernate.sql.ast.tree.select.QueryGroup;
import org.hibernate.sql.ast.tree.select.QueryPart;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.ast.tree.select.SelectClause;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.ast.tree.select.SortSpecification;
import org.hibernate.sql.ast.tree.update.Assignment;
import org.hibernate.sql.ast.tree.update.UpdateStatement;
import org.hibernate.sql.exec.spi.JdbcOperation;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.model.ast.ColumnWriteFragment;
import org.hibernate.sql.model.internal.TableDeleteCustomSql;
import org.hibernate.sql.model.internal.TableDeleteStandard;
import org.hibernate.sql.model.internal.TableInsertCustomSql;
import org.hibernate.sql.model.internal.TableInsertStandard;
import org.hibernate.sql.model.internal.TableUpdateCustomSql;
import org.hibernate.sql.model.internal.TableUpdateStandard;

public class ReactiveSqlAstTranslator<T extends JdbcOperation> implements SqlAstTranslator<T> {

	private final SqlAstTranslator delegate;

	public ReactiveSqlAstTranslator(SqlAstTranslator delegate) {
		this.delegate = delegate;
	}

	@Override
	public SessionFactoryImplementor getSessionFactory() {
		return delegate.getSessionFactory();
	}

	@Override
	public void render(SqlAstNode sqlAstNode, SqlAstNodeRenderingMode renderingMode) {
		delegate.render( sqlAstNode, renderingMode );
	}

	@Override
	public boolean supportsFilterClause() {
		return delegate.supportsFilterClause();
	}

	@Override
	public QueryPart getCurrentQueryPart() {
		return delegate.getCurrentQueryPart();
	}

	@Override
	public Stack<Clause> getCurrentClauseStack() {
		return delegate.getCurrentClauseStack();
	}

	@Override
	public Set<String> getAffectedTableNames() {
		return delegate.getAffectedTableNames();
	}

	@Override
	public JdbcOperation translate(JdbcParameterBindings jdbcParameterBindings, QueryOptions queryOptions) {
		return delegate.translate( jdbcParameterBindings, queryOptions );
	}

	@Override
	public void visitSelectStatement(SelectStatement statement) {
		delegate.visitSelectStatement( statement );
	}

	@Override
	public void visitDeleteStatement(DeleteStatement statement) {
		delegate.visitDeleteStatement( statement );
	}

	@Override
	public void visitUpdateStatement(UpdateStatement statement) {
		delegate.visitUpdateStatement( statement );
	}

	@Override
	public void visitInsertStatement(InsertSelectStatement statement) {
		delegate.visitInsertStatement( statement );
	}

	@Override
	public void visitAssignment(Assignment assignment) {
		delegate.visitAssignment( assignment );
	}

	@Override
	public void visitQueryGroup(QueryGroup queryGroup) {
		delegate.visitQueryGroup( queryGroup );
	}

	@Override
	public void visitQuerySpec(QuerySpec querySpec) {
		delegate.visitQuerySpec( querySpec );
	}

	@Override
	public void visitSortSpecification(SortSpecification sortSpecification) {
		delegate.visitSortSpecification( sortSpecification );
	}

	@Override
	public void visitOffsetFetchClause(QueryPart querySpec) {
		delegate.visitOffsetFetchClause( querySpec );
	}

	@Override
	public void visitSelectClause(SelectClause selectClause) {
		delegate.visitSelectClause( selectClause );
	}

	@Override
	public void visitSqlSelection(SqlSelection sqlSelection) {
		delegate.visitSqlSelection( sqlSelection );
	}

	@Override
	public void visitFromClause(FromClause fromClause) {
		delegate.visitFromClause( fromClause );
	}

	@Override
	public void visitTableGroup(TableGroup tableGroup) {
		delegate.visitTableGroup( tableGroup );
	}

	@Override
	public void visitTableGroupJoin(TableGroupJoin tableGroupJoin) {
		delegate.visitTableGroupJoin( tableGroupJoin );
	}

	@Override
	public void visitNamedTableReference(NamedTableReference tableReference) {
		delegate.visitNamedTableReference( tableReference );
	}

	@Override
	public void visitValuesTableReference(ValuesTableReference tableReference) {
		delegate.visitValuesTableReference( tableReference );
	}

	@Override
	public void visitQueryPartTableReference(QueryPartTableReference tableReference) {
		delegate.visitQueryPartTableReference( tableReference );
	}

	@Override
	public void visitFunctionTableReference(FunctionTableReference tableReference) {
		delegate.visitFunctionTableReference( tableReference );
	}

	@Override
	public void visitTableReferenceJoin(TableReferenceJoin tableReferenceJoin) {
		delegate.visitTableReferenceJoin( tableReferenceJoin );
	}

	@Override
	public void visitColumnReference(ColumnReference columnReference) {
		delegate.visitColumnReference( columnReference );
	}

	@Override
	public void visitExtractUnit(ExtractUnit extractUnit) {
		delegate.visitExtractUnit( extractUnit );
	}

	@Override
	public void visitFormat(Format format) {
		delegate.visitFormat( format );
	}

	@Override
	public void visitDistinct(Distinct distinct) {
		delegate.visitDistinct( distinct );
	}

	@Override
	public void visitOverflow(Overflow overflow) {
		delegate.visitOverflow( overflow );
	}

	@Override
	public void visitStar(Star star) {
		delegate.visitStar( star );
	}

	@Override
	public void visitTrimSpecification(TrimSpecification trimSpecification) {
		delegate.visitTrimSpecification( trimSpecification );
	}

	@Override
	public void visitCastTarget(CastTarget castTarget) {
		delegate.visitCastTarget( castTarget );
	}

	@Override
	public void visitBinaryArithmeticExpression(BinaryArithmeticExpression arithmeticExpression) {
		delegate.visitBinaryArithmeticExpression( arithmeticExpression );
	}

	@Override
	public void visitCaseSearchedExpression(CaseSearchedExpression caseSearchedExpression) {
		delegate.visitCaseSearchedExpression( caseSearchedExpression );
	}

	@Override
	public void visitCaseSimpleExpression(CaseSimpleExpression caseSimpleExpression) {
		delegate.visitCaseSimpleExpression( caseSimpleExpression );
	}

	@Override
	public void visitAny(Any any) {
		delegate.visitAny( any );
	}

	@Override
	public void visitEvery(Every every) {
		delegate.visitEvery( every );
	}

	@Override
	public void visitSummarization(Summarization every) {
		delegate.visitSummarization( every );
	}

	@Override
	public void visitOver(Over<?> over) {
		delegate.visitOver( over );
	}

	@Override
	public void visitSelfRenderingExpression(SelfRenderingExpression expression) {
		delegate.visitSelfRenderingExpression( expression );
	}

	@Override
	public void visitSqlSelectionExpression(SqlSelectionExpression expression) {
		delegate.visitSqlSelectionExpression( expression );
	}

	@Override
	public void visitEntityTypeLiteral(EntityTypeLiteral expression) {
		delegate.visitEntityTypeLiteral( expression );
	}

	@Override
	public void visitTuple(SqlTuple tuple) {
		delegate.visitTuple( tuple );
	}

	@Override
	public void visitCollation(Collation collation) {
		delegate.visitCollation( collation );
	}

	@Override
	public void visitParameter(JdbcParameter jdbcParameter) {
		delegate.visitParameter( jdbcParameter );
	}

	@Override
	public void visitJdbcLiteral(JdbcLiteral<?> jdbcLiteral) {
		delegate.visitJdbcLiteral( jdbcLiteral );
	}

	@Override
	public void visitQueryLiteral(QueryLiteral<?> queryLiteral) {
		delegate.visitQueryLiteral( queryLiteral );
	}

	@Override
	public void visitUnaryOperationExpression(UnaryOperation unaryOperationExpression) {
		delegate.visitUnaryOperationExpression( unaryOperationExpression );
	}

	@Override
	public void visitModifiedSubQueryExpression(ModifiedSubQueryExpression expression) {
		delegate.visitModifiedSubQueryExpression( expression );
	}

	@Override
	public void visitBooleanExpressionPredicate(BooleanExpressionPredicate booleanExpressionPredicate) {
		delegate.visitBooleanExpressionPredicate( booleanExpressionPredicate );
	}

	@Override
	public void visitBetweenPredicate(BetweenPredicate betweenPredicate) {
		delegate.visitBetweenPredicate( betweenPredicate );
	}

	@Override
	public void visitFilterPredicate(FilterPredicate filterPredicate) {
		delegate.visitFilterPredicate( filterPredicate );
	}

	@Override
	public void visitFilterFragmentPredicate(FilterPredicate.FilterFragmentPredicate fragmentPredicate) {
		delegate.visitFilterFragmentPredicate( fragmentPredicate );
	}

	@Override
	public void visitSqlFragmentPredicate(SqlFragmentPredicate predicate) {
		delegate.visitSqlFragmentPredicate( predicate );
	}

	@Override
	public void visitGroupedPredicate(GroupedPredicate groupedPredicate) {
		delegate.visitGroupedPredicate( groupedPredicate );
	}

	@Override
	public void visitInListPredicate(InListPredicate inListPredicate) {
		delegate.visitInListPredicate( inListPredicate );
	}

	@Override
	public void visitInSubQueryPredicate(InSubQueryPredicate inSubQueryPredicate) {
		delegate.visitInSubQueryPredicate( inSubQueryPredicate );
	}

	@Override
	public void visitExistsPredicate(ExistsPredicate existsPredicate) {
		delegate.visitExistsPredicate( existsPredicate );
	}

	@Override
	public void visitJunction(Junction junction) {
		delegate.visitJunction( junction );
	}

	@Override
	public void visitLikePredicate(LikePredicate likePredicate) {
		delegate.visitLikePredicate( likePredicate );
	}

	@Override
	public void visitNegatedPredicate(NegatedPredicate negatedPredicate) {
		delegate.visitNegatedPredicate( negatedPredicate );
	}

	@Override
	public void visitNullnessPredicate(NullnessPredicate nullnessPredicate) {
		delegate.visitNullnessPredicate( nullnessPredicate );
	}

	@Override
	public void visitRelationalPredicate(ComparisonPredicate comparisonPredicate) {
		delegate.visitRelationalPredicate( comparisonPredicate );
	}

	@Override
	public void visitSelfRenderingPredicate(SelfRenderingPredicate selfRenderingPredicate) {
		delegate.visitSelfRenderingPredicate( selfRenderingPredicate );
	}

	@Override
	public void visitDurationUnit(DurationUnit durationUnit) {
		delegate.visitDurationUnit( durationUnit );
	}

	@Override
	public void visitDuration(Duration duration) {
		delegate.visitDuration( duration );
	}

	@Override
	public void visitConversion(Conversion conversion) {
		delegate.visitConversion( conversion );
	}

	@Override
	public void visitStandardTableInsert(TableInsertStandard tableInsert) {
		delegate.visitStandardTableInsert( tableInsert );
	}

	@Override
	public void visitCustomTableInsert(TableInsertCustomSql tableInsert) {
		delegate.visitCustomTableInsert( tableInsert );
	}

	@Override
	public void visitStandardTableDelete(TableDeleteStandard tableDelete) {
		delegate.visitStandardTableDelete( tableDelete );
	}

	@Override
	public void visitCustomTableDelete(TableDeleteCustomSql tableDelete) {
		delegate.visitCustomTableDelete( tableDelete );
	}

	@Override
	public void visitStandardTableUpdate(TableUpdateStandard tableUpdate) {
		delegate.visitStandardTableUpdate( tableUpdate );
	}

	@Override
	public void visitCustomTableUpdate(TableUpdateCustomSql tableUpdate) {
		delegate.visitCustomTableUpdate( tableUpdate );
	}

	@Override
	public void visitColumnWriteFragment(ColumnWriteFragment columnWriteFragment) {
		delegate.visitColumnWriteFragment( columnWriteFragment );
	}
}
