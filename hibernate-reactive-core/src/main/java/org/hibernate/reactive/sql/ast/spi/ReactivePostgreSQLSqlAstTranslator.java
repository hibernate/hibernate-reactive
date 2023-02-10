/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.ast.spi;

import org.hibernate.dialect.PostgreSQLSqlAstTranslator;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.reactive.pool.impl.PostgresParameters;
import org.hibernate.reactive.sql.results.internal.ReactiveStandardValuesMappingProducer;
import org.hibernate.sql.ast.tree.Statement;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.spi.JdbcOperation;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;

import static org.hibernate.sql.ast.SqlTreePrinter.logSqlAst;
import static org.hibernate.sql.results.graph.DomainResultGraphPrinter.logDomainResultGraph;

public class ReactivePostgreSQLSqlAstTranslator<T extends JdbcOperation> extends PostgreSQLSqlAstTranslator<T> {

	private int paramCounter = 0;

	public ReactivePostgreSQLSqlAstTranslator(SessionFactoryImplementor sessionFactory, Statement statement) {
		super( sessionFactory, statement );
	}

	@Override
	protected JdbcOperationQuerySelect translateSelect(SelectStatement selectStatement) {
		logDomainResultGraph( selectStatement.getDomainResultDescriptors() );
		logSqlAst( selectStatement );

		visitSelectStatement( selectStatement );

		final int rowsToSkip;
		return new JdbcOperationQuerySelect(
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

	@Override
	public String getSql() {
		// Not all queries goes through the appendSql
		String sql = super.getSql();
		return PostgresParameters.INSTANCE.process( sql );
	}
}
