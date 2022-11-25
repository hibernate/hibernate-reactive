package org.hibernate.reactive.query.sql.internal;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.query.results.ResultSetMapping;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sql.internal.NativeSelectQueryPlanImpl;
import org.hibernate.reactive.query.spi.ReactiveNativeSelectQueryPlan;

public class ReactiveNativeSelectQueryPlanImpl extends NativeSelectQueryPlanImpl implements ReactiveNativeSelectQueryPlan {

	public ReactiveNativeSelectQueryPlanImpl(
			String sql,
			Set affectedTableNames,
			List parameterList,
			ResultSetMapping resultSetMapping,
			SessionFactoryImplementor sessionFactory) {
		super( sql, affectedTableNames, parameterList, resultSetMapping, sessionFactory );
	}

	@Override
	public CompletionStage<List> performReactiveList() {
		return null;
	}

	@Override
	public CompletionStage<List> performReactiveList(DomainQueryExecutionContext executionContext) {
		return null;
	}
}
