package org.hibernate.reactive.query.sqm.spi;

import org.hibernate.engine.query.spi.NativeQueryInterpreter;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.query.sql.internal.ParameterParser;
import org.hibernate.query.sql.spi.NativeSelectQueryDefinition;
import org.hibernate.query.sql.spi.NativeSelectQueryPlan;
import org.hibernate.query.sql.spi.ParameterRecognizer;
import org.hibernate.reactive.query.sql.internal.ReactiveNativeSelectQueryPlanImpl;

public class ReactiveNativeQueryInterpreter implements NativeQueryInterpreter {
	public static final ReactiveNativeQueryInterpreter INSTANCE = new ReactiveNativeQueryInterpreter();

	@Override
	public void recognizeParameters(String nativeQuery, ParameterRecognizer recognizer) {
		ParameterParser.parse( nativeQuery, recognizer );
	}

	@Override
	public <R> NativeSelectQueryPlan<R> createQueryPlan(NativeSelectQueryDefinition<R> queryDefinition, SessionFactoryImplementor sessionFactory) {
		return new ReactiveNativeSelectQueryPlanImpl(
				queryDefinition.getSqlString(),
				queryDefinition.getAffectedTableNames(),
				queryDefinition.getQueryParameterOccurrences(),
				queryDefinition.getResultSetMapping(),
				sessionFactory
		);
	}
}
