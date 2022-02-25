/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.internal.util.collections.Stack;
import org.hibernate.internal.util.collections.StandardStack;
import org.hibernate.query.criteria.LiteralHandlingMode;
import org.hibernate.query.criteria.internal.compile.ExplicitParameterInfo;
import org.hibernate.query.criteria.internal.compile.ImplicitParameterBinding;
import org.hibernate.query.criteria.internal.compile.InterpretedParameterMetadata;
import org.hibernate.query.criteria.internal.compile.RenderingContext;
import org.hibernate.query.criteria.internal.expression.function.FunctionExpression;
import org.hibernate.sql.ast.Clause;
import org.hibernate.type.Type;
import org.hibernate.type.TypeResolver;

import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.ParameterExpression;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link RenderingContext} used when compiling criteria queries for
 * reactive execution. Implementation based on an inner class belonging to
 * {@link org.hibernate.query.criteria.internal.compile.CriteriaCompiler}.
 *
 * @author Gavin King
 * @author Steve Ebersole
 */
public class CriteriaQueryRenderingContext implements RenderingContext, InterpretedParameterMetadata {

	private final Map<ParameterExpression<?>, ExplicitParameterInfo<?>> explicitParameterInfoMap = new HashMap<>();
	private final List<ImplicitParameterBinding> implicitParameterBindings = new ArrayList<>();

	//TODO: roll this back once we have parameters?
//		final LiteralHandlingMode criteriaLiteralHandlingMode = sessionFactory
//				.getSessionFactoryOptions()
//				.getCriteriaLiteralHandlingMode();

	private int aliasCount;
	private int explicitParameterCount;

	private final Stack<Clause> clauseStack = new StandardStack<>();
	@SuppressWarnings("rawtypes")
	private final Stack<FunctionExpression> functionContextStack = new StandardStack<>();

	private final Dialect dialect;
	private final TypeResolver typeResolver;

	public CriteriaQueryRenderingContext(SessionFactoryImplementor sessionFactory) {
		dialect = sessionFactory.getServiceRegistry().getService( JdbcServices.class ).getDialect();
		typeResolver = sessionFactory.getTypeResolver();
	}

	@Override
	public Dialect getDialect() {
		return dialect;
	}

	@Override
	public LiteralHandlingMode getCriteriaLiteralHandlingMode() {
		return LiteralHandlingMode.INLINE;
//				return criteriaLiteralHandlingMode;
	}
	@Override
	public Map<ParameterExpression<?>, ExplicitParameterInfo<?>> explicitParameterInfoMap() {
		return explicitParameterInfoMap;
	}
	@Override
	public List<ImplicitParameterBinding> implicitParameterBindings() {
		return implicitParameterBindings;
	}

	public String generateAlias() {
		return "generatedAlias" + aliasCount++;
	}

	public String generateParameterName() {
		return "param" + explicitParameterCount++;
	}

	@Override
	public Stack<Clause> getClauseStack() {
		return clauseStack;
	}

	@Override @SuppressWarnings("rawtypes")
	public Stack<FunctionExpression> getFunctionStack() {
		return functionContextStack;
	}

	@Override
	public ExplicitParameterInfo<?> registerExplicitParameter(ParameterExpression<?> criteriaQueryParameter) {
		ExplicitParameterInfo<?> parameterInfo = explicitParameterInfoMap.get( criteriaQueryParameter );
		if ( parameterInfo == null ) {
			if ( StringHelper.isNotEmpty( criteriaQueryParameter.getName() ) ) {
				parameterInfo = new ExplicitParameterInfo<>(
						criteriaQueryParameter.getName(),
						null,
						criteriaQueryParameter.getJavaType()
				);
			}
			else if ( criteriaQueryParameter.getPosition() != null ) {
				parameterInfo = new ExplicitParameterInfo<>(
						null,
						criteriaQueryParameter.getPosition(),
						criteriaQueryParameter.getJavaType()
				);
			}
			else {
				parameterInfo = new ExplicitParameterInfo<>(
						generateParameterName(),
						null,
						criteriaQueryParameter.getJavaType()
				);
			}

			explicitParameterInfoMap.put( criteriaQueryParameter, parameterInfo );
		}

		return parameterInfo;
	}

	public String registerLiteralParameterBinding(final Object literal, final Class javaType) {
		ImplicitParameterBinding binding = new ImplicitParameterBinding() {
			final String parameterName = generateParameterName();
			public String getParameterName() {
				return parameterName;
			}
			public Class<?> getJavaType() {
				return javaType;
			}
			public void bind(TypedQuery typedQuery) {
				typedQuery.setParameter( parameterName, literal );
			}
		};

		implicitParameterBindings.add( binding );
		return binding.getParameterName();
	}

	public String getCastType(Class javaType) {
		Type hibernateType = typeResolver.heuristicType( javaType.getName() );
		if ( hibernateType == null ) {
			throw new IllegalArgumentException(
					"Could not convert java type [" + javaType.getName() + "] to Hibernate type"
			);
		}
		return hibernateType.getName();
	}
}
