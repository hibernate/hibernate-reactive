/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.query.criteria.internal.CriteriaBuilderImpl;
import org.hibernate.query.criteria.internal.CriteriaQueryImpl;
import org.hibernate.query.criteria.internal.SelectionImplementor;
import org.hibernate.query.criteria.internal.ValueHandlerFactory;
import org.hibernate.query.criteria.internal.compile.ImplicitParameterBinding;
import org.hibernate.query.criteria.internal.compile.InterpretedParameterMetadata;
import org.hibernate.query.criteria.internal.compile.RenderingContext;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.session.Criteria;
import org.hibernate.reactive.session.CriteriaQueryOptions;
import org.hibernate.reactive.session.ReactiveQuery;
import org.hibernate.reactive.session.ReactiveQueryExecutor;
import org.hibernate.type.Type;

import javax.persistence.TypedQuery;
import java.lang.invoke.MethodHandles;
import java.util.List;

/**
 * A reactific {@link CriteriaQueryImpl}, providing the implementation
 * of {@link javax.persistence.criteria.CriteriaQuery}.
 *
 * @author Gavin King
 */
public class ReactiveCriteriaQueryImpl<T> extends CriteriaQueryImpl<T> implements Criteria<T> {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveCriteriaQueryImpl(CriteriaBuilderImpl criteriaBuilder, Class<T> returnType) {
		super(criteriaBuilder, returnType);
	}

	private String renderQuery(RenderingContext renderingContext) {
		StringBuilder jpaql = new StringBuilder();
		getQueryStructure().render( jpaql, renderingContext );
		renderOrderByClause( renderingContext, jpaql );
		return jpaql.toString();
	}

	public ReactiveQuery<T> build(CriteriaQueryRenderingContext context, ReactiveQueryExecutor session) {
		final SelectionImplementor<?> selection = (SelectionImplementor<?>) getSelection();
		ReactiveQuery<T> query = session.createReactiveCriteriaQuery(
				renderQuery( context ),
				getResultType(),
				new CriteriaQueryOptions() {
					@Override
					public SelectionImplementor<?> getSelection() {
						return selection;
					}
					@Override
					public InterpretedParameterMetadata getParameterMetadata() {
						return context;
					}
					@Override @SuppressWarnings("rawtypes")
					public List<ValueHandlerFactory.ValueHandler> getValueHandlers() {
						return selection == null ? null : selection.getValueHandlers();
					}
					@Override
					public void validate(Type[] returnTypes) {
						if ( selection != null ) {
							validateSelection( returnTypes, selection );
						}
					}
				}
		);

		for ( ImplicitParameterBinding implicitParameterBinding: context.implicitParameterBindings() ) {
			implicitParameterBinding.bind( (TypedQuery<?>) query );
		}

//		return new CriteriaQueryTypeQueryAdapter(
//				session,
//				query,
//				context.explicitParameterInfoMap()
//		);

		return query;
	}

	private static void validateSelection(Type[] returnTypes, SelectionImplementor<?> selection) {
		if ( selection.isCompoundSelection() ) {
			if ( returnTypes.length != selection.getCompoundSelectionItems().size() ) {
				throw LOG.unexpectedNumberOfReturnedValues( returnTypes.length, selection.getCompoundSelectionItems().size() );
			}
		}
		else {
			if  (returnTypes.length > 1 ) {
				throw LOG.unexpectedNumberOfReturnedValues( returnTypes.length, 1 );
			}
		}
	}
}
