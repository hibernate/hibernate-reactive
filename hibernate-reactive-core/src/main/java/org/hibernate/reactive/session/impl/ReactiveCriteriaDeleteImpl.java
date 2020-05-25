package org.hibernate.reactive.session.impl;

import org.hibernate.query.criteria.internal.CriteriaBuilderImpl;
import org.hibernate.query.criteria.internal.CriteriaDeleteImpl;
import org.hibernate.query.criteria.internal.compile.ImplicitParameterBinding;
import org.hibernate.reactive.session.ReactiveQuery;
import org.hibernate.reactive.session.ReactiveSession;

import javax.persistence.TypedQuery;

/**
 * A reactific {@link CriteriaDeleteImpl}, providing the implementation
 * of {@link javax.persistence.criteria.CriteriaDelete}.
 *
 * @author Gavin King
 */
public class ReactiveCriteriaDeleteImpl<T> extends CriteriaDeleteImpl<T> implements Criteria<T> {

	public ReactiveCriteriaDeleteImpl(CriteriaBuilderImpl criteriaBuilder) {
		super(criteriaBuilder);
	}

	@Override
	public ReactiveQuery<T> build(CriteriaQueryRenderingContext context, ReactiveSession session) {
		ReactiveQuery<T> query = session.createReactiveQuery(
				renderQuery( context ),
				null,
				null,
				new CriteriaQueryOptions( null, context.implicitParameterBindings() )
		);

		for ( ImplicitParameterBinding implicitParameterBinding: context.implicitParameterBindings() ) {
			implicitParameterBinding.bind( (TypedQuery<?>) query );
		}

		return query;
	};
}
