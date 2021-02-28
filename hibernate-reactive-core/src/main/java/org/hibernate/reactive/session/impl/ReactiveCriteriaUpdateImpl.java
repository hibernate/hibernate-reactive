/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.query.criteria.internal.CriteriaBuilderImpl;
import org.hibernate.query.criteria.internal.CriteriaUpdateImpl;
import org.hibernate.query.criteria.internal.compile.ImplicitParameterBinding;
import org.hibernate.reactive.session.Criteria;
import org.hibernate.reactive.session.ReactiveQuery;
import org.hibernate.reactive.session.ReactiveQueryExecutor;

import javax.persistence.TypedQuery;

/**
 * A reactific {@link CriteriaUpdateImpl}, providing the implementation
 * of {@link javax.persistence.criteria.CriteriaUpdate}.
 *
 * @author Gavin King
 */
public class ReactiveCriteriaUpdateImpl<T> extends CriteriaUpdateImpl<T> implements Criteria<T> {

	public ReactiveCriteriaUpdateImpl(CriteriaBuilderImpl criteriaBuilder) {
		super(criteriaBuilder);
	}

	public ReactiveQuery<T> build(CriteriaQueryRenderingContext context, ReactiveQueryExecutor session) {
		ReactiveQuery<T> query = session.createReactiveCriteriaQuery(
				renderQuery( context ), null, () -> context
		);

		for ( ImplicitParameterBinding implicitParameterBinding: context.implicitParameterBindings() ) {
			implicitParameterBinding.bind( (TypedQuery<?>) query );
		}

		return query;
	};
}
