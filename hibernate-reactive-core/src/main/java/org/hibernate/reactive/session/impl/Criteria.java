package org.hibernate.reactive.session.impl;

import org.hibernate.reactive.session.ReactiveQuery;
import org.hibernate.reactive.session.ReactiveSession;

/**
 * Abstracts over criteria {@link ReactiveCriteriaQueryImpl queries},
 * {@link ReactiveCriteriaUpdateImpl updates}, and
 * {@link ReactiveCriteriaDeleteImpl deletes}.
 *
 * @author Gavin King
 */
public interface Criteria<R> {
	void validate();
	ReactiveQuery<R> build(CriteriaQueryRenderingContext context, ReactiveSession session);
}
