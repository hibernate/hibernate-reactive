/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session;

import org.hibernate.reactive.session.impl.CriteriaQueryRenderingContext;
import org.hibernate.reactive.session.impl.ReactiveCriteriaDeleteImpl;
import org.hibernate.reactive.session.impl.ReactiveCriteriaQueryImpl;
import org.hibernate.reactive.session.impl.ReactiveCriteriaUpdateImpl;

/**
 * Abstracts over criteria {@link ReactiveCriteriaQueryImpl queries},
 * {@link ReactiveCriteriaUpdateImpl updates}, and
 * {@link ReactiveCriteriaDeleteImpl deletes}.
 *
 * @author Gavin King
 */
public interface Criteria<R> {
	void validate();
	ReactiveQuery<R> build(CriteriaQueryRenderingContext context, ReactiveQueryExecutor session);
}
