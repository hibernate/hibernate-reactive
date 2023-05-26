/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.action.spi.AfterTransactionCompletionProcess;
import org.hibernate.action.spi.BeforeTransactionCompletionProcess;
import org.hibernate.action.spi.Executable;
import org.hibernate.event.spi.EventSource;
import org.hibernate.reactive.engine.ReactiveExecutable;

public final class ReactiveEntityInsertActionHolder implements Executable, ReactiveExecutable, Comparable<ReactiveEntityInsertActionHolder>,
		Serializable {

	private final ReactiveEntityInsertAction delegate;

	public ReactiveEntityInsertActionHolder(ReactiveEntityInsertAction delegate) {
		Objects.requireNonNull( delegate );
		this.delegate = delegate;
	}

	@Override
	public int compareTo(ReactiveEntityInsertActionHolder o) {
		return delegate.compareActionTo( o.delegate );
	}

	@Override
	public Serializable[] getPropertySpaces() {
		return delegate.getPropertySpaces();
	}

	@Override
	public void beforeExecutions() throws HibernateException {
		delegate.beforeExecutions();
	}

	@Override
	public void execute() throws HibernateException {
		delegate.execute();
	}

	@Override
	public AfterTransactionCompletionProcess getAfterTransactionCompletionProcess() {
		return delegate.getAfterTransactionCompletionProcess();
	}

	@Override
	public BeforeTransactionCompletionProcess getBeforeTransactionCompletionProcess() {
		return delegate.getBeforeTransactionCompletionProcess();
	}

	@Override
	public void afterDeserialize(EventSource session) {
		delegate.afterDeserialize( session );
	}

	@Override
	public CompletionStage<Void> reactiveExecute() {
		return delegate.reactiveExecute();
	}

	public ReactiveEntityInsertAction getDelegate() {
		return delegate;
	}
}
