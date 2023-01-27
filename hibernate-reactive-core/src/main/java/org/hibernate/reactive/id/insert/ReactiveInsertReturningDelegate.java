/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.insert;

import java.util.concurrent.CompletionStage;

import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.PostInsertIdentityPersister;
import org.hibernate.id.insert.Binder;
import org.hibernate.id.insert.InsertReturningDelegate;

public class ReactiveInsertReturningDelegate extends InsertReturningDelegate implements ReactiveAbstractReturningDelegate {

	public ReactiveInsertReturningDelegate(PostInsertIdentityPersister persister, Dialect dialect) {
		super( persister, dialect );
	}

	@Override
	public PostInsertIdentityPersister getPersister() {
		return super.getPersister();
	}

	@Override
	public CompletionStage<Object> reactivePerformInsert(
			String insertSQL,
			SharedSessionContractImplementor session,
			Binder binder) {
		throw LOG.notYetImplemented();
	}
}
