/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.SubselectFetch;
import org.hibernate.sql.exec.internal.BaseExecutionContext;
import org.hibernate.sql.results.graph.entity.LoadingEntityEntry;

/**
 * Copy and paste of {@link org.hibernate.loader.ast.internal.ExecutionContextWithSubselectFetchHandler}
 * We should change the scope in ORM.
 */
class ExecutionContextWithSubselectFetchHandler extends BaseExecutionContext {

	private final SubselectFetch.RegistrationHandler subSelectFetchableKeysHandler;

	public ExecutionContextWithSubselectFetchHandler(SharedSessionContractImplementor session, SubselectFetch.RegistrationHandler subSelectFetchableKeysHandler) {
		super( session );
		this.subSelectFetchableKeysHandler = subSelectFetchableKeysHandler;
	}

	@Override
	public void registerLoadingEntityEntry(EntityKey entityKey, LoadingEntityEntry entry) {
		if ( subSelectFetchableKeysHandler != null ) {
			subSelectFetchableKeysHandler.addKey( entityKey, entry );
		}
	}

}
