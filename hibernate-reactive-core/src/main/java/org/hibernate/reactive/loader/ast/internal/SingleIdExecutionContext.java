/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import org.hibernate.LockOptions;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.SubselectFetch;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.spi.QueryOptionsAdapter;
import org.hibernate.sql.exec.internal.BaseExecutionContext;
import org.hibernate.sql.results.graph.entity.LoadingEntityEntry;

/**
 * A copy of {@link org.hibernate.loader.ast.internal.SingleIdExecutionContext} in Hibernate ORM.
 */
class SingleIdExecutionContext extends BaseExecutionContext {
	private final Object entityInstance;
	private final Object entityId;
	private final Boolean readOnly;
	private final LockOptions lockOptions;
	private final SubselectFetch.RegistrationHandler subSelectFetchableKeysHandler;

	public SingleIdExecutionContext(
			Object entityId,
			Object entityInstance,
			Boolean readOnly,
			LockOptions lockOptions,
			SubselectFetch.RegistrationHandler subSelectFetchableKeysHandler,
			SharedSessionContractImplementor session) {
		super( session );
		this.entityInstance = entityInstance;
		this.entityId = entityId;
		this.readOnly = readOnly;
		this.lockOptions = lockOptions;
		this.subSelectFetchableKeysHandler = subSelectFetchableKeysHandler;
	}

	@Override
	public Object getEntityInstance() {
		return entityInstance;
	}

	@Override
	public Object getEntityId() {
		return entityId;
	}

	@Override
	public QueryOptions getQueryOptions() {
		return new QueryOptionsAdapter() {
			@Override
			public Boolean isReadOnly() {
				return readOnly;
			}

			@Override
			public LockOptions getLockOptions() {
				return lockOptions;
			}
		};
	}

	@Override
	public void registerLoadingEntityEntry(EntityKey entityKey, LoadingEntityEntry entry) {
		subSelectFetchableKeysHandler.addKey( entityKey, entry );
	}

}
