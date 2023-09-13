/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.impl;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.mutation.MergeCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveMergeCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveScopedUpdateCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveUpdateCoordinator;

public class ReactiveMergeCoordinatorStandardScopeFactory extends MergeCoordinator
		implements ReactiveUpdateCoordinator {

	public ReactiveMergeCoordinatorStandardScopeFactory(AbstractEntityPersister entityPersister, SessionFactoryImplementor factory) {
		super( entityPersister, factory );
	}

	@Override
	public ReactiveScopedUpdateCoordinator makeScopedCoordinator() {
		return new ReactiveMergeCoordinator(
				entityPersister(),
				factory(),
				this.getStaticUpdateGroup(),
				this.getBatchKey(),
				this.getVersionUpdateGroup(),
				this.getVersionUpdateBatchkey()
		);
	}
}
