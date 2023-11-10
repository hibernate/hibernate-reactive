/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.internal;

import org.hibernate.metamodel.mapping.internal.ToOneAttributeMapping;
import org.hibernate.reactive.sql.results.graph.entity.internal.ReactiveEntityDelayedFetchInitializer;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.DomainResultAssembler;
import org.hibernate.sql.results.graph.FetchParent;
import org.hibernate.sql.results.graph.FetchParentAccess;
import org.hibernate.sql.results.graph.Initializer;
import org.hibernate.sql.results.graph.entity.internal.EntityDelayedFetchImpl;

public class ReactiveEntityDelayedFetchImpl extends EntityDelayedFetchImpl {
	public ReactiveEntityDelayedFetchImpl(
			FetchParent fetchParent,
			ToOneAttributeMapping fetchedAttribute,
			NavigablePath navigablePath,
			DomainResult<?> keyResult,
			boolean selectByUniqueKey) {
		super( fetchParent, fetchedAttribute, navigablePath, keyResult, selectByUniqueKey );
	}

	@Override
	protected Initializer buildEntityDelayedFetchInitializer(
			FetchParentAccess parentAccess,
			NavigablePath navigablePath,
			ToOneAttributeMapping entityValuedModelPart,
			boolean selectByUniqueKey,
			DomainResultAssembler<?> resultAssembler) {
		return new ReactiveEntityDelayedFetchInitializer( parentAccess, navigablePath, entityValuedModelPart, selectByUniqueKey, resultAssembler );
	}
}
