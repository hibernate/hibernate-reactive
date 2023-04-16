/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.metamodel.mapping.internal;

import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.metamodel.mapping.internal.PluralAttributeMappingImpl;
import org.hibernate.reactive.sql.results.graph.collection.internal.ReactiveCollectionDomainResult;
import org.hibernate.reactive.sql.results.graph.collection.internal.ReactiveEagerCollectionFetch;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.ast.tree.from.TableGroup;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.DomainResultCreationState;
import org.hibernate.sql.results.graph.Fetch;
import org.hibernate.sql.results.graph.FetchParent;

public class ReactivePluralAttributeMapping extends PluralAttributeMappingImpl implements PluralAttributeMapping {

	public ReactivePluralAttributeMapping(PluralAttributeMappingImpl original) {
		super( original );
	}

	@Override
	public <T> DomainResult<T> createDomainResult(
			NavigablePath navigablePath,
			TableGroup tableGroup,
			String resultVariable,
			DomainResultCreationState creationState) {
		final TableGroup collectionTableGroup = creationState.getSqlAstCreationState()
				.getFromClauseAccess()
				.getTableGroup( navigablePath );

		assert collectionTableGroup != null;

		// This is only used for collection initialization where we know the owner is available, so we mark it as visited
		// which will cause bidirectional to-one associations to be treated as such and avoid a join
		creationState.registerVisitedAssociationKey( getKeyDescriptor().getAssociationKey() );

		return new ReactiveCollectionDomainResult( navigablePath, this, resultVariable, tableGroup, creationState );
	}

	@Override
	protected Fetch buildEagerCollectionFetch(
			NavigablePath fetchedPath,
			PluralAttributeMapping fetchedAttribute,
			TableGroup collectionTableGroup,
			FetchParent fetchParent,
			DomainResultCreationState creationState) {
		return new ReactiveEagerCollectionFetch(
				fetchedPath,
				fetchedAttribute,
				collectionTableGroup,
				fetchParent,
				creationState
		);
	}
}
