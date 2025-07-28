/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.embeddable.internal;

import org.hibernate.metamodel.mapping.NonAggregatedIdentifierMapping;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.InitializerParent;
import org.hibernate.sql.results.graph.embeddable.EmbeddableInitializer;
import org.hibernate.sql.results.graph.embeddable.internal.EmbeddableForeignKeyResultImpl;

public class ReactiveEmbeddableForeignKeyResultImpl<T> extends EmbeddableForeignKeyResultImpl<T> {

	public ReactiveEmbeddableForeignKeyResultImpl(EmbeddableForeignKeyResultImpl<T> original) {
		super( original );
	}

	@Override
	public EmbeddableInitializer<?> createInitializer(InitializerParent parent, AssemblerCreationState creationState) {
		return getReferencedModePart() instanceof NonAggregatedIdentifierMapping
			   ? new ReactiveNonAggregatedIdentifierMappingInitializer( this, null, creationState, true )
			   : new ReactiveEmbeddableInitializerImpl( this, null, null, null, creationState, true );
	}
}
