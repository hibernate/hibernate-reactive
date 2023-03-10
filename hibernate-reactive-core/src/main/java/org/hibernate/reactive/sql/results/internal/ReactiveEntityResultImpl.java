/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.internal;

import org.hibernate.metamodel.mapping.EntityValuedModelPart;
import org.hibernate.reactive.sql.results.graph.entity.internal.ReactiveEntityAssembler;
import org.hibernate.reactive.sql.results.graph.entity.internal.ReactiveEntityResultInitializer;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.ast.tree.from.TableGroup;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResultAssembler;
import org.hibernate.sql.results.graph.FetchParentAccess;
import org.hibernate.sql.results.graph.Initializer;
import org.hibernate.sql.results.graph.entity.internal.EntityResultImpl;

public class ReactiveEntityResultImpl extends EntityResultImpl {
	public ReactiveEntityResultImpl(
			NavigablePath navigablePath,
			EntityValuedModelPart entityValuedModelPart,
			TableGroup tableGroup,
			String resultVariable) {
		super( navigablePath, entityValuedModelPart, tableGroup, resultVariable );
	}

	@Override
	public DomainResultAssembler createResultAssembler(
			FetchParentAccess parentAccess,
			AssemblerCreationState creationState) {
		final Initializer initializer = creationState.resolveInitializer(
				getNavigablePath(),
				getReferencedModePart(),
				() -> new ReactiveEntityResultInitializer(
						this,
						getNavigablePath(),
						getLockMode( creationState ),
						getIdentifierFetch(),
						getDiscriminatorFetch(),
						getRowIdResult(),
						creationState
				)
		);

		return new ReactiveEntityAssembler( this.getResultJavaType(), initializer.asEntityInitializer() );
	}
}
