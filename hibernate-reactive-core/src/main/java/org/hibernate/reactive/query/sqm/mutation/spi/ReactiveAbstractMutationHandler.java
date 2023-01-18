/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.spi;


import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.query.sqm.tree.SqmDeleteOrUpdateStatement;
import org.hibernate.reactive.query.sqm.mutation.internal.ReactiveHandler;

/**
 * @see org.hibernate.query.sqm.mutation.spi.AbstractMutationHandler
 */
public interface ReactiveAbstractMutationHandler extends ReactiveHandler {

	SqmDeleteOrUpdateStatement<?> getSqmDeleteOrUpdateStatement();

	EntityMappingType getEntityDescriptor();

	SessionFactoryImplementor getSessionFactory();

}
