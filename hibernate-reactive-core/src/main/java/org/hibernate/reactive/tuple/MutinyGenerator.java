/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.tuple;

import io.smallrye.mutiny.Uni;
import org.hibernate.Incubating;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.BeforeExecutionGenerator;
import org.hibernate.generator.EventType;
import org.hibernate.reactive.mutiny.Mutiny;

@Incubating
public abstract class MutinyGenerator implements BeforeExecutionGenerator {

    @Override
    public Object generate(SharedSessionContractImplementor session, Object owner, Object currentValue,
                           EventType eventType) {
        throw new UnsupportedOperationException( "Use generate(Mutiny.Session, Object, Object, EventType) instead" );
    }

    public abstract Uni<Object> generate(Mutiny.Session session, Object owner, Object currentValue,
                                         EventType eventType);
}
