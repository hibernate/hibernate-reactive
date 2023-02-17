/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.tuple;

import java.util.concurrent.CompletionStage;
import org.hibernate.Incubating;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.BeforeExecutionGenerator;
import org.hibernate.generator.EventType;
import org.hibernate.reactive.stage.Stage;

@Incubating
public abstract class StageGenerator implements BeforeExecutionGenerator {

    @Override
    public Object generate(SharedSessionContractImplementor session, Object owner, Object currentValue,
                           EventType eventType) {
        throw new UnsupportedOperationException( "Use generate(Stage.Session, Object, Object, EventType) instead" );
    }

    public abstract CompletionStage<Object> generate(Stage.Session session, Object owner, Object currentValue,
                                                     EventType eventType);
}
