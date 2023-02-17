/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.mutation;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import java.util.concurrent.CompletionStage;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.BeforeExecutionGenerator;
import org.hibernate.generator.EventType;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.mutiny.impl.MutinySessionFactoryImpl;
import org.hibernate.reactive.mutiny.impl.MutinySessionImpl;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.stage.impl.StageSessionImpl;
import org.hibernate.reactive.tuple.MutinyGenerator;
import org.hibernate.reactive.tuple.StageGenerator;

final class GeneratorValueUtil {

    private GeneratorValueUtil() {
    }


    static CompletionStage<?> generateValue(
            SharedSessionContractImplementor session, Object entity, Object currentValue,
            BeforeExecutionGenerator generator, EventType eventType) {
        if (generator instanceof StageGenerator) {
            final Stage.Session stageSession = new StageSessionImpl( (ReactiveSession) session );
            return ((StageGenerator) generator).generate(stageSession, entity, currentValue, eventType);
        }

        if (generator instanceof MutinyGenerator) {
            MutinySessionFactoryImpl mutinyFactory = new MutinySessionFactoryImpl( (SessionFactoryImpl) session.getFactory() );
            Mutiny.Session mutinySession = new MutinySessionImpl( (ReactiveSession) session, mutinyFactory );
            return ((MutinyGenerator) generator).generate(mutinySession, entity, currentValue, eventType).subscribeAsCompletionStage();
        }


        // We should throw an exception, but I don't want to break things for people using @CreationTimestamp or similar
        // annotations. We need an alternative for Hibernate Reactive.
        return completedFuture( generator.generate( session, entity, currentValue, eventType) );
    }
}
