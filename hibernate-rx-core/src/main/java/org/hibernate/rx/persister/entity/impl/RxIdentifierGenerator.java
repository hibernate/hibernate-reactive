package org.hibernate.rx.persister.entity.impl;

import org.hibernate.engine.spi.SessionFactoryImplementor;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface RxIdentifierGenerator {
	CompletionStage<Optional<Integer>> generate(SessionFactoryImplementor factory);
}
