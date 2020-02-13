package org.hibernate.rx.persister.entity.impl;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface RxOptimizer<T> {
	CompletionStage<Optional<T>> generate(RxAccessCallback callback);
}
