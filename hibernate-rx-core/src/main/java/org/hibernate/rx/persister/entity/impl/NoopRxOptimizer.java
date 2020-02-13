package org.hibernate.rx.persister.entity.impl;

import org.hibernate.AssertionFailure;
import org.hibernate.id.IdentifierGeneratorHelper;
import org.hibernate.id.IntegralDataTypeHolder;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

public class NoopRxOptimizer implements RxOptimizer<Long> {

	@Override
	public synchronized CompletionStage<Optional<Long>> generate(RxAccessCallback callback) {
		return callback.getNextValue();
	}
}
