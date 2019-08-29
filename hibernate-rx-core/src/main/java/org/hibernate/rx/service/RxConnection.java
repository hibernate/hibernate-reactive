package org.hibernate.rx.service;

import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import org.hibernate.rx.RxSession;
import org.hibernate.service.spi.Wrapped;

public interface RxConnection extends Wrapped {
	CompletionStage<Void> inTransaction(
			Consumer<RxSession> consumer,
			RxSession delegate);
}

