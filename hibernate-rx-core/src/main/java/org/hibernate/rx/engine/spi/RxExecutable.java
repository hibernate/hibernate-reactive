package org.hibernate.rx.engine.spi;

import org.hibernate.action.spi.Executable;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

/**
 * An operation that is scheduled for later non-blocking
 * execution in an {@link RxActionQueue}. Reactive counterpart
 * to {@link Executable}.
 */
public interface RxExecutable extends Executable, Comparable, Serializable {
	CompletionStage<Void> rxExecute();
}
