package org.hibernate.rx.action.spi;

import java.util.concurrent.CompletionStage;

import org.hibernate.action.spi.Executable;

public interface RxExecutable extends Executable {
    CompletionStage<Void> rxExecute();
}
