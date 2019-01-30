package org.hibernate.rx;

import java.util.concurrent.CompletableFuture;

public interface StateControl {

	void clear();

	CompletableFuture<Void> refresh(Object var1);

	CompletableFuture<Void> flush();

}
