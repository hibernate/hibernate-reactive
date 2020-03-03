package org.hibernate.rx.util.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class RxUtil {

	public static <T, R> CompletionStage<R> zipArray(
			Function<? super Object[], ? extends R> zipper,
			@SuppressWarnings("unchecked") CompletionStage<? extends T>... sources) {
		Object[] results = new Object[sources.length];
		CompletionStage<?> state = nullFuture();
		for ( int i = 0; i < sources.length; i++ ) {
			CompletionStage<? extends T> completionStage = sources[i];
			int finalI = i;
			CompletionStage<Void> stage = completionStage.thenAccept( result -> {
				results[finalI] = result;
			} );
			state = state.thenCompose( v -> stage );
		}
		return state.thenApply( v -> zipper.apply( results ) );
	}

	public static CompletionStage<Void> nullFuture() {
		return completedFuture( null );
	}

	public static <T> CompletionStage<T> completedFuture(T value) {
		return CompletableFuture.completedFuture( value );
	}

	public static <T> CompletionStage<T> failedFuture(Throwable t) {
		CompletableFuture<T> ret = new CompletableFuture<>();
		ret.completeExceptionally( t );
		return ret;
	}

	public static <T extends Throwable, Ret> Ret rethrow(Throwable x) throws T {
		throw (T) x;
	}

	public static <T extends Throwable, Ret> void rethrowIfNotNull(Throwable x) throws T {
		if (x != null ) {
			throw (T) x;
		}
	}
}
