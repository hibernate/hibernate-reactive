package org.hibernate.rx.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public class Handlers {

    // Simplest implementation for now
    static <T> CompletionStage<T> toCompletionStage(Consumer<Handler<AsyncResult<T>>> completionConsumer) {
        CompletableFuture<T> cs = new CompletableFuture<>();
        try {
            completionConsumer.accept( ar -> {
                if (ar.succeeded()) {
                    cs.complete( ar.result() );
                }
                else {
                    cs.completeExceptionally( ar.cause() );
                }
            } );
        }
        catch (Exception e) {
            // unsure we need this ?
            cs.completeExceptionally(e);
        }
        return cs;
    }

    static <T> void execute(io.vertx.sqlclient.PreparedQuery<T> delegate, io.vertx.sqlclient.Tuple tuple, Handler<AsyncResult<T>> handler) {
        delegate.execute( tuple, ar -> {
            if ( ar.succeeded() ) {
                handler.handle( io.vertx.core.Future.succeededFuture( ar.result() ) );
            }
            else {
                handler.handle( io.vertx.core.Future.failedFuture( ar.cause() ) );
            }
        } );
    }

    static <T> void execute(io.vertx.sqlclient.PreparedQuery<T> delegate, Handler<AsyncResult<T>> handler) {
        delegate.execute( ar -> {
            if ( ar.succeeded() ) {
                handler.handle( io.vertx.core.Future.succeededFuture( ar.result() ) );
            }
            else {
                handler.handle( io.vertx.core.Future.failedFuture( ar.cause() ) );
            }
        } );
    }
}