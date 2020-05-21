package org.hibernate.reactive.service;

import io.vertx.core.Vertx;
import org.hibernate.service.Service;

/**
 * Used to get the reference to the Vertx instance to use.
 * This is a service so to allow injecting an external instance,
 * or allow Hibernate Reactive to manage its own instance.
 */
public interface VertxService extends Service {

    Vertx getVertx();

}
