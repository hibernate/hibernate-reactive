package org.hibernate.reactive.cfg;

import org.hibernate.cfg.AvailableSettings;

public interface ReactiveSettings extends AvailableSettings {

    /**
     * References a {@link io.vertx.sqlclient.Pool} instance
     * to be used for reactive connections
     */
    String VERTX_POOL = "hibernate.reactive.connection.vertx.pool";

}
