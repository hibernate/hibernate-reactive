package org.hibernate.rx.cfg;

import org.hibernate.cfg.AvailableSettings;

public interface AvailableRxSettings extends AvailableSettings {

    /**
     * References a {@link io.vertx.sqlclient.Pool} instance
     * to be used for reactive connections
     */
    String VERTX_POOL = "hibernate.rx.connection.vertx.pool";

}
