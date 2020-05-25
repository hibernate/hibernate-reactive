/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.boot.impl;

import org.hibernate.boot.cfgxml.spi.LoadedConfig;
import org.hibernate.boot.registry.BootstrapServiceRegistry;
import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.service.StandardServiceInitiators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReactiveServiceRegistryBuilder extends StandardServiceRegistryBuilder {

    public static StandardServiceRegistryBuilder forJpa(BootstrapServiceRegistry bootstrapServiceRegistry) {
        final LoadedConfig loadedConfig = new LoadedConfig( null ) {
            @Override
            protected void addConfigurationValues(Map configurationValues) {
                // here, do nothing
            }
        };
        return new StandardServiceRegistryBuilder(
                bootstrapServiceRegistry,
                new HashMap(),
                loadedConfig,
                defaultReactiveInitiatorList()
        ) {
            @Override
            public StandardServiceRegistryBuilder configure(LoadedConfig loadedConfig) {
                getAggregatedCfgXml().merge( loadedConfig );
                // super also collects the properties - here we skip that part
                return this;
            }
        };
    }

    private static List<StandardServiceInitiator> defaultReactiveInitiatorList() {
        final List<StandardServiceInitiator> initiators = new ArrayList<>( ReactiveServiceInitiators.LIST.size() );
        initiators.addAll( StandardServiceInitiators.LIST );
        return initiators;
    }

}
