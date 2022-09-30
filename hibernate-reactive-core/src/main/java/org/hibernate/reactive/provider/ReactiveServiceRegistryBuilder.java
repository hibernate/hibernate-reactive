/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.boot.cfgxml.internal.ConfigLoader;
import org.hibernate.boot.cfgxml.spi.LoadedConfig;
import org.hibernate.boot.registry.BootstrapServiceRegistry;
import org.hibernate.boot.registry.BootstrapServiceRegistryBuilder;
import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.boot.registry.internal.StandardServiceRegistryImpl;
import org.hibernate.cfg.Environment;
import org.hibernate.internal.util.PropertiesHelper;
import org.hibernate.reactive.provider.impl.ReactiveServiceInitiators;
import org.hibernate.service.ServiceRegistry;

/**
 * Adaptation of {@link StandardServiceRegistryBuilder}; the main difference
 * is the use of a different initial set of services and the fact this will
 * not attempt to load service contributors and other {@code Integrators}.
 *
 * @see ReactiveServiceInitiators
 */
public final class ReactiveServiceRegistryBuilder extends StandardServiceRegistryBuilder {

    public static StandardServiceRegistryBuilder forJpa(BootstrapServiceRegistry bootstrapServiceRegistry) {
        final LoadedConfig loadedConfig = new LoadedConfig( null ) {
            @Override
            protected void addConfigurationValues(Map configurationValues) {
                // here, do nothing
            }
        };
        return new StandardServiceRegistryBuilder(
                bootstrapServiceRegistry,
                new HashMap<>(),
                new ConfigLoader( bootstrapServiceRegistry ),
                loadedConfig,
                defaultReactiveInitiatorList()
        ) {
            @Override
            public StandardServiceRegistryBuilder configure(LoadedConfig loadedConfig) {
                super.getAggregatedCfgXml().merge( loadedConfig );
                // super also collects the properties - here we skip that part
                return this;
            }
        };
    }

    /**
     * Create a default builder.
     */
    public ReactiveServiceRegistryBuilder() {
        this( new BootstrapServiceRegistryBuilder().enableAutoClose().build() );
    }

    /**
     * Create a builder with the specified bootstrap services.
     *
     * @param bootstrapServiceRegistry Provided bootstrap registry to use.
     */
    public ReactiveServiceRegistryBuilder(BootstrapServiceRegistry bootstrapServiceRegistry) {
        this( bootstrapServiceRegistry, LoadedConfig.baseline() );
    }

    /**
     * Create a builder with the specified bootstrap services.
     *
     * @param bootstrapServiceRegistry Provided bootstrap registry to use.
     */
    public ReactiveServiceRegistryBuilder(BootstrapServiceRegistry bootstrapServiceRegistry, LoadedConfig loadedConfigBaseline) {
        this(
                bootstrapServiceRegistry,
                PropertiesHelper.map( Environment.getProperties() ),
                loadedConfigBaseline,
                defaultReactiveInitiatorList()
        );
    }

    /**
     * Intended for use exclusively from JPA boot-strapping, or extensions of
     * this class. Consider this an SPI.
     *
     * @see #forJpa
     */
    ReactiveServiceRegistryBuilder(
            BootstrapServiceRegistry bootstrapServiceRegistry,
            Map<String,Object> settings,
            LoadedConfig loadedConfig) {
        this( bootstrapServiceRegistry, settings, loadedConfig, defaultReactiveInitiatorList() );
    }

    /**
     * Intended for use exclusively from Quarkus bootstrapping, or extensions of
     * this class which need to override the standard ServiceInitiator list.
     * Consider this an SPI.
     */
    ReactiveServiceRegistryBuilder(
            BootstrapServiceRegistry bootstrapServiceRegistry,
            Map<String,Object> settings,
            LoadedConfig loadedConfig,
            @SuppressWarnings("rawtypes")
            List<StandardServiceInitiator<?>> initiators) {
        super(
                bootstrapServiceRegistry,
                settings,
                new ConfigLoader( bootstrapServiceRegistry ),
                loadedConfig,
                initiators
        );
    }

    /**
     * Destroy a service registry.  Applications should only destroy registries they have explicitly created.
     *
     * @param serviceRegistry The registry to be closed.
     */
    public static void destroy(ServiceRegistry serviceRegistry) {
        if ( serviceRegistry == null ) {
            return;
        }

        ( (StandardServiceRegistryImpl) serviceRegistry ).destroy();
    }

    @SuppressWarnings("rawtypes")
    private static List<StandardServiceInitiator<?>> defaultReactiveInitiatorList() {
        final List<StandardServiceInitiator<?>> initiators = new ArrayList<>( ReactiveServiceInitiators.LIST.size() );
        initiators.addAll( ReactiveServiceInitiators.LIST );
        return initiators;
    }

}
