/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.boot.registry.selector.spi.StrategySelector;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.tool.schema.spi.SchemaManagementTool;

public class ReactiveSchemaManagementToolInitiator  implements StandardServiceInitiator<SchemaManagementTool> {
	public static final ReactiveSchemaManagementToolInitiator INSTANCE = new ReactiveSchemaManagementToolInitiator();

	public SchemaManagementTool initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		final Object setting = configurationValues.get( AvailableSettings.SCHEMA_MANAGEMENT_TOOL );
		SchemaManagementTool tool = registry.getService( StrategySelector.class ).resolveStrategy( SchemaManagementTool.class, setting );
		if ( tool == null ) {
			tool = new ReactiveSchemaManagementTool();
		}

		return tool;
	}

	@Override
	public Class<SchemaManagementTool> getServiceInitiated() {
		return SchemaManagementTool.class;
	}
}
