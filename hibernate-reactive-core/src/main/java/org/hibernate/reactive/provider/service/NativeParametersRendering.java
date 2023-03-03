/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.DialectDelegateWrapper;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.sql.ast.internal.JdbcParameterRendererStandard;
import org.hibernate.sql.ast.spi.JdbcParameterRenderer;

public class NativeParametersRendering implements StandardServiceInitiator<JdbcParameterRenderer> {
	/**
	 * Singleton access
	 */
	public static final NativeParametersRendering INSTANCE = new NativeParametersRendering();

	@Override
	public JdbcParameterRenderer initiateService(Map<String, Object> configurationValues, ServiceRegistryImplementor registry) {
		final Dialect dialect = registry.getService( JdbcEnvironment.class ).getDialect();
		final Dialect realDialect = DialectDelegateWrapper.extractRealDialect( dialect );
		if ( realDialect instanceof PostgreSQLDialect ) {
			return PostgreSQLParameterRenderer.INSTANCE;
		}
		//TODO: Create optimised implementations for the other most relevant dialects
		return JdbcParameterRendererStandard.INSTANCE;
	}

	@Override
	public Class<JdbcParameterRenderer> getServiceInitiated() {
		return JdbcParameterRenderer.class;
	}

}
