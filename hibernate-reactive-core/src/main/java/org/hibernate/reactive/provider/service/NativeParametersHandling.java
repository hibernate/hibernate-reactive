/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.DialectDelegateWrapper;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.sql.ast.internal.ParameterMarkerStrategyStandard;
import org.hibernate.sql.ast.spi.ParameterMarkerStrategy;
import org.hibernate.type.descriptor.jdbc.JdbcType;

/**
 * Replaces the JdbcParameterRendererInitiator so to not require
 * users to set AvailableSettings.DIALECT_NATIVE_PARAM_MARKERS : this
 * gets enforces as the Vert.x SQL clients require it.
 */
public class NativeParametersHandling implements StandardServiceInitiator<ParameterMarkerStrategy> {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	/**
	 * Singleton access
	 */
	public static final NativeParametersHandling INSTANCE = new NativeParametersHandling();

	@Override
	public ParameterMarkerStrategy initiateService(Map<String, Object> configurationValues, ServiceRegistryImplementor registry) {
		final Dialect dialect = registry.getService( JdbcServices.class ).getDialect();
		final Dialect realDialect = DialectDelegateWrapper.extractRealDialect( dialect );
		final ParameterMarkerStrategy renderer = recommendRendered( realDialect );
		LOG.debugf( "Initializing service JdbcParameterRenderer with implementation: %s", renderer.getClass() );
		return renderer;
	}

	/**
	 * Given a {@link Dialect}, returns the recommended {@link ParameterMarkerStrategy}.
	 * <p>
	 *     The default strategy is {@link ParameterMarkerStrategyStandard}.
	 * </p>
	 * @return the selected strategy for the dialect, never null
	 */
	private ParameterMarkerStrategy recommendRendered(Dialect realDialect) {
		if ( realDialect instanceof PostgreSQLDialect ) {
			return new PostgreSQLNativeParameterMarkers();
		}
		//TBD : Implementations for other DBs

		return realDialect.getNativeParameterMarkerStrategy() == null
				? ParameterMarkerStrategyStandard.INSTANCE
				: realDialect.getNativeParameterMarkerStrategy();
	}

	@Override
	public Class<ParameterMarkerStrategy> getServiceInitiated() {
		return ParameterMarkerStrategy.class;
	}

	private static class PostgreSQLNativeParameterMarkers implements ParameterMarkerStrategy {
		@Override
		public String createMarker(int position, JdbcType jdbcType) {
			return "$" + position;
		}
	}

}
