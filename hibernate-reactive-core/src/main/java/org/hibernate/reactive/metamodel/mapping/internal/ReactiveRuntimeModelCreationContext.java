/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.metamodel.mapping.internal;

import java.util.Map;

import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.boot.spi.BootstrapContext;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.cache.spi.CacheImplementor;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.generator.Generator;
import org.hibernate.mapping.GeneratorSettings;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.metamodel.spi.MappingMetamodelImplementor;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.query.sqm.function.SqmFunctionRegistry;
import org.hibernate.reactive.tuple.entity.ReactiveEntityMetamodel;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.tuple.entity.EntityMetamodel;
import org.hibernate.type.descriptor.java.spi.JavaTypeRegistry;
import org.hibernate.type.spi.TypeConfiguration;

public class ReactiveRuntimeModelCreationContext implements RuntimeModelCreationContext {

	private final RuntimeModelCreationContext delegate;

	public ReactiveRuntimeModelCreationContext(RuntimeModelCreationContext delegate) {
		this.delegate = delegate;
	}

	@Override
	public EntityMetamodel createEntityMetamodel(PersistentClass persistentClass, EntityPersister persister) {
		return new ReactiveEntityMetamodel( persistentClass, persister, delegate );
	}

	@Override
	public SessionFactoryImplementor getSessionFactory() {
		return delegate.getSessionFactory();
	}

	@Override
	public BootstrapContext getBootstrapContext() {
		return delegate.getBootstrapContext();
	}

	@Override
	public MetadataImplementor getBootModel() {
		return delegate.getBootModel();
	}

	@Override
	public MappingMetamodelImplementor getDomainModel() {
		return delegate.getDomainModel();
	}

	@Override
	public TypeConfiguration getTypeConfiguration() {
		return delegate.getTypeConfiguration();
	}

	@Override
	public JavaTypeRegistry getJavaTypeRegistry() {
		return delegate.getJavaTypeRegistry();
	}

	@Override
	public MetadataImplementor getMetadata() {
		return delegate.getMetadata();
	}

	@Override
	public SqmFunctionRegistry getFunctionRegistry() {
		return delegate.getFunctionRegistry();
	}

	@Override
	public Map<String, Object> getSettings() {
		return delegate.getSettings();
	}

	@Override
	public Dialect getDialect() {
		return delegate.getDialect();
	}

	@Override
	public CacheImplementor getCache() {
		return delegate.getCache();
	}

	@Override
	public SessionFactoryOptions getSessionFactoryOptions() {
		return delegate.getSessionFactoryOptions();
	}

	@Override
	public JdbcServices getJdbcServices() {
		return delegate.getJdbcServices();
	}

	@Override
	public SqlStringGenerationContext getSqlStringGenerationContext() {
		return delegate.getSqlStringGenerationContext();
	}

	@Override
	public ServiceRegistry getServiceRegistry() {
		return delegate.getServiceRegistry();
	}

	@Override
	public Map<String, Generator> getGenerators() {
		return delegate.getGenerators();
	}

	@Override
	public GeneratorSettings getGeneratorSettings() {
		return delegate.getGeneratorSettings();
	}
}
