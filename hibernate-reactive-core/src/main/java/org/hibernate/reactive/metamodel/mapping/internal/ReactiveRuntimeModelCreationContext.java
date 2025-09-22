/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.metamodel.mapping.internal;

import org.hibernate.boot.model.relational.Database;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.boot.spi.BootstrapContext;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.cache.spi.CacheImplementor;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.generator.Generator;
import org.hibernate.generator.GeneratorCreationContext;
import org.hibernate.mapping.GeneratorSettings;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Property;
import org.hibernate.mapping.RootClass;
import org.hibernate.mapping.SimpleValue;
import org.hibernate.mapping.Value;
import org.hibernate.metamodel.spi.MappingMetamodelImplementor;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.query.sqm.function.SqmFunctionRegistry;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.Type;
import org.hibernate.type.descriptor.java.spi.JavaTypeRegistry;
import org.hibernate.type.spi.TypeConfiguration;

import java.util.Map;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.reactive.generator.values.internal.ReactiveGeneratedValuesHelper.augmentWithReactiveGenerator;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;

public class ReactiveRuntimeModelCreationContext implements RuntimeModelCreationContext {

	private static final Log LOG = make( Log.class, lookup() );

	private final RuntimeModelCreationContext delegate;

	public ReactiveRuntimeModelCreationContext(RuntimeModelCreationContext delegate) {
		this.delegate = delegate;
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

	@Override
	public Generator getOrCreateIdGenerator(String rootName, PersistentClass persistentClass){
		final Generator existing = getGenerators().get( rootName );
		if ( existing != null ) {
			return existing;
		}
		else {
			final SimpleValue identifier = (SimpleValue) persistentClass.getIdentifier();
			final Generator idgenerator = augmentWithReactiveGenerator(
					identifier.createGenerator(
							getDialect(),
							persistentClass.getRootClass(),
							persistentClass.getIdentifierProperty(),
							getGeneratorSettings()
					),
					new IdGeneratorCreationContext(
							persistentClass.getRootClass(),
							persistentClass.getIdentifierProperty(),
							getGeneratorSettings(),
							identifier,
							this
					),
					this );
			getGenerators().put( rootName, idgenerator );
			return idgenerator;
		}
	}

	private record IdGeneratorCreationContext(
			RootClass rootClass,
			Property property,
			GeneratorSettings defaults,
			SimpleValue identifier,
			RuntimeModelCreationContext buildingContext) implements GeneratorCreationContext {

		@Override
		public Database getDatabase() {
			return buildingContext.getBootModel().getDatabase();
		}

		@Override
		public ServiceRegistry getServiceRegistry() {
			return buildingContext.getBootstrapContext().getServiceRegistry();
		}

		@Override
		public SqlStringGenerationContext getSqlStringGenerationContext() {
			return defaults.getSqlStringGenerationContext();
		}

		@Override
		public String getDefaultCatalog() {
			return defaults.getDefaultCatalog();
		}

		@Override
		public String getDefaultSchema() {
			return defaults.getDefaultSchema();
		}

		@Override
		public RootClass getRootClass() {
			return rootClass;
		}

		@Override
		public PersistentClass getPersistentClass() {
			return rootClass;
		}

		@Override
		public Property getProperty() {
			return property;
		}

		@Override
		public Value getValue() {
			return identifier;
		}

		@Override
		public Type getType() {
			return identifier.getType();
		}
	}

}
