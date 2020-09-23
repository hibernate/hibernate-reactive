/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.orm;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.persistence.SharedCacheMode;
import javax.persistence.ValidationMode;
import javax.persistence.spi.ClassTransformer;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.persistence.spi.PersistenceUnitTransactionType;
import javax.sql.DataSource;

public class HibernatePersistenceUnitInfo implements PersistenceUnitInfo {

	public static String JPA_VERSION = "2.1";
	private String persistenceUnitName;
	private PersistenceUnitTransactionType transactionType
			= PersistenceUnitTransactionType.RESOURCE_LOCAL;
	private List<String> managedClassNames;
	private List<String> mappingFileNames = new ArrayList<>();
	private Properties properties;
	private DataSource jtaDataSource;
	private DataSource nonjtaDataSource;
	private List<ClassTransformer> transformers = new ArrayList<>();

	public HibernatePersistenceUnitInfo(
			String persistenceUnitName, List<String> managedClassNames, Properties properties) {
		this.persistenceUnitName = persistenceUnitName;
		this.managedClassNames = managedClassNames;
		this.properties = properties;
	}

	// standard setters / getters

	public String getPersistenceUnitName() {
		return persistenceUnitName;
	}

	@Override
	public String getPersistenceProviderClassName() {
		return null;
	}

	public void setPersistenceUnitName(String persistenceUnitName) {
		this.persistenceUnitName = persistenceUnitName;
	}

	public PersistenceUnitTransactionType getTransactionType() {
		return transactionType;
	}

	public void setTransactionType(PersistenceUnitTransactionType transactionType) {
		this.transactionType = transactionType;
	}

	public List<String> getManagedClassNames() {
		return managedClassNames;
	}

	@Override
	public boolean excludeUnlistedClasses() {
		return false;
	}

	@Override
	public SharedCacheMode getSharedCacheMode() {
		return null;
	}

	@Override
	public ValidationMode getValidationMode() {
		return null;
	}

	public void setManagedClassNames(List<String> managedClassNames) {
		this.managedClassNames = managedClassNames;
	}

	public List<String> getMappingFileNames() {
		return mappingFileNames;
	}

	@Override
	public List<URL> getJarFileUrls() {
		return null;
	}

	@Override
	public URL getPersistenceUnitRootUrl() {
		return null;
	}

	public void setMappingFileNames(List<String> mappingFileNames) {
		this.mappingFileNames = mappingFileNames;
	}

	public Properties getProperties() {
		return properties;
	}

	@Override
	public String getPersistenceXMLSchemaVersion() {
		return null;
	}

	@Override
	public ClassLoader getClassLoader() {
		return null;
	}

	@Override
	public void addTransformer(ClassTransformer transformer) {

	}

	@Override
	public ClassLoader getNewTempClassLoader() {
		return null;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public DataSource getJtaDataSource() {
		return jtaDataSource;
	}

	@Override
	public DataSource getNonJtaDataSource() {
		return null;
	}

	public void setJtaDataSource(DataSource jtaDataSource) {
		this.jtaDataSource = jtaDataSource;
	}

	public DataSource getNonjtaDataSource() {
		return nonjtaDataSource;
	}

	public void setNonjtaDataSource(DataSource nonjtaDataSource) {
		this.nonjtaDataSource = nonjtaDataSource;
	}

	public List<ClassTransformer> getTransformers() {
		return transformers;
	}

	public void setTransformers(List<ClassTransformer> transformers) {
		this.transformers = transformers;
	}
}
