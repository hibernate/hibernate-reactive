/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.hibernate.context.spi.CurrentTenantIdentifierResolver;
import org.hibernate.reactive.containers.DatabaseConfiguration;

/**
 * For testing purpose, it allows us to select different tenants before opening the session.
 */
public class MyCurrentTenantIdentifierResolver implements CurrentTenantIdentifierResolver {

	public enum Tenant {
		/**
		 * Vert.x checks if a database exists before running the queries.
		 * We set the default one as the regular one we use for the other tests.
		 */
		DEFAULT( DatabaseConfiguration.DB_NAME ),
		TENANT_1( "dbtenant1" ),
		TENANT_2( "dbtenant2" );

		private String dbName;

		Tenant(String dbName) {
			this.dbName = dbName;
		}

		/**
		 * @return the name of the database for the selected tenant
		 */
		public String getDbName() {
			return dbName;
		}
	}

	private Tenant tenantId = Tenant.DEFAULT;

	public void setTenantIdentifier(Tenant tenantId) {
		this.tenantId = tenantId;
	}

	@Override
	public String resolveCurrentTenantIdentifier() {
		return tenantId.name();
	}

	@Override
	public boolean validateExistingCurrentSessions() {
		return false;
	}
}
