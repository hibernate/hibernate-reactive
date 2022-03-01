/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

class MariaDatabase extends MySQLDatabase {

	static MariaDatabase INSTANCE = new MariaDatabase();

	public final static String IMAGE_NAME = "mariadb:10.7.1";

	/**
	 * Holds configuration for the MariaDB database container. If the build is run with <code>-Pdocker</code> then
	 * Testcontainers+Docker will be used.
	 * <p>
	 * TIP: To reuse the same containers across multiple runs, set `testcontainers.reuse.enable=true` in a file located
	 * at `$HOME/.testcontainers.properties` (create the file if it does not exist).
	 */
	public static final VertxMariaContainer maria = new VertxMariaContainer( IMAGE_NAME )
			.withUsername( DatabaseConfiguration.USERNAME )
			.withPassword( DatabaseConfiguration.PASSWORD )
			.withDatabaseName( DatabaseConfiguration.DB_NAME )
			.withReuse( true );

	protected MariaDatabase() {
	}

	@Override
	public String getConnectionUri() {
		return connectionUri( maria );
	}

	@Override
	public String getDefaultUrl() {
		return super.getDefaultUrl().replaceAll( "^mysql:", "mariadb:" );
	}
}
