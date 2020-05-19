package org.hibernate.reactive.containers;

/**
 * Contains the common constants that we need for the configuration of databases
 * during tests.
 */
public interface DatabaseConfiguration {

	boolean USE_DOCKER = Boolean.getBoolean("docker");

	String USERNAME = "hreactive";
	String PASSWORD = "hreactive";
	String DB_NAME = "hreactive";

}
