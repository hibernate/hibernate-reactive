package org.hibernate.rx.containers;

/**
 * Contains the common constants that we need for the configuration of databases
 * during tests.
 */
public interface DatabaseConfiguration {
	
	boolean USE_DOCKER = Boolean.getBoolean( "docker" );

	String USERNAME = "hibernate-rx";
	String PASSWORD = "hibernate-rx";
	String DB_NAME = "hibernate-rx";

}
