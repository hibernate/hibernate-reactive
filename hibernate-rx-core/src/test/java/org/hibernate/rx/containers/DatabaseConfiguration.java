package org.hibernate.rx.containers;

public interface DatabaseConfiguration {
	
	public static final boolean USE_DOCKER = Boolean.getBoolean( "docker" );
	
	public static final String USERNAME = "hibernate-rx";
	public static final String PASSWORD = "hibernate-rx";
	public static final String DB_NAME = "hibernate-rx";

}
