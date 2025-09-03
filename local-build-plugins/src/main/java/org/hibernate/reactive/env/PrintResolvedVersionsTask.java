package org.hibernate.reactive.env;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.TaskAction;

import org.jetbrains.annotations.NotNull;

@CacheableTask
public abstract class PrintResolvedVersionsTask extends DefaultTask {

	@Classpath
	@NotNull
	public abstract ConfigurableFileCollection getClasspath();

	@TaskAction
	public void printVersions() {
		var hibernateCoreVersion = "n/a";
		var vertxVersion = "n/a";

		for ( final var file : getClasspath().getFiles() ) {
			String name = file.getName();
			if ( name.startsWith( "hibernate-core-" ) && name.endsWith( ".jar" ) ) {
				hibernateCoreVersion = name.substring( "hibernate-core-".length(), name.length() - 4 );
			}
			if ( name.startsWith( "vertx-sql-client-" ) && name.endsWith( ".jar" ) ) {
				vertxVersion = name.substring( "vertx-sql-client-".length(), name.length() - 4 );
			}
		}

		System.out.println( "Resolved Hibernate ORM Core Version: " + hibernateCoreVersion );
		System.out.println( "Resolved Vert.x SQL client Version: " + vertxVersion );
	}
}
