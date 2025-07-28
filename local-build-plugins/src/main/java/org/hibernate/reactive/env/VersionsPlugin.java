package org.hibernate.reactive.env;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.function.Consumer;

import org.gradle.api.Plugin;
import org.gradle.api.Project;

/**
 * Build plugin which applies some DSL extensions to the Project.  Currently, these
 * extensions are all related to version.
 *
 * @author Steve Ebersole
 */
public class VersionsPlugin implements Plugin<Project> {
	public static final String VERSION_FILE = "versionFile";

	public static final String PROJECT_VERSION = "projectVersion";
	public static final String RELEASE_VERSION = "releaseVersion";
	public static final String DEVELOPMENT_VERSION = "developmentVersion";

	public static final String ORM_VERSION = "hibernateOrmVersion";
	public static final String ORM_PLUGIN_VERSION = "hibernateOrmGradlePluginVersion";
	public static final String SKIP_ORM_VERSION_PARSING = "skipOrmVersionParsing";

	public static final String RELATIVE_FILE = "gradle/version.properties";
	public static final String RELATIVE_CATALOG = "gradle/libs.versions.toml";

	@Override
	public void apply(Project project) {
		final File versionFile = project.getRootProject().file( RELATIVE_FILE );
		project.getExtensions().add( VERSION_FILE, versionFile );

		final ProjectVersion releaseVersion = determineReleaseVersion( project );
		final ProjectVersion developmentVersion = determineDevelopmentVersion( project );
		final ProjectVersion projectVersion = determineProjectVersion( project, releaseVersion, versionFile );

		project.getLogger().lifecycle( "Project version: {} ({})", projectVersion.getFullName(), projectVersion.getFamily() );
		project.getExtensions().add( PROJECT_VERSION, projectVersion );

		if ( releaseVersion != null ) {
			project.getLogger().lifecycle( "Release version: {} ({})", releaseVersion.getFullName(), releaseVersion.getFamily() );
			project.getExtensions().add( RELEASE_VERSION, releaseVersion );
		}
		else {
			project.getLogger().lifecycle( "Release version: n/a" );
		}

		if ( developmentVersion != null ) {
			project.getLogger().lifecycle( "Development version: {} ({})", developmentVersion.getFullName(), developmentVersion.getFamily() );
			project.getExtensions().add( DEVELOPMENT_VERSION, developmentVersion );
		}
		else {
			project.getLogger().lifecycle( "Development version: n/a" );
		}

		final VersionsTomlParser tomlParser = new VersionsTomlParser( project.getRootProject().file( RELATIVE_CATALOG ) );
		final String ormVersionString = determineOrmVersion( project, tomlParser );
		final Object ormVersion = resolveOrmVersion( ormVersionString, project );
		project.getLogger().lifecycle( "ORM version: {}", ormVersion );
		project.getExtensions().add( ORM_VERSION, ormVersion );

		final Object ormPluginVersion = determineOrmPluginVersion( ormVersion, project, tomlParser );
		project.getLogger().lifecycle( "ORM Gradle plugin version: {}", ormPluginVersion );
		project.getExtensions().add( ORM_PLUGIN_VERSION, ormPluginVersion );
	}

	private ProjectVersion determineReleaseVersion(Project project) {
		if ( project.hasProperty( RELEASE_VERSION ) ) {
			final Object version = project.property( RELEASE_VERSION );
			if ( version != null ) {
				return new ProjectVersion( (String) version );
			}
		}
		return null;
	}

	private ProjectVersion determineDevelopmentVersion(Project project) {
		if ( project.hasProperty( DEVELOPMENT_VERSION ) ) {
			final Object version = project.property( DEVELOPMENT_VERSION );
			if ( version != null ) {
				return new ProjectVersion( (String) version );
			}
		}
		return null;
	}

	public static ProjectVersion determineProjectVersion(Project project, ProjectVersion releaseVersion, File versionFile) {
		if ( releaseVersion != null ) {
			return releaseVersion;
		}

		final String fullName = readVersionProperties( versionFile );
		return new ProjectVersion( fullName );
	}

	private static String readVersionProperties(File file) {
		if ( !file.exists() ) {
			throw new RuntimeException( "Version file " + file.getAbsolutePath() + " does not exists" );
		}

		final Properties versionProperties = new Properties();
		withInputStream( file, (stream) -> {
			try {
				versionProperties.load( stream );
			}
			catch (IOException e) {
				throw new RuntimeException( "Unable to load properties from file - " + file.getAbsolutePath(), e );
			}
		} );

		return versionProperties.getProperty( "projectVersion" );
	}

	private static void withInputStream(File file, Consumer<InputStream> action) {
		try ( final FileInputStream stream = new FileInputStream( file ) ) {
			action.accept( stream );
		}
		catch (IOException e) {
			throw new RuntimeException( "Error reading file stream = " + file.getAbsolutePath(), e );
		}
	}

	private String determineOrmVersion(Project project, VersionsTomlParser parser) {
		// Check if it has been set in the project
		if ( project.hasProperty( ORM_VERSION ) ) {
			return (String) project.property( ORM_VERSION );
		}

		// Check in the catalog
		final String version = parser.read( ORM_VERSION );
		if ( version != null ) {
			return version;
		}
		throw new IllegalStateException( "Hibernate ORM version not specified on project" );
	}

	private Object resolveOrmVersion(String stringForm, Project project) {
		if ( project.hasProperty( SKIP_ORM_VERSION_PARSING )
				&& Boolean.parseBoolean( (String) project.property( SKIP_ORM_VERSION_PARSING ) ) ) {
			return stringForm;
		}
		return new ProjectVersion( stringForm );
	}

	private Object determineOrmPluginVersion(Object ormVersion, Project project, VersionsTomlParser parser) {
		// Check if it has been set in the project
		if ( project.hasProperty( ORM_PLUGIN_VERSION ) ) {
			return project.property( ORM_PLUGIN_VERSION );
		}

		// Check in the catalog
		final String version = parser.read( ORM_PLUGIN_VERSION );
		if ( version != null ) {
			return version;
		}

		throw new IllegalStateException( "Hibernate ORM Gradle plugin version not specified on project" );
	}
}
