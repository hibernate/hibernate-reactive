package org.hibernate.reactive.env;

import java.io.File;
import java.util.Optional;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.VersionCatalog;
import org.gradle.api.artifacts.VersionCatalogsExtension;
import org.gradle.api.artifacts.VersionConstraint;
import org.gradle.api.logging.Logger;
import org.gradle.api.provider.Provider;

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

	@Override
	public void apply(Project project) {
		final Logger log = project.getLogger();

		// Expose the version file as an extension (used during releases)
		final File versionFile = project.getRootProject().file( RELATIVE_FILE );
		project.getExtensions().add( VERSION_FILE, versionFile );

		// 1) Read release/development via -P if they come
		final ProjectVersion releaseVersion = findProjectVersion( RELEASE_VERSION, project );
		if ( releaseVersion != null ) {
			log.lifecycle( "Release version: {} ({})", releaseVersion.getFullName(), releaseVersion.getFamily() );
			project.getExtensions().add( RELEASE_VERSION, releaseVersion );
		}
		else {
			log.lifecycle( "Release version: n/a" );
		}

		final ProjectVersion developmentVersion = findProjectVersion( DEVELOPMENT_VERSION, project );
		if ( developmentVersion != null ) {
			log.lifecycle( "Development version: {} ({})", developmentVersion.getFullName(), developmentVersion.getFamily() );
			project.getExtensions().add( DEVELOPMENT_VERSION, developmentVersion );
		}
		else {
			log.lifecycle( "Development version: n/a" );
		}

		// 2) read projectVersion from version.properties using ValueSource (cacheable)
		final var projectVersionString = project.getProviders()
				.of( VersionPropertiesSource.class, spec -> {
					final var rf = project.getLayout().getProjectDirectory().file( RELATIVE_FILE );
					spec.getParameters().getFile().set( rf );
				}
		);

		final ProjectVersion projectVersion = determineProjectVersion( project, releaseVersion, projectVersionString );
		log.lifecycle( "Project version: {} ({})", projectVersion.getFullName(), projectVersion.getFamily() );
		project.getExtensions().add( PROJECT_VERSION, projectVersion );

		// 3) Version Catalog ("libs"): see gradle/libs.versions.toml
		final VersionCatalogsExtension catalogs = project.getExtensions().getByType( VersionCatalogsExtension.class );
		final VersionCatalog libs = catalogs.named( "libs" );

		final String ormVersionString = determineVersion( ORM_VERSION, project, libs );
		final Object ormVersion = resolveOrmVersion( ormVersionString, project );
		log.lifecycle( "ORM version: {}", ormVersion );
		project.getExtensions().add( ORM_VERSION, ormVersion );

		final String ormPluginVersion = determineVersion( ORM_PLUGIN_VERSION, project, libs );
		log.lifecycle( "ORM Gradle plugin version: {}", ormPluginVersion );
		project.getExtensions().add( ORM_PLUGIN_VERSION, ormPluginVersion );
	}

	private ProjectVersion findProjectVersion(String property, Project project) {
		if ( project.hasProperty( property ) ) {
			final Object version = project.property( property );
			if ( version != null ) {
				return new ProjectVersion( (String) version );
			}
		}
		return null;
	}

	// --- Project version (ValueSource for configuration cache) ---------------
	public static ProjectVersion determineProjectVersion(Project project, ProjectVersion releaseVersion, Provider<String> projectVersionString) {
		if ( releaseVersion != null ) {
			return releaseVersion;
		}
		// if we don't have an explicit release, use value from 'version/gradle.properties' (with ValueSource)
		final String fullName = projectVersionString.get();
		if ( fullName.isEmpty() ) {
			final var file = project.getRootProject().file( RELATIVE_FILE );
			throw new RuntimeException( "Property 'projectVersion' not found in " + file.getAbsolutePath() );
		}
		return new ProjectVersion( fullName );
	}

	private String determineVersion(String property, Project project, VersionCatalog libs) {
		// Check if the property is defined in the project
		if ( project.hasProperty( property ) ) {
			return (String) project.property( property );
		}

		// Otherwise, look for it in the catalog
		final Optional<VersionConstraint> vc = libs.findVersion( property );
		if ( vc.isPresent() ) {
			final String required = vc.get().getRequiredVersion();
			if ( !required.isEmpty() ) {
				return required;
			}
		}

		throw new IllegalStateException( "Property '" + property + "' not found in version catalog" );
	}

	private Object resolveOrmVersion(String stringForm, Project project) {
		if ( project.hasProperty( SKIP_ORM_VERSION_PARSING )
				&& Boolean.parseBoolean( (String) project.property( SKIP_ORM_VERSION_PARSING ) ) ) {
			return stringForm;
		}
		return new ProjectVersion( stringForm );
	}
}
