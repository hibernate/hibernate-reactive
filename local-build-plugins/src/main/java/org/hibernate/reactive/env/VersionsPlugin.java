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

		// Expose the version file as an extension
		final File versionFile = project.getRootProject().file( RELATIVE_FILE );
		project.getExtensions().add( VERSION_FILE, versionFile );

		// 1) release/development via -P if they come
		final ProjectVersion releaseVersion = determineReleaseVersion( project );
		final ProjectVersion developmentVersion = determineDevelopmentVersion( project );

		// 2) read projectVersion from version.properties using ValueSource (cacheable)
		final var projectVersionString = project.getProviders().of(
				VersionPropertiesSource.class, spec -> {
					final var rf = project.getLayout()
							.getProjectDirectory()
							.file( RELATIVE_FILE );
					spec.getParameters().getFile().set( rf );
				}
		);

		final ProjectVersion projectVersion = determineProjectVersion(
				project, releaseVersion, projectVersionString
		);

		log.lifecycle( "Project version: {} ({})", projectVersion.getFullName(), projectVersion.getFamily() );
		project.getExtensions().add( PROJECT_VERSION, projectVersion );

		if ( releaseVersion != null ) {
			log.lifecycle( "Release version: {} ({})", releaseVersion.getFullName(), releaseVersion.getFamily() );
			project.getExtensions().add( RELEASE_VERSION, releaseVersion );
		}
		else {
			log.lifecycle( "Release version: n/a" );
		}

		if ( developmentVersion != null ) {
			log.lifecycle(
					"Development version: {} ({})",
					developmentVersion.getFullName(),
					developmentVersion.getFamily()
			);
			project.getExtensions().add( DEVELOPMENT_VERSION, developmentVersion );
		}
		else {
			log.lifecycle( "Development version: n/a" );
		}

		// 3) Version Catalog ("libs") See local-build-plugins/settings.gradle
		final VersionCatalogsExtension catalogs = project.getExtensions().getByType( VersionCatalogsExtension.class );
		final VersionCatalog libs = catalogs.named( "libs" );

		final String ormVersionString = determineOrmVersion( project, libs );
		final Object ormVersion = resolveOrmVersion( ormVersionString, project );
		log.lifecycle( "ORM version: {}", ormVersion );
		project.getExtensions().add( ORM_VERSION, ormVersion );

		final Object ormPluginVersion = determineOrmPluginVersion( ormVersion, project, libs );
		log.lifecycle( "ORM Gradle plugin version: {}", ormPluginVersion );
		project.getExtensions().add( ORM_PLUGIN_VERSION, ormPluginVersion );
	}

	// --- Release / Development (with -P) --------------------------------------
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

	// --- Project version (ValueSource for configuration cache) ---------------

	public static ProjectVersion determineProjectVersion(
			Project project,
			ProjectVersion releaseVersion,
			Provider<String> projectVersionString
	) {
		if ( releaseVersion != null ) {
			return releaseVersion;
		}
		// if don't have an explicit release, use value of file (with ValueSource)
		final String fullName = projectVersionString.get();
		if ( fullName.isEmpty() ) {
			final var file = project.getRootProject().file( RELATIVE_FILE );
			throw new RuntimeException( "Property 'projectVersion' is missing in " + file.getAbsolutePath() );
		}
		return new ProjectVersion( fullName );
	}

	// --- ORM version from -P or catalogs ------------------------------------

	private String determineOrmVersion(Project project, VersionCatalog libs) {
		// Check if it has been set in the project
		// -PhibernateOrmVersion have priority
		if ( project.hasProperty( ORM_VERSION ) ) {
			return (String) project.property( ORM_VERSION );
		}
		// Find in Version Catalog
		final Optional<VersionConstraint> vc = libs.findVersion( ORM_VERSION );
		if ( vc.isPresent() ) {
			final String required = vc.get().getRequiredVersion();
			if ( !required.isEmpty() ) {
				return required;
			}
		}
		throw new IllegalStateException( "Hibernate ORM version not specified on project" );
	}

	private Object resolveOrmVersion(String stringForm, Project project) {
		if ( project.hasProperty( SKIP_ORM_VERSION_PARSING )
				&& Boolean.parseBoolean( (String) project.property( SKIP_ORM_VERSION_PARSING ) )
		) {
			return stringForm;
		}
		return new ProjectVersion( stringForm );
	}

	private Object determineOrmPluginVersion(Object ormVersion, Project project, VersionCatalog libs) {
		// Check if it has been set in the project
		// -PhibernateOrmGradlePluginVersion have priority
		if ( project.hasProperty( ORM_PLUGIN_VERSION ) ) {
			return project.property( ORM_PLUGIN_VERSION );
		}
		// Find in Version Catalog
		final Optional<VersionConstraint> vc = libs.findVersion( ORM_PLUGIN_VERSION );
		if ( vc.isPresent() ) {
			final String required = vc.get().getRequiredVersion();
			if ( !required.isEmpty() ) {
				return required;
			}
		}

		throw new IllegalStateException( "Hibernate ORM Gradle plugin version not specified on project" );
	}
}
