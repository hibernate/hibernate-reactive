/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.it.quarkus.qe.database;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.testcontainers.utility.DockerImageName;

/**
 * A utility class with methods to generate a {@link DockerImageName} for Testcontainers.
 * <p>
 * Testcontainers might not work if the image required is available in multiple different registries (for example when
 * using podman instead of docker).
 * These methods make sure to pick a registry as default.
 * </p>
 */
public final class DockerImage {

	/**
	 * The absolute path of the project root that we have set in Gradle.
	 */
	private static final String PROJECT_ROOT = System.getProperty( "hibernate.reactive.project.root" );

	/**
	 * The path to the directory containing all the Dockerfile files
	 */
	private static final Path DOCKERFILE_DIR_PATH = Path.of( PROJECT_ROOT ).resolve( "tooling" ).resolve( "docker" );

	/**
	 * Extract the image name and version from the first FROM instruction in the Dockerfile.
	 * Note that everything else is ignored.
	 */
	public static DockerImageName fromDockerfile(String databaseName) {
		try {
			final ImageInformation imageInformation = readFromInstruction( databaseName.toLowerCase() );
			return imageName( imageInformation.getRegistry(), imageInformation.getImage(), imageInformation.getVersion() );
		}
		catch (IOException e) {
			throw new RuntimeException( e );
		}
	}

	public static DockerImageName imageName(String registry, String image, String version) {
		return DockerImageName
				.parse( registry + "/" + image + ":" + version )
				.asCompatibleSubstituteFor( image );
	}

	private static class ImageInformation {
		private final String registry;
		private final String image;
		private final String version;

		public ImageInformation(String fullImageInfo) {
			// FullImageInfo pattern: <registry>/<image>:<version>
			// For example: "docker.io/cockroachdb/cockroach:v24.3.13" becomes registry = "docker.io", image = "cockroachdb/cockroach", version = "v24.3.13"
			final int registryEndPos = fullImageInfo.indexOf( '/' );
			final int imageEndPos = fullImageInfo.lastIndexOf( ':' );
			this.registry = fullImageInfo.substring( 0, registryEndPos );
			this.image = fullImageInfo.substring( registryEndPos + 1, imageEndPos );
			this.version = fullImageInfo.substring( imageEndPos + 1 );
		}

		public String getRegistry() {
			return registry;
		}

		public String getImage() {
			return image;
		}

		public String getVersion() {
			return version;
		}

		@Override
		public String toString() {
			return registry + "/" + image + ":" + version;
		}
	}

	private static Path dockerFilePath(String database) {
		// Get project root from system property set by Gradle, with fallback
		return DOCKERFILE_DIR_PATH.resolve( database.toLowerCase() + ".Dockerfile" );
	}

	private static ImageInformation readFromInstruction(String database) throws IOException {
		return readFromInstruction( dockerFilePath( database ) );
	}

	/**
	 * Read a Dockerfile and extract the first FROM instruction.
	 *
	 * @param dockerfilePath path to the Dockerfile
	 * @return the first FROM instruction found, or empty if none found
	 * @throws IOException if the file cannot be read
	 */
	private static ImageInformation readFromInstruction(Path dockerfilePath) throws IOException {
		if ( !Files.exists( dockerfilePath ) ) {
			throw new FileNotFoundException( "Dockerfile not found: " + dockerfilePath );
		}

		List<String> lines = Files.readAllLines( dockerfilePath );
		for ( String line : lines ) {
			// Skip comments and empty lines
			String trimmedLine = line.trim();
			if ( trimmedLine.isEmpty() || trimmedLine.startsWith( "#" ) ) {
				continue;
			}

			if ( trimmedLine.startsWith( "FROM " ) ) {
				return new ImageInformation( trimmedLine.substring( "FROM ".length() ) );
			}
		}

		throw new IOException( " Missing FROM instruction in " + dockerfilePath );
	}
}
