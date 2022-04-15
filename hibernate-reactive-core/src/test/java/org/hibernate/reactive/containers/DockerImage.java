/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

import org.testcontainers.utility.DockerImageName;

/**
 * A utility class with methods to generate {@link DockerImageName} for testcontainers.
 * <p>
 * Testcontainers might not work if the image required is available in multiple different registries (for example when
 * using podman instead of docker).
 * These methods make sure to pick a registry as default.
 * </p>
 */
public final class DockerImage {

	public static final String DEFAULT_REGISTRY = "docker.io";

	public static DockerImageName imageName(String image, String version) {
		return imageName( DEFAULT_REGISTRY, image, version );
	}

	public static DockerImageName imageName(String registry, String image, String version) {
		return DockerImageName
				.parse( registry + "/" + image + ":" + version )
				.asCompatibleSubstituteFor( image );
	}
}
