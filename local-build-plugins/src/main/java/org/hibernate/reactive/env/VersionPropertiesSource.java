package org.hibernate.reactive.env;

import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ValueSource;
import org.gradle.api.provider.ValueSourceParameters;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * Reads gradle/version.properties and returns the "projectVersion" property.
 * Compatible with Configuration Cache (Gradle declares the file as input).
 */
public abstract class VersionPropertiesSource implements ValueSource<String, VersionPropertiesSource.Params> {
	public interface Params extends ValueSourceParameters {
		RegularFileProperty getFile();
	}

	@Override
	public String obtain() {
		final var file = getParameters().getFile().getAsFile().get();
		if ( !file.exists() ) {
			throw new RuntimeException( "Version file " + file.getAbsolutePath() + " does not exists" );
		}
		final var props = new Properties();
		try (final var in = new FileInputStream( file )) {
			props.load( in );
			return props.getProperty( "projectVersion" );
		}
		catch (Exception e) {
			throw new RuntimeException( "Unable to load properties from file - " + file.getAbsolutePath(), e );
		}
	}
}
