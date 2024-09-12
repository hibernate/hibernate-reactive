package org.hibernate.reactive.env;

/**
 * @author Steve Ebersole
 */
public class ProjectVersion {
	final String major;
	final String minor;
	final boolean snapshot;

	private final String family;
	private final String fullName;

	ProjectVersion(String fullName) {
		this.fullName = fullName;

		try {
			final String[] hibernateVersionComponents = fullName.split( "\\." );
			major = hibernateVersionComponents[0];
			minor = hibernateVersionComponents[1];
		}
		catch (Exception e) {
			throw new IllegalArgumentException( "Invalid version number: " + fullName + "." );
		}

		family = major + "." + minor;
		snapshot = fullName.endsWith( "-SNAPSHOT" );
	}

	public String getMajor() {
		return major;
	}

	public String getMinor() {
		return minor;
	}

	public String getFullName() {
		return fullName;
	}

	public String getFamily() {
		return family;
	}

	public boolean isSnapshot() {
		return snapshot;
	}

	@Override
	public String toString() {
		return fullName;
	}
}
