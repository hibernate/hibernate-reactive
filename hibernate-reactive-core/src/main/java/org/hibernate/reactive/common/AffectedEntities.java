/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.hibernate.Incubating;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.internal.util.collections.ArrayHelper;

import static java.util.Collections.addAll;

/**
 * A description of the entities and tables affected by a native query.
 * <p>
 * The entities and tables given by an instance passed to
 * {@code createNativeQuery()} must be synchronized with the database
 * before execution of the query.
 *
 * @author Gavin King
 */
@Incubating
public class AffectedEntities {
	private static final String[] NO_TABLES = new String[0];

	private final String[] queryTables;
	private final Class<?>[] queryEntities;

	public AffectedEntities(Class<?>... queryEntities) {
		this.queryTables = NO_TABLES;
		this.queryEntities = queryEntities;
	}

	public String[] getAffectedTables() {
		return queryTables;
	}

	public Class<?>[] getAffectedEntities() {
		return queryEntities;
	}

	public String[] getAffectedSpaces(SessionFactoryImplementor factory) {
		List<String> spaces = new ArrayList<>();
		addAll( spaces, getAffectedTables() );
		for ( Class<?> entity : getAffectedEntities() ) {
			Serializable[] querySpaces = factory.getMappingMetamodel()
					.getEntityDescriptor( entity.getName() )
					.getQuerySpaces();
			spaces.addAll( Arrays.asList( (String[]) querySpaces ) );
		}
		return spaces.toArray( ArrayHelper.EMPTY_STRING_ARRAY );
	}
}
