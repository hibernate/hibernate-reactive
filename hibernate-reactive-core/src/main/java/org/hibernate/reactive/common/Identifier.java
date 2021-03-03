/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.common;

import org.hibernate.Incubating;

import javax.persistence.metamodel.SingularAttribute;

/**
 * Represents a value of an attribute that forms part of the
 * natural key of an entity.
 *
 * @see org.hibernate.annotations.NaturalId
 *
 * @param <T> the owning entity class
 */
@Incubating
public class Identifier<T> {
    private final String attributeName;
    private final Object id;

    private <I> Identifier(SingularAttribute<T,I> attribute, I id) {
        this.id = id;
        this.attributeName = attribute.getName();
    }

    private Identifier(Class<T> entityClass, String attributeName, Object id) {
        this.attributeName = attributeName;
        this.id = id;
    }

    public static <T,I> Identifier<T> value(SingularAttribute<T,I> attribute, I id) {
        return new Identifier<T>(attribute, id);
    }

    public static <T,I> Identifier<T> value(Class<T> entityClass, String attributeName, Object id) {
        return new Identifier<T>(entityClass, attributeName, id);
    }

    public Object getId() {
        return id;
    }

    public String getAttributeName() {
        return attributeName;
    }
}
