/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.common;

import org.hibernate.Incubating;

import jakarta.persistence.metamodel.SingularAttribute;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a value of an attribute that forms part of the
 * natural key of an entity.
 *
 * @see org.hibernate.annotations.NaturalId
 *
 * @param <T> the owning entity class
 */
@Incubating
public abstract class Identifier<T> {

    public abstract Id<T>[] ids();

    public abstract Map<String,Object> namedValues();

    public static class Id<T> extends Identifier<T> {
        private <I> Id(SingularAttribute<T,I> attribute, I id) {
            this.id = id;
            this.attributeName = attribute.getName();
        }

        private Id(String attributeName, Object id) {
            this.attributeName = attributeName;
            this.id = id;
        }

        private final String attributeName;
        private final Object id;

        public Object getId() {
            return id;
        }

        public String getAttributeName() {
            return attributeName;
        }

        @Override
        public Id<T>[] ids() {
            return new Id[]{this};
        }

        @Override
        public Map<String, Object> namedValues() {
            return Collections.singletonMap(attributeName, id);
        }
    }

    public static class Composite<T> extends Identifier<T> {
        Id<T>[] ids;

        public Composite(Id<T>[] ids) {
            this.ids = ids;
        }

        public Id<T>[] getValues() {
            return ids;
        }

        @Override
        public Id<T>[] ids() {
            return ids;
        }

        @Override
        public Map<String, Object> namedValues() {
            Map<String, Object> namedValues = new HashMap<>();
            for (Id<T> id : ids) {
                namedValues.put( id.getAttributeName(), id.getId() );
            }
            return namedValues;
        }
    }

    public static <T,I> Id<T> id(SingularAttribute<T,I> attribute, I id) {
        return new Id<>(attribute, id);
    }

    public static <T,I> Id<T> id(String attributeName, Object id) {
        return new Id<>(attributeName, id);
    }

    public static <T,I> Id<T> id(Class<T> entityClass, String attributeName, Object id) {
        return new Id<>(attributeName, id);
    }

    @SafeVarargs
    public static <T,I> Identifier<T> composite(Id<T>... ids) {
        return new Composite<>(ids);
    }
}
