/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.types;

import io.vertx.core.json.JsonObject;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

@Converter
public class StringToJson implements AttributeConverter<String, JsonObject> {

    @Override
    public JsonObject convertToDatabaseColumn(String string) {
        return string == null ? null : new JsonObject( string );
    }

    @Override
    public String convertToEntityAttribute(JsonObject dbData) {
        return dbData == null ? null : dbData.encodePrettily();
    }
}
