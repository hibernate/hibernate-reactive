/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;
import java.math.BigInteger;

@Converter
class BinInteger implements AttributeConverter<BigInteger,String> {
	@Override
	public String convertToDatabaseColumn(BigInteger attribute) {
		return attribute==null ? null : attribute.toString(2);
	}

	@Override
	public BigInteger convertToEntityAttribute(String string) {
		return string==null ? null : new BigInteger(string, 2);
	}
}
