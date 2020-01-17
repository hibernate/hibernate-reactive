package org.hibernate.rx;

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
