package org.hibernate.rx.util.impl;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Documents that a non-final field is only ever modified during the bootstrapping phase, making it effectively final
 * for the remainder of its lifecycle. The annotated field thus may be considered as safely published to other threads.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.FIELD)
public @interface EffectivelyFinal {
}
