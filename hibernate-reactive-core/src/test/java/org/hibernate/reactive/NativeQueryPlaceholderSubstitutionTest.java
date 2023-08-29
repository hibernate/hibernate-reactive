/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import static java.util.concurrent.TimeUnit.MINUTES;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@Timeout(value = 10, timeUnit = MINUTES)
public class NativeQueryPlaceholderSubstitutionTest extends BaseReactiveTest {

  @Override
  protected Collection<Class<?>> annotatedEntities() {
    return List.of(Widget.class);
  }

  @Test
  public void testThatSchemaGetsSubstitutedDuringNativeSelectQuery(VertxTestContext context) {

    test(context, getSessionFactory().withSession(session ->
      session.createNativeQuery("select count(*) from {h-schema}widgets", Integer.class)
          .getSingleResult()
          .thenAccept(result -> Assertions.assertEquals(0, result))
    ));
  }

  @Test
  public void testThatSchemaGetsSubstitutedDuringNativeNonSelectQuery(VertxTestContext context) {

    test(context, getSessionFactory().withSession(session ->
        session.createNativeQuery("update {h-schema}widgets set id = 1")
            .executeUpdate()
            .thenAccept(result -> Assertions.assertEquals(0, result))
    ));
  }

  @Entity(name = "Widget")
  @Table(name = "widgets")
  public static class Widget {

    @Id
    @GeneratedValue
    private Long id;

    @Override
    public String toString() {
      return "Widget{" +
          "id=" + id +
          '}';
    }
  }

}
