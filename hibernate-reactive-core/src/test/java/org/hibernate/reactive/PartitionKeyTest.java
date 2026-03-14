/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.PartitionKey;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)
public class PartitionKeyTest extends BaseReactiveTest
{

    @Override
    protected Collection<Class<?>> annotatedEntities() {
        return List.of( User.class);
    }

    @Test
    public void test(VertxTestContext context){
        User user = new User();
        user.setFirstname("John");
        user.setLastname("Doe");
        user.setTenantKey("tenant1");

        test( context, getMutinySessionFactory()
                .withSession( session -> session.persist( user )
                        .chain( session::flush )
                        .invoke( () -> session.find( User.class, user.getId() ) )
                        .invoke( () -> user.setLastname("Cash"))
                        .chain( session::flush )
                        .chain( () -> session.find(User.class, user.getId()))
                        .invoke( () -> assertSurname(user))
                        .invoke( () -> session.remove( session.find(User.class, user.getId() ) ) )
                        .chain( session::flush )));

    }

    private void assertSurname(User user) {
        assertThat(user).isNotNull();
        assertThat(user.getLastname()).isEqualTo("Cash");
    }

    @Table(name = "user_tbl")
    @Entity(name = "User")
    static class User {

        @GeneratedValue
        @Id
        private Long id;

        private String firstname;

        private String lastname;

        @PartitionKey
        private String tenantKey;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getFirstname() {
            return firstname;
        }

        public void setFirstname(String firstname) {
            this.firstname = firstname;
        }

        public String getLastname() {
            return lastname;
        }

        public void setLastname(String lastname) {
            this.lastname = lastname;
        }

        public String getTenantKey() {
            return tenantKey;
        }

        public void setTenantKey(String tenantKey) {
            this.tenantKey = tenantKey;
        }
    }
}
