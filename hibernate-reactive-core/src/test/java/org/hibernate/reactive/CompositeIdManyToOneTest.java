/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;

@Ignore
public class CompositeIdManyToOneTest extends BaseReactiveTest {

    @Override
    protected Collection<Class<?>> annotatedEntities() {
        return List.of( GroceryList.class, ShoppingItem.class);
    }

    @Test
    public void reactivePersist(TestContext context) {

        GroceryList gl = new GroceryList();
        gl.id = 4L;
        ShoppingItem si = new ShoppingItem();
        si.groceryList = gl;
//        si.groceryListId = gl.id;
        si.itemName = "Milk";
        si.itemCount = 2;
        gl.shoppingItems.add(si);

        test(
                context,
                openSession()
                        .thenCompose( s -> s.persist( gl )
                                .thenCompose( v -> s.flush() )
                        ).thenCompose( v -> openSession() )
                        .thenCompose( s -> s.createQuery("from ShoppingItem si where si.groceryList.id = :gl")
                                .setParameter("gl", gl.id)
                                .getResultList() )
                        .thenAccept( list -> context.assertEquals( 1, list.size() ) )
        );
    }

    @Entity(name = "GroceryList")
    public static class GroceryList implements Serializable {

        @Id private Long id;

        @OneToMany(mappedBy = "groceryList", fetch = FetchType.LAZY, cascade = CascadeType.ALL, orphanRemoval = true)
        private List<ShoppingItem> shoppingItems = new ArrayList<>();

    }

    @Entity(name = "ShoppingItem")
    @IdClass(ShoppingItemId.class)
    public static class ShoppingItem implements Serializable {

//        @Id Long groceryListId;
        @Id private String itemName;

        @Id @ManyToOne
        @JoinColumn(name = "grocerylistid")
        private GroceryList groceryList;


        private int itemCount;

    }

    public static class ShoppingItemId implements Serializable {
        private String itemName;
        private GroceryList groceryList;
    }
}
