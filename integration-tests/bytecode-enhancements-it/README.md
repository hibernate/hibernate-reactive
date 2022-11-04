This module will test bytecode enhancements with PostgreSQL.

It duplicates some code from the tests in hibernate-reactive-core and enable bytecode enhancements via
the Hibernate gradle plugin.

Note that it will only enhance the entity if the classes are in the /src/main/java folder.

It's not an elegant solution, but it's better than not having test.
As soon as we figure out a better way, we will replace this module.