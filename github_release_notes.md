### Highlights

* Compatible with Hibernate ORM 8.0.0.CR1.
* `PessimisticLockScope.EXTENDED` now locks the collection tables, like Hibernate ORM ([#4515](https://github.com/hibernate/hibernate-reactive/issues/4515)).
* Transactions marked for rollback only are rolled back ([#4448](https://github.com/hibernate/hibernate-reactive/issues/4448)).
* `VertxFactory` creates Vert.x with other options, for example to enable metrics ([#4491](https://github.com/hibernate/hibernate-reactive/issues/4491)).
* JSON arrays, and JSON on MariaDB, Oracle and SQL Server.
* Fixes for the lock timeout of the connections ([#4523](https://github.com/hibernate/hibernate-reactive/issues/4523)) and for the identifier generators under load ([#1611](https://github.com/hibernate/hibernate-reactive/issues/1611)).
* Upgrading from Hibernate Reactive 4.x? See the [migration guide](https://docs.hibernate.org/reactive/{{releaseVersionFamily}}/migration-guide/).

* See the [website](https://hibernate.org/reactive/releases/{{releaseVersionFamily}}) for requirements and compatibilities.
* See the [What's New](https://hibernate.org/reactive/releases/{{releaseVersionFamily}}/#whats-new) guide for details about new features and capabilities.
* See the [issues fixed in this release](https://github.com/hibernate/hibernate-reactive/issues?q=milestone%3A{{tagName}}+is%3Aclosed).
