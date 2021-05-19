Rapid Reactive testing with JBang

If you want to see them in action, here's how it works:

1. [Download JBang](https://www.jbang.dev/download)
2. Start the default Postgres we use for testing (you can replace podman with docker):
    ```
    podman run --rm --name HibernateTestingPGSQL \
    -e POSTGRES_USER=hreact -e POSTGRES_PASSWORD=hreact -e POSTGRES_DB=hreact \
    -p 5432:5432 postgres:13.2
    ```
3. JBang it:
    ```
    jbang https://github.com/hibernate/hibernate-reactive/tree/main/tooling/jbang/Example.java
    ```
   or
    ```
    jbang https://github.com/hibernate/hibernate-reactive/tree/main/tooling/jbang/SampleIssueTest.java
    ```
4. (Optional) If you want to edit the class with Idea
    ```
    jbang edit --open=idea https://github.com/hibernate/hibernate-reactive/tree/main/tooling/jbang/SampleIssueTest.java
    ```

_NOTE: For testing with other DB's see [podman.md](https://github.com/hibernate/hibernate-reactive/blob/main/podman.md)_.