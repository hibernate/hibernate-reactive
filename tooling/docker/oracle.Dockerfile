# Oracle Database Free
# See https://hub.docker.com/r/gvenzl/oracle-free
# NOTE: Only the FROM value is used by Testcontainers in our test suite.
# The rest of this file is for running the container manually (see podman.md).
FROM docker.io/gvenzl/oracle-free:23-slim-faststart
ENV ORACLE_PASSWORD=hreact
ENV ORACLE_DATABASE=hreact
ENV APP_USER=hreact
ENV APP_USER_PASSWORD=hreact
EXPOSE 1521
