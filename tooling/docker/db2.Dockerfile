# IBM DB2
# See https://hub.docker.com/r/ibmcom/db2
# NOTE: Only the FROM value is used by Testcontainers in our test suite.
# The rest of this file is for running the container manually (see podman.md).
FROM icr.io/db2_community/db2:12.1.3.0
ENV DB2INSTANCE=hreact
ENV DB2INST1_PASSWORD=hreact
ENV DBNAME=hreact
ENV LICENSE=accept
ENV AUTOCONFIG=false
ENV ARCHIVE_LOGS=false
EXPOSE 50000
