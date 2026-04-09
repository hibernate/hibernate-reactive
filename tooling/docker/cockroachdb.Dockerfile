# CockroachDB
# See https://hub.docker.com/r/cockroachdb/cockroach
# NOTE: Only the FROM value is used by Testcontainers in our test suite.
# The rest of this file is for running the container manually (see podman.md).
FROM docker.io/cockroachdb/cockroach:v26.1.2
EXPOSE 26257
CMD ["start-single-node", "--insecure"]
