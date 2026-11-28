# Detector image (milestone 2, step 8): the Spark streaming job (src/streaming/job.py).
#
# Why our own image: the stock Spark image ships Python 3.10 and this repo is 3.11; the laptop has
# Java 8 and Spark 4 needs 17. Contents: Python 3.11, a Java 17 runtime, PySpark 4.2.1 and every
# other dependency exactly as uv.lock pins them, the Kafka source jars, and this repo's packages.
# Guardrail S5: base images by exact tag, jars by version + SHA-256 (each checked against Maven
# Central's SHA-1 when pinned, 2026-11-27). PySpark already bundles what kafka-clients needs at
# run time (lz4, snappy, zstd, slf4j) and jsr305 / scala-parallel-collections.
#
#   make detect          # builds this image and starts the job (Compose profile `spark`)

FROM ghcr.io/astral-sh/uv:0.13.8 AS uv
FROM eclipse-temurin:17.0.21.1_2-jre-noble AS java

FROM python:3.11.17-slim-bookworm

ENV JAVA_HOME=/opt/java/openjdk \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1
ENV PATH=/opt/venv/bin:$JAVA_HOME/bin:$PATH

COPY --from=java /opt/java/openjdk /opt/java/openjdk
COPY --from=uv /uv /usr/local/bin/uv

WORKDIR /app
# Dependencies first (cached until uv.lock changes), then the project itself, not editable. uv's
# download cache stays in a build cache mount, out of the image (it was ~3 GB).
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-default-groups --group spark --no-install-project
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-default-groups --group spark --no-editable

ARG MAVEN=https://repo1.maven.org/maven2
ARG JARS=/opt/venv/lib/python3.11/site-packages/pyspark/jars
ADD --checksum=sha256:ea37e79382c7be77156f86862a37a305e050a2ddb7415f60e785bd62ca2dd1df \
    ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.2.1/spark-sql-kafka-0-10_2.13-4.2.1.jar ${JARS}/
ADD --checksum=sha256:930fd5a3460dce68e9560c5f90437400b357764a51830d1aaa000a3ef71a3dd7 \
    ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.2.1/spark-token-provider-kafka-0-10_2.13-4.2.1.jar ${JARS}/
ADD --checksum=sha256:014b4ef36884478a932641818b080c36a8f8e2e92dc771e800c9d2694f4f83d8 \
    ${MAVEN}/org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar ${JARS}/
ADD --checksum=sha256:f77a5060d6936a9144023584232f9de3f2248f1abfe156e9d31795f18770674f \
    ${MAVEN}/org/apache/commons/commons-pool2/2.13.1/commons-pool2-2.13.1.jar ${JARS}/
RUN chmod 644 ${JARS}/*.jar

# Non-root; the checkpoint directory exists in the image so a new named volume takes its owner.
RUN useradd --uid 1000 --create-home app && mkdir /checkpoints && chown app /checkpoints
USER app
WORKDIR /home/app
ENTRYPOINT ["python", "-m", "streaming.job"]
