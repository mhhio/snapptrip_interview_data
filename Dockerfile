# Use the Bitnami Spark image as the base
FROM bitnami/spark:3.5.1

# Install required packages with pip
RUN pip3 install --no-cache-dir psycopg2-binary

# Switch to root user to install dependencies
USER root

# Move the JAR file to the Spark jars directory
COPY ./jars/postgresql-42.2.18.jar /opt/bitnami/spark/jars/
COPY ./log4j.properties /opt/bitnami/spark/

# Switch back to the non-root user
USER 1001