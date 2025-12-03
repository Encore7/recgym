FROM bitnami/spark:3.5.0

USER root

# Delta Lake + Hadoop AWS dependencies
ENV DELTA_VERSION=3.2.0 \
    HADOOP_AWS_VERSION=3.3.4 \
    AWS_SDK_VERSION=1.12.262

# Add jars
ADD https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${DELTA_VERSION}/delta-core_2.12-${DELTA_VERSION}.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar /opt/bitnami/spark/jars/

USER 1001
