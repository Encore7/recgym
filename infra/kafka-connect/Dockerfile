FROM confluentinc/cp-kafka-connect:7.4.0
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.4.3
ENV CONNECT_PLUGIN_PATH="/usr/share/confluent-hub-components,/usr/share/java"
