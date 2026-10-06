FROM flink:1.20.1

# The official image ships the S3 filesystem plugin under /opt/flink/opt but
# does not activate it. The Operator-created JobManager and TaskManager pods
# need the plugin directory to make s3:// checkpoint/savepoint paths usable.
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
    && cp /opt/flink/opt/flink-s3-fs-hadoop-1.20.1.jar /opt/flink/plugins/s3-fs-hadoop/

COPY job/target/zio-flink-wordcount-0.1.0.jar /opt/flink/usrlib/zio-flink-jobs.jar
