# Hadoop / YARN config for Spark

These XML files are used by Spark when submitting jobs with **`--master yarn`**.

- **core-site.xml** — `fs.defaultFS` (HDFS namenode address).
- **yarn-site.xml** — ResourceManager address and NodeManager aux services.
- **hdfs-site.xml** — HDFS client options.

They are mounted into the Spark container as `HADOOP_CONF_DIR`, so `spark-submit --master yarn` can connect to YARN and HDFS. See **run.md** § 1.1 for Standalone vs YARN.
