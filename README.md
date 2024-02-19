# TestSparkLocalEnv


Deploy Spark using Docker-compose
we will explore deploying Apache Spark using Docker-compose. Docker-compose simplifies the process of managing multi-container applications and provides an ideal solution for setting up Spark clusters in a development environment. We will cover the creation of a Dockerfile for building the Spark image, configuring the entrypoint script to manage Spark workloads, and creating a Docker Compose file to define the Spark cluster’s services. By the end of this part, you will have a working Spark cluster running on your local machine, ready to process data and run Spark jobs.
ref: https://medium.com/@SaphE/testing-apache-spark-locally-docker-compose-and-kubernetes-deployment-94d35a54f222

run spark jobs : ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 spark-stream.py 