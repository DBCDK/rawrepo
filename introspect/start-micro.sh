#!/usr/bin/env bash
java $JAVA_OPTS \
	 	-Djava.util.logging.config.file=./logging.properties \
	 	-Dcontainer.hostname=$HOSTNAME \
	 	-cp "/home/ja7/ftp/payara-micro-4.1.2.173.jar:./json-log-formater.jar" json-logging fish.payara.micro.PayaraMicro --deploy target/rawrepo-introspect-1.5-SNAPSHOT.war --port 8081 \
		--logproperties ./logging.properties