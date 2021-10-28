#!/bin/bash

mvn clean package

training=../datasets/retailrocket9_train.txt

JAVA_OPTS="-Xmx8g -XshowSettings:vm"

for predictions in $(ls -1 *prediction*txt); do
  echo ${predictions}
  java $JAVA_OPTS -jar target/evaluator-0.0.1-SNAPSHOT-jar-with-dependencies.jar ${training} ${predictions}
done

