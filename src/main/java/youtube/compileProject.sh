#!/bin/bash

mkdir classes
javac -d classes MRmapper.java
javac -d classes MRreducer.java
jar -cvf youtubeHadoop.jar -C classes/ .
javac -classpath $CLASSPATH:youtubeHadoop.jar -d classes MRdriver.java
jar -uvf youtubeHadoop.jar -C classes/ .
scp youtubeHadoop.jar root@e-ye:/user/emersonsjsu
