#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-1.8.0/bin

$JAVA_HOME/java -cp "/home/hadoop/riskAnalytics/riskAnalytics-0.0.1-SNAPSHOT.jar:/home/hadoop/riskAnalytics/riskAnalytics1_lib/*" com.vij.riskAnalytics.MarketDataHistoryReader
$JAVA_HOME/java -cp "/home/hadoop/riskAnalytics/riskAnalytics-0.0.1-SNAPSHOT.jar:/home/hadoop/riskAnalytics/riskAnalytics1_lib/*" com.vij.riskAnalytics.TradeDataReader
