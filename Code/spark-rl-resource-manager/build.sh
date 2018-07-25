#!/usr/local/bin/bash

# current working directory
SRC_DIR=$(pwd)

# sbt publish dir
SBT_DIR="/Users/d058715/.ivy2/local/default/spark-rl-resource-manager_2.11"

# maven publish dir
M2_DIR="/Users/d058715/.m2/repository/default/spark-rl-resource-manager_2.11"

# delete both
# rm -rf "${SBT_DIR}"
# rm -rf "${M2_DIR}"

# republish everything
sbt publishLocal
sbt publishM2