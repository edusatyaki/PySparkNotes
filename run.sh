#!/bin/zsh
# Run the build with Java 17 and the venv's Python for the Spark workers.
cd "$(dirname "$0")"
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH=$JAVA_HOME/bin:$PATH
export PYSPARK_PYTHON=$PWD/.venv/bin/python
.venv/bin/python build/build.py "$@" 2>&1 | grep -v -E "WARN|log4j|setLogLevel|incubator|psutil|UserWarning|warnings.warn"
