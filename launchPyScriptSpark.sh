if [ $# -gt 2 ] || [ $# -lt 1 ]
  then
    echo "Usage: launchPyScript scriptFile [-q]"
    echo "scriptFile: path and filename of Python Script to launch in Spark"
    echo "[-q]: Suppress stderr crap"
    exit 1
fi

if [ $# -eq 2  ] && [ $2 == "-q" ]
  then
    /usr/local/spark-1.6.1-bin-hadoop1/bin/spark-submit --master spark://master:7077 $1 2> /dev/null
    exit 0
  else
    /usr/local/spark-1.6.1-bin-hadoop1/bin/spark-submit --master spark://master:7077 $1
fi
