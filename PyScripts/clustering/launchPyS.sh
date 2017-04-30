if [ $# -gt 2 ] || [ $# -lt 1 ]
  then
    echo "Usage: launchPyScript scriptFile [-q]"
    echo "scriptFile: path and filename of Python Script to launch in Spark"
    echo "[-q]: Suppress stderr crap"
    exit 1
fi

if [ $# -eq 2  ] && [ $2 == "-q" ]
  then
/home/cc/installed_stuff/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --master yarn --deploy-mode cluster $1 2> /dev/null
    exit 0
  else
    /home/cc/installed_stuff/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --master yarn --deploy-mode cluster $1
fi
