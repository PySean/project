$HADOOP_PREFIX/bin/hadoop fs -rmr clusteredArticles
$HADOOP_PREFIX/bin/hadoop fs -mkdir clusteredArticles
$HADOOP_PREFIX/bin/hadoop fs -moveFromLocal clust0 clusteredArticles/.
$HADOOP_PREFIX/bin/hadoop fs -moveFromLocal clust1 clusteredArticles/.
$HADOOP_PREFIX/bin/hadoop fs -moveFromLocal clust2 clusteredArticles/.
