#!/usr/bin/perl

$hadoop_prefix = `echo \$HADOOP_PREFIX`;
chomp $hadoop_prefix;

$outputdir = "articleCount";
$inputdir = "articleInput";
print `$hadoop_prefix/bin/hadoop fs -rm -r $outputdir`;
print `$hadoop_prefix/bin/hadoop jar $hadoop_prefix/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar -file wordcount_mapper.py  -mapper wordcount_mapper.py -file wordcount_reducer.py -reducer wordcount_reducer.py -combiner wordcount_reducer.py -input $inputdir -output $outputdir`;
print `$hadoop_prefix/bin/hadoop fs -cat $outputdir/part-00000 > newanswer`;
