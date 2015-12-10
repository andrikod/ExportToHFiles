# ExportToHFiles
MapReduce Job to export HTables into HFiles for Bulk Import for HBase v0.92. The HFiles can be later imported into HBase via the org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles job.

## Problem
The task required to migrate the HBase data of an old Hadoop cluster to a new one. Both clusters were Cloudera clusters, but they differed in versions:

Old Cloudera Cluster
 `CDH: 4.0.1  HBase: 0.92`
 
New Cloudera Cluster
 `CDH 5.3.2    HBase  0.98`

The provided HBase tools to backup HTables and restore them back or into another cluster were unfortunately not suitable. The MapReduce jobs like: 
[Export and Import] (https://hbase.apache.org/book.html#_export)
[CopyTable] (https://hbase.apache.org/book.html#copytable)

assume that the HBase version between the clusters is compatible, which was not true for the transition from v0.92 to v0.98.

Solutions such as to export the data into text files were not suitable as well, due to the binary format of the HTables' data.

## Solution

Inspired by the [cloudera blog entry](http://blog.cloudera.com/blog/2013/09/how-to-use-hbase-bulk-loading-and-why/), the idea was to write a custom MR Job, which exports the data of a HTable into HFiles (`HFileOutputFormat2`), and then to bulk import them into the new cluster via the org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles MR Job.

## Usage
Export: 
```
hbase org.apache.hadoop.hbase.mapreduce.ExportToHFiles <HTABLE> <HFILES_PATH>
```

Bulk import: 
```
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles <HFILES_PATH> <HTABLE>
```
 
## Links ##
Cloudera Blog entry:  http://blog.cloudera.com/blog/2013/09/how-to-use-hbase-bulk-loading-and-why/
HBase official documentation: https://hbase.apache.org/book.html
