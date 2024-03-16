# files109
A files system demonstration for linux.

This files system is aimed at supporting unlimited files.
The number of files here is dependent on the size of the files system.

## Brief Description

The files system is divided into two parts: indexes and data part

### Indexes Part

The indexes part looks like this for example:
```
===BEGIN===
data_key: james.txt
data_begin: 10
data_end: 109

data_key: jame1.txt
data_begin: 110
data_end: 137

data_key: jame2.txt
data_begin: 138
data_end: 142

===END===
```

The Indexes part is written to the end of the partition.

It is used to get the file contents from the data part.


### Data Part

The data part has no file structure. It is written at the beginning
of the partition
