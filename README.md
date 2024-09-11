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
james.txt
10
109

jame1.txt
110
137

jame2.txt
138
142

===END===
```

The Indexes part is written to the end of the partition.

It is used to get the file contents from the data part.


### Data Part

The data part has no file structure. It is written at the beginning
of the partition
