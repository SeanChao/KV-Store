# Project 1: KVStore using Log-structured Merge Tree

The handout files include two main parts:

- The `KVStoreAPI` class in `kvstore_api.h` that specifies the interface of KVStore.
- Test files including correctness test (`correctness.cc`) and persistence test (`persistence.cc`).

Explanation of each handout file:

```text
.
├── Makefile  // Makefile if you use GNU Make
├── README.md // This readme file
├── correctness.cc // Correctness test, you should not modify this file
├── data      // Data directory used in our test
├── kvstore.cc     // your implementation
├── kvstore.h      // your implementation
├── kvstore_api.h  // KVStoreAPI, you should not modify this file
├── persistence.cc // Persistence test, you should not modify this file
└── test.h         // Base class for testing, you should not modify this file
```

First have a look at the `kvstore_api.h` file to check functions you need to implement. Then modify the `kvstore.cc` and `kvstore.h` files and feel free to add new class files.

We will use all files with `.cc`, `.cpp`, `.cxx` suffixes to build correctness and persistence tests. Thus, you can use any IDE to finish this project as long as you ensure that all C++ source files are submitted.

For the test files, of course you could modify it to debug your programs. But remember to change it back when you are testing.

Good luck :)

## Compile

The program should be compiled with `g++` >= 9 with built-in `filesystem` support.

## SS Table

### Naming Convention and Hierarchy

The SS Tables are stored under given directory. With hierarchy and naming style given below.

```tree
.
├── level-0
│   |── sstable-0
│   └── sstable-1
└── level-1
    |── sstable-0
    |── sstable-1
    |── sstable-2
    └── sstable-3
...
```

### Data Structure

The SS Table, as a binary file, is composed of three parts: The data segment(including only an array of data entries), the index table part and meta data part.  
The meta data consists of a `uint64_t` offest indicating the index of offset table.

```text
Data Entry
+----------------+
|key|length|value|
+----------------+
length: the length of string value (8 bytes)

Data Segment:
+-----------------------------+
|Data Entry 1|...|Data Entry n|
+-----------------------------+

Index table:
+-----------------------------+
|key1|offset1|key2|offest2|...|
+-----------------------------+

Meta data:
+---------------------+
|offset of index table|
+---------------------+
```
