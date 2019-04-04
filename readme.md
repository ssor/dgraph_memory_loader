# dgraph memory loader

## Instruction

This project aims to provide a package for load RDF bytes in memory to dgraph.

Dgraph already has a tool [live](https://github.com/dgraph-io/dgraph/tree/master/dgraph/cmd/live), but this tool just load data from file.

If data already in memory, it's an unnecessarily operation to save data to file and then load. 

As such result, many codes are just copied from tool live.


## How to Use

I have an example in cmd dir. I think you should have demo data, so you will do like below:

```bash

./build.sh

./memory_loader -f [your rdf file]

``` 