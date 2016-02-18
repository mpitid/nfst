
# Lucene Finite State Transducers

A simple wrapper around Lucene's FST implementation in the spirit of the [Rust FST command-line tool][rust-fst].

[rust-fst]: http://blog.burntsushi.net/transducers/

## Building

To build and run you need to have [Java 8][java] and [sbt] installed.

The following should build and create a script wrapper under `target/pack/bin`.

```bash
sbt pack
```

[sbt]: http://www.scala-sbt.org
[java]: http://openjdk.java.net/projects/jdk8

## Running

For a full list of options run

```
./target/pack/bin/cli --help
```
If you have multiple versions of Java installed, export `JAVA_HOME`, e.g.

```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd6
```

Alternatively, you can run directly with:

```bash
java -cp target/pack/lib/\* prototypes.fst.cli --help

```

## Examples

1.  Build an FST from a CSV file:

    ```bash
    LC_ALL=C sort input.csv -o sorted.csv
    ./target/pack/bin/cli map --values --delimiter , sorted.csv out.fst
    ```

2.  Build an FST from a file containing 1-grams to 5-grams encoded as ordinals in decimal, separated by spaces. The values will be the count of each entry:

    ```bash
    env LC_ALL=C sort -k1n -k2n -k3n -k4n -k5n input.ordinals -o sorted.ordinals
    ./target/pack/bin/cli map --format ints sorted.ordinals out.fst
    ```

3.  Lookup up the count for an ordinal in our previous FST:

    ```bash
    ./target/pack/bin/cli get --format ints out.fst 42
    42 2679
    ```
