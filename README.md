# poemark

Poe Mark provides alerts when new items enter the market in the game Path of Exile. Users can subscribe to update feeds of particular types of items (ie. Windripper Imperial Bow) and receive a feed of updates as these items are put on the market at a low price. The low price is determined by computing the historic mean and standard deviation of the item's price.

Commit History:
See other repositories for commit history.
For module getdata see: JackProject | https://github.com/axl002/JackProject
For module pipeline see: kafka-flink101 | https://github.com/axl002/kafka-flink-101 (thanks to tgrall)

#getdata

JackKafka: This reads from the official Path of Exile market api, parses the json and writes to a kafka topic. It makes use of the jackson json processing library.

JackLocalWrite: This is identical to JackKafka but writes results to disk.

ReadAndInsert: This reads processed items from a kafka topic that flink has written to and writes the item information to rethinkDB.

InsertLookUp: This reads item mean and standard deviation values from a kafka topic that flink has written to and writes the values to rethinkDB.

#pipeline

PipeLine: This reads information from a kafka topic containing json of the items for sale (written to by JackKafka). It filters the stream for duplicates, computes the mean and standard deviation of the items, and writes the items and price metrics to two distinct kafka topics which ReadAndInsert and InsertLookUp handle.
