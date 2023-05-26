# warp10-plugin-kafka

The Warp 10 Kafka Plugin allows your Warp 10 instance to consume messages from a Kafka message broker and process those messages using WarpScript.

# Installation

## Using WarpFleet

```
wf g -d CONFDIR -l LIBDIR io.warp10 warp10-plugin-kafka
```

Where `CONFDIR` is the configuration directory of your Warp 10 instance (or use `-c CONFFILE` with a release prior to 2.1), and `LIBDIR` the lib directory where the downloaded jar file should be copied.

## From source

```
git clone https://github.com/senx/warp10-plugin-kafka.git
cd warp10-plugin-kafka
./gradlew -Duberjar shadowJar
```

Then copy the jar (suffixed with `uberjar`) from the `build/libs` directory into the `lib` directory of your Warp 10 deployment.

# Configuration

The Kafka plugin will scan the directory specified in the `kafka.dir` configuration key at the period configured in `kafka.period` (expressed in milliseconds, default to one minute), reloading consumer specifications from `.mc2` files found in the directory.

# Usage

The plugin will interact with Kafka according to specification files. A specification file is a valid WarpScript (`.mc2`) file producing a map with the following fields:

```
{
  'topics' [ 'topic1' 'topic2' ] // List of Kafka topics to subscribe to
  'parallelism' 1                // Number of threads to start for processing the incoming messages. Each thread will handle a certain number of partitions.
  'config' { }                   // Map of Kafka consumer parameters
  'macro' <% %>                  // Macro to execute for each incoming message
  'timeout' 10000                // Polling timeout (in ms), if no message is received within this delay, the macro will be called with an empty map as input
}
```

Once loaded, this specification will trigger the consumption of the specified Kafka topics. You should set the `groupid` in the `config` map. For each received message, the macro specified in `macro` will be called, preserving the execution environment (stack) between calls. The message is pushed onto the stack prior to calling the macro. It is pushed as a map with the following fields:

```
{
  'timestamp' 123        // The record timestamp
  'timestampType' 'type' // The type of timestamp, can be one of 'NoTimestampType', 'CreateTime', 'LogAppendTime'
  'topic' 'topic_name'   // Name of the topic which received the message
  'offset' 123           // Offset of the message in 'topic'
  'partition' 123        // Id of the partition which received the message
  'key' ...              // Byte array of the message key
  'value' ...            // Byte array of the message value
  'headers' { }          // Map of message headers
}
```

When the `timeout` has expired, instead of a message map like above, the macro is called with an empty map.

The configured macro can perform any WarpScript operation. If you wish to store some data, you can use the [`UPDATE`](https://warp10.io/doc/UPDATE) function.
