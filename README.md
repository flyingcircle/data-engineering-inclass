
## Part B Questions

*Find the Kafka monitoring console for your topic. Briefly describe its contents. Do the measured values seem reasonable to you?*

It says that for both the producer and consumer they run at a rate of 5.79 KB/s. This makes sense to me. For each test, I send ~40KB of data. So this would be about 6-7 seconds worth of throughput.


## Part C Questions
```bash
jeremy@instance-2:~/examples/clients/cloud/python$ wc ~/bcsample.json 
     0  28000 339596 /home/jeremy/bcsample.json
```
*What happens if you run your consumer multiple times while only running the producer once?*

The consumer simply polls and waits for a message to consume since there are none left in the topic.

*Before the consumer runs, where might the data go, where might it be stored?*

It's being stored in Kafka's cloud in some kind of temporary storage. I believe the data is likely being stored in an Apache Kafka SQL DB.

*Is there a way to determine how much data Kafka/Confluent is storing for your topic? Do the Confluent monitoring tools help with this?*

The monitoring tools are confusing on this. There is a graph on the cluster page showing data storage usage for the whole cluster, but I don't see easy way to see the messages currently waiting for a topic.

## Part D Description

The producers created a total of 2000 records combined. After I ran the consumer, it consumed all 2000 records that were currently in the topic.

## Part E

There is now producing and consuming happening at the same time. In the topic dashboard I can see activity on both the production side and the consumption side. Kafka has seemed to settled at both the producer and consumers handling the data at about 3.26 KB/Sec.

## Part F Questions

*Can you create a consumer that only consumes specific keys? If you run this consumer multiple times with varying keys then does it allow you to consume messages out of order while maintaining order within each key?*

As far as I can tell, I do not see any way to grab messages by a particular key without consuming them. I've looked through the API docs, but haven't seen anything in the consumer that looks like it has this feature.


## Part G Questions

*What does Producer.flush() do?*

The docs state: "Invoking this method makes all buffered records immediately available to send"

*What happens if you do not call producer.flush()?*

If you don't flush, then the messages are ultimately never sent to Kafka

*What happens if you call producer.flush() after sending each record?*

The records are sent, but it takes a lot longer to do. I imagine that this is due to performing a network call for each record rather 1 larger network call.

*What happens if you wait for 2 seconds after every 5th record send, and you call flush only after every 15 record sends, and you have a consumer running concurrently?  Specifically, does the consumer receive each message immediately? only after a flush? Something else?*

The consumer only receives messages after the producer flushes.

## Part H Description



## Part I Description