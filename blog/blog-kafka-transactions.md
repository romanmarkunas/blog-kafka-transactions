# Messaging system latencies, part 1: Apache Kafka

Most developers I talk to about Kafka agree on a catchphrase "Kafka is designed 
for throughput". That's fair and you can find plenty of benchmarks that show
700k/s throughput for single producer without replication. But does that mean
we should discard Kafka when talking about low latency messaging? 

After quick googling I found [this outdated SO question](https://stackoverflow.com/questions/20520492/how-to-minimize-the-latency-involved-in-kafka-messaging-framework).
That question is very old and includes very old version of Kafka (0.7.2 whereas
current version is 2.0.0) and also [this article](https://engineering.linkedin.com/kafka/benchmarking-apache-kafka-2-million-writes-second-three-cheap-machines) 
states that very decent latencies (2-3ms 99 percentile) are achievable. Having 
such controversial info is not enough to make final decision so I decided to 
create a little benchmark myself, to finally conclude whether Kafka is good for 
low-latency applications.


## What is measured

For all tests I run producer, consumer and broker on the same machine. For these 
tests I used my work laptop: i7-7820HQ, 16GB, Windows 10 (yes, I know...). This 
is why pre-allocation is on in broker settings and you will need to disable it
should you run provided code on Linux box. Also I didn't change default Kafka
startup scripts, which means brokers had 1GB heap maximum.   

Latency test intent is to test following scenarios:
1. light throughput of 200 messages/second + non-durable broker
1. light throughput of 200 messages/second + fault-tolerant broker
1. moderate throughput of 4000 messages/second + non-durable broker
1. moderate throughput of 4000 messages/second + fault tolerant broker

This test do not measure latency drops due to cluster node failovers as 
these scenarios are very different depending on your partitioning schema and 
replication factor. Hopefully node failures are not part of your normal 
day-to-day operations ;)

As usual, all code can be found [here](https://github.com/romanmarkunas/blog-kafka-artemis-latency), 
if you want to play around and see how your setup compares. All scenarios are 
located in test directory under benchmark/LatencyBenchmark.

For exact broker/client configuration see code above. Also I'll put a little 
explanation why these settings were used.


## Results

#### Lowest latencies possible

Measurements @ 200 messages/s:
Total sent     - 5000
Total received - 5000
Send rate      - 200.004
99 percentile  - 1.576891
75 percentile  - 1.180825
Min latency    - 0.593806
Max latency    - 8.921906
Avg latency    - 1.085105

Measurements @ 4000 messages/s:
Total sent     - 50000
Total received - 50000
Send rate      - 3837.725
99 percentile  - 1.327242
75 percentile  - 0.945013
Min latency    - 0.413972
Max latency    - 13.287300
Avg latency    - 0.802173

So these are lowest latencies possible on my machine. Configuration for these:
1. No batching on producer (consumer always fetches all available messages up 
to configured size)
1. No acks on producer
1. No commits after each message (but uses asynchronous background commits) on 
consumer
1. Single node cluster, no replication
1. Broker has delayed sync to disk
1. All internal topics are single partition

As you can see most messages make a roundtrip within 1-2 ms. There are always 
outliers at rare occasion, which are caused by different maintenance operations
and probably some interference from OS. For example, expired offset removal took
around 7 ms.

Also it's possible to decrease consumer poll timeout to get smaller max latency
at cost of higher 99 percentile. 

#### Non fault-tolerant setup with consumer commit after each read

Measurements @ 200 messages/s and synchronous commit:
Total sent     - 5000
Total received - 5000
Send rate      - 200.004
99 percentile  - 4.535680
75 percentile  - 1.927668
Min latency    - 0.904814
Max latency    - 9.848230
Avg latency    - 1.865007

I made these measurements as I was curious how synchronous commit affects 
latency, and it's roughly 30% overhead.

#### Fault-tolerant setup with synchronous consumer commit

Measurements @ 200 messages/s and synchronous commit:
Total sent     - 5000
Total received - 5000
Send rate      - 199.989
99 percentile  - 5.137226
75 percentile  - 3.924183
Min latency    - 2.115347
Max latency    - 14.580346
Avg latency    - 3.613249

Measurements @ 4000 messages/s and synchronous commit:
Total sent     - 50000
Total received - 50000
Send rate      - 3993.356
99 percentile  - 17.898772
75 percentile  - 11.823148
Min latency    - 5.039950
Max latency    - 35.926347
Avg latency    - 11.018564

Fault tolerant config:
1. Broker made out of 3 nodes
1. Producer waits for ack from each broker node
1. Sync to disk is still delayed

For the higher throughput scenario I had to turn batching on and put a setting
to batch at most 500 records. Otherwise maximum throughput would be 1000 /
MIN_LATENCY = 1000 / 5 = 200 messages/s


## Conclusion

Looks like these articles about huge latencies of 100ms magnitude relate to zgit 
older versions of Kafka. Replicated operation showed pretty decent results,
which should be enough for most cases. Please note that these results are not
made on dedicated hardware and I didn't do any memory/OS settings/affinity
tuning, so very likely it's possible to get even better results.  

There are no measurements for durable messages (force flush each message on 
disk) and I think tests for such setup are extremely hardware dependent and 
should be made using proper HDD/SSD.

My takeaway from this is that unless you need sub-millisecond latencies I 
could do with Kafka. Unless you are in that outlier part of spectrum 
technologies/solution selection must be done based on functionality it provides
and not on specific performance parameters. And of course reusing well-known 
technology is much better than having a zoo of trendy names, that barely anyone
in team understands properly.


## P.S.

Coming soon! Follow-up post where same tests are performed with Apache Artemis
(former ActiveMQ).
