# Amazon Kinesis Hot Shard Advisor (khs)
Easily identify hot shard and hot key on your Kinesis data streams.

## About
The Amazon Kinesis Hot Shard Advisor is a CLI tool that simplifies identifying whether you have hot key or hot shard issues on your Kinesis data streams. The tool can also identify whether you are hitting the shard level throughput limit per-second basis.

## Why KHS?
An Amazon Kinesis Data Stream consists of one or more shards. Records with the same partition key is always written to the same shard. Each shard has a [maximum throughput](https://aws.amazon.com/kinesis/data-streams/faqs/#:~:text=A%20shard%20has%20a%20sequence,2%20MB%2Fsecond%20for%20reads.) it can support. When a producer application attempts to put records at a higher rate than the maximum throughput available, they receive `WriteThroughputExceeded` error. This is commonly known as the **hot shard problem**.   

When you experience a hot shard problem, you will notice an increase of `WriteProvisionedThroughputExceeded` metric in CloudWatch. However, for bursty workloads you may not notice the corresponding data in `IncomingRecords` and `IncomingBytes` metrics due to per minute aggregation. You can get the visibility to bursty writes with khs because it reads the records from your stream and aggregate the record size per second based on `ApproximateArrivalTimestamp`.

Typically when there's a hot shard problem, you can increase the number of shards in the stream to increase available throughput (or change the capacity mode of your stream to on-demand). Increasing the number of shards does not solve the problem if it's caused by the producer using a key (or set of keys) more frequently than others. This is known as the **hot key problem** and requires an adjutment to parition key scheme to solve it. khs helps you identify hot key issues by showing the frequency of keys that are written to your stream within a given time period.

## Prerequisite
In order to perform analysis on your Kinesis Data Stream, khs must be executed under the context of an IAM entity with access to the following actions.

```
kinesis:ListShards
kinesis:DescribeStreamSummary
kinesis:RegisterStreamConsumer
kinesis:DeregisterStreamConsumer
kinesis:SubscribeToShard
kinesis:DescribeStreamConsumer
```

** khs will use the AWSCli configuration to access Kinesis data streams on a particular AWS region. If the default region is not configured in the current session, you must also specify the region of the Kinesis Data Stream via [AWS_DEFAULT_REGION](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-region) environment variable.

## Getting Started
1. download the compatible binary from [releases](https://github.com/awslabs/kinesis-hot-shard-advisor/releases) and **rename** the file name to **khs**
2. Use the command line and navigate to the directory where you downloaded the binary earlier
3. run the below command
```
khs -stream=[YOUR STREAM NAME]] -from="yyyy-mm-dd hh:mm" -to="yyyy-mm-dd hh:mm"
Example:  khs -stream=lab3 -from="2022-02-24 10:07" -to="2022-02-24 10:09" 
Note: The date range should be within the retention period of your Kinesis data streams.
```
## Output sample
```
Listing shards for stream lab3...OK!
 10 / 10 [====================================================] 100.00% 2m52s
output is written to out.html
```


## View the Report
Once you see the output as above, open the file pointed by `out` option (by default this is set to **out.html** file from your current directory) with your default browser to view the report.

## Usage
```
  Usage of khs:
  -cms
    	Use count-min-sketch (Optional) algorithm for counting key distribution (Optional). Default is false. Use this method to avoid OOM condition when analysing busy streams with high cardinality.
  -from string
    	Start time in yyyy-mm-dd hh:mm format (Optional). Default value is current time - 5 minutes.
  -limit int
    	Number of keys to output in key distribution graph (Optional). (default 10)
  -out string
    	Path to output file (Optional). Default is out.html. (default "out.html")
  -shard-ids string
    	Comma separated list of shard ids to analyse.
  -stream string
    	Stream name
  -to string
    	End time in yyyy-mm-dd hh:mm format (Optional). Default value is current time.
  -top int
    	Number of shards to emit to the report(Optional). Use 0 to emit all shards. Emitting all shards can result in a large file that may take a lot of system resources to view in the browser. (default 10)
```

## Sample Report
![Sample Report](images/samplereport.png)

## Contributing to the project
If you are interested in contributing to this project, [here](CONTRIBUTING.md) are the steps you can follow.
