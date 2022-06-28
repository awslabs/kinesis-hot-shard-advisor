# Amazon Kinesis Hot Shard Advisor (khs)
Easily identify hot shard and hot key on your Kinesis data streams.

## About
The Amazon Kinesis Hot Shard Advisor is a CLI tool that simplifies identifying whether you have hot key or hot shard issues on your Kinesis data streams. The tool can also identify whether you are hitting the shard level throughput limit per-second basis.

## Prerequisite
In order to perform analysis on your Kinesis Data Stream, khs must be executing under the context of an IAM enitity with access to following actions.

```
kinesis:ListShards
kinesis:DescribeStreamSummary
kinesis:RegisterStreamConsumer
kinesis:DeregisterStreamConsumer
kinesis:SubscribeToShard
kinesis:DescribeStreamConsumer
```

** khs will use the AWSCli configuration to access Kinesis data streams on a particular AWS region. If the default region is not configured in the current session, you must also specify the region of Kinesis Data Stream via [AWS_DEFAULT_REGION](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-region) environment variable.

## Gettng Started
1. download the compatible binary from [releases](https://github.com/awslabs/kinesis-hot-shard-advisor/releases)
2. Use command line and navigate to the directory where you downloaded the binary earlier
3. run the below command
```
khs -stream=[YOUR STREAM NAME]] -from="yyyy-mm-dd hh:mm" -to="yyyy-mm-dd hh:mm"
Example:  khs -stream=lab3 -from="2022-02-24 10:07" -to="2022-02-24 10:09"* 
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
  -stream string
    	Stream name
  -from string
    	Start time in yyyy-mm-dd hh:mm format (Optional). Default value is current time - 5 minutes.
  -to string
    	End time in yyyy-mm-dd hh:mm format (Optional). Default value is current time.
  -out string
    	Path to output file (Optional). Default is out.html. (default "out.html")
  -cms
    	Use count-min-sketch (Optional) algorithm for counting key distribution (Optional). Default is false. Use this method to avoid OOM condition when analysing busy streams with high cardinality.
  -limit int
    	Number of keys to output in key distribution graph (Optional). Default is 10. (default 10)
```

## Sample Report
![Sample Report](images/samplereport.png)

## Contributing to the project
If you are interested in contributing to this project, [here](CONTRIBUTING.md) are the steps you can follow.
