### Micro-pattern benchmarks in SPARK streaming 
####Spark Streaming benchmark implementation
| Task Name  | Pattern(s) | Selectivity | Tasks |
| ------------- | ------------- |------------- |------------- |
| Identity  | Transform   |1:1|1|
| Project   | Transform   |1:1|1|
| Sequence   | Transform   |1:1|3|
|   Quality Chk  | Filter   |N:M, M|1|
|   Data Gen. | Flat Map   | 1:N |1|
|   Average | Flat Map, Aggregate   |N:1 |2|
|   Fork-Merge | Duplicate, Transform   |1:3 |4|
|   Split-Merge | Hash, Transform   |1:1|4|

![MICRO1](https://github.com/anshuiisc/FIG/blob/master/micro1.png)
![MICRO2](https://github.com/anshuiisc/FIG/blob/master/micor2.png)

### Application  benchmarks 
| App. Name  | Code |
| ------------- | ------------- |
| Pipeline for realtime forecasting and storage  | FCAST   |

<!--

| Predictive Analytics dataflow   | PRED   |


- Steps to run benchmark's
- Once cloned  run 
    ```
   mvn clean compile package -DskipTests
    ```
- To submit jar microbenchmarks- 
 ```
 storm jar <stormJarPath>   in.dream_lab.bm.stream_iot.storm.topo.micro.MicroTopologyDriver  C  <microTaskName>  <inputDataFilePath used by CustomEventGen and spout>   PLUG-<expNum>  <rate as 1x,2x>  <outputLogPath>   <tasks.properties File Path>   <TopoName>
 
 
 ```
 !-->



![FCAST](https://github.com/anshuiisc/FIG/blob/master/FCAST.png)


