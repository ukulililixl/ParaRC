# SPDist

### Compile

```bash
$> ./compile.sh
```

### Configuration

* PRS Generator
    * ecs.r7.2xlarge in Alibaba Cloud
* Controller and agent
    * ecs.r7.xlarge in Alibaba Cloud
* Configuration files
    * There is an example under `./conf/sysSetting.xml`

### Generate MLP

```bash
$> ./TradeoffAnalysis [code] [n] [k] [w] [repairIdx]
```

For example, to generate a color combination for (14,12) Clay code to repair the
first block, we run `./TradeoffAnalysis Clay 14 12 256 0`

#### Encode data

* ParaRC leverages [OpenEC](https://github.com/ukulililixl/openec) to encode data in HDFS-3. Please refer to the document of OpenEC to encode data in HDFS.

#### Start ParaRC

* `python script/start.py`

#### Test degraded read

* `./DistClient degradeRead [blockname] [method]`

For example, to test parallel repair of Clay codes, we run `./DistClient degradeRead block-0 dist`

#### Test full-node recovery

* `./DistClient standbyRepair [node] [method]` 

For example, to test full-node recovery of Clay codes, we run `./DistClient standbyRepair 192.168.0.2 Clay dist`
