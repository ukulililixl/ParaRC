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
$> ./GenMLP [code] [n] [k] [w] [repairIdx]
```

For example, to generate a color combination for (14,12) Clay code to repair the
first block, we run 

`./GenMLP Clay 14 12 256 0`

#### Encode data

* ParaRC leverages [OpenEC](https://github.com/ukulililixl/openec) to encode data in HDFS-3. Please refer to the document of OpenEC to deploy HDFS-3 and OpenEC.
* Please run the following script to encode data in HDFS-3 by (14,10) Clay code with w=256.
    * `python script/gendata.py 20 Clay 14 10 256`
    * This script will encode 20 stripes with (14,10) Clay code with w=256.

#### Start ParaRC

* `python script/start.py`

#### Test degraded read

* `./DistClient degradeRead [blockname] [method]`

For example, to test parallel repair of Clay codes, we run `./DistClient degradeRead block-0 dist`

#### Test full-node recovery

* `./DistClient nodeRepair [node] [method]` 

For example, to test full-node recovery of Clay codes, we run `./DistClient nodeRepair 192.168.0.2 Clay dist`
