


### 1. set winutils and hadoop path


```
 HADOOP_HOME
 Path
 F:\software\hadoop-2.8.1
 https://github.com/steveloughran/winutils
```


```shell
docker run -d --name hbase-standalone -h hbase-standalone -p 16010:16010 -p 16020:16020 -p 16030:16030 -p 2181:2181 mckdev/hbase
```


```shell

hbase shell
   
create 't_example', {NAME => 'cf_info', VERSIONS => 3}, {NAME => 'cf_details', TTL => 259200}

list

describe 't_example'

```

### Manager
> https://github.com/MeetYouDevs/hbase-manager
