# flume-ftp-sink
flume-ftp-sink
## config
Property Name | Default | Description                             |
| :-----------: | :------ | :-------------------------------------- |
|      type     | -       | need to be `com.jzy.flume.FlumeFtpSink` |
|      host     | -       | host                                    |
|      port     | 21      | port                                    |
|      user     | -       | user                                    |
|    password   | -       | password                                |
|   ftpDirPath  | /       | ftp upload dir                          |
|     prefix    | DATA-   | file prefix                             |
|     suffix    | .dat    | file prefix                             |
|   tempSuffix  | .tmp    | temp suffix when uploading              |
|   batchSize   | 1000    | max records of a file|
|   rollInterval | 30 | Number of seconds to wait before rolling current file (0 = never roll based on time interval) |
