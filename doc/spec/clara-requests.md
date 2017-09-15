# Definitions

| Name | Value |
| ---- | ----- |
| `<addr>`              | `<host>%<port>` |
| `<lang>`              | `java` or `cpp` or `python` |
| `<dpe_name>`          | `<addr>_<lang>` |
| `<container_name>`    | `<addr>_<lang>:<container>` |
| `<service_name>`      | `<addr>_<lang>:<container>:<engine>` |
| `<service_status>`    | `error` or `warning` or `info` |


# Topology Requests

### Deploy container

| Field | Value |
| ----- | ----- |
| **parameters**    | `<container_name>` |
| **actor**         | DPE |
| **proxy**         | `<addr>` |
| **topic**         | `dpe:<dpe_name>` |
| **data**          | `startContainer?<container>` |

### Deploy service

| Field | Value |
| ----- | ----- |
| **parameters**    | `<service_name>`, `<class_path>`, `<pool_size>`, `<description>` |
| **actor**         | DPE |
| **proxy**         | `<addr>` |
| **topic**         | `container:<container_name>` |
| **data**          | `startService?<engine>?<classs_path>?<pool_size>?<description>` |

__Notes__: `<pool_size>` and `<description>` are optional.

### Exit DPE

| Field | Value |
| ----- | ----- |
| **parameters**    | `<dpe_name>` |
| **actor**         | DPE |
| **proxy**         | `<addr>` |
| **topic**         | `dpe:<dpe_name>` |
| **data**          | `stopDpe` |

### Exit container

| Field | Value |
| ----- | ----- |
| **parameters**    | `<container_name>` |
| **actor**         | DPE |
| **proxy**         | `<addr>` |
| **topic**         | `dpe:<dpe_name>` |
| **data**          | `stopContainer?<container>` |

### Exit service

| Field | Value |
| ----- | ----- |
| **parameters**    | `<service_name>` |
| **actor**         | DPE |
| **proxy**         | `<addr>` |
| **topic**         | `dpe:<dpe_name>` |
| **data**          | `stopService?<engine>` |


## Service Requests

### Configure Service

| Field | Value |
| ----- | ----- |
| **parameters**    | `<service_name>`, `<engine_data>` |
| **actor**         | Service |
| **proxy**         | `<addr>` |
| **topic**         | `<service_name>` |
| **composition**   | `<service_name>;` |
| **action**        | `CONFIGURE` |
| **data**          | `<data>` |

### Execute Service

| Field | Value |
| ----- | ----- |
| **parameters**    | `<service_name>`, `<engine_data>`, `<request_id>` |
| **actor**         | Service |
| **proxy**         | `<addr>` |
| **topic**         | `<service_name>` |
| **composition**   | `<service_name>;` |
| **action**        | `EXECUTE` |
| **request ID**    | `<request_id>` |
| **data**          | `<data>` |

### Execute Composition

| Field | Value |
| ----- | ----- |
| **parameters**    | `<composition>`, `<engine_data>`, `<request_id>` |
| **actor**         | Service |
| **proxy**         | `<addr>` of every service that start the composition |
| **topic**         | `<service_name>` of every service that start the composition |
| **composition**   | `<composition>` |
| **action**        | `EXECUTE` |
| **request ID**    | `<request_id>` |
| **data**          | `<data>` |

### Start data reporting

| Field | Value |
| ----- | ----- |
| **parameters**    | `<service_name>`, `<event_count>` |
| **actor**         | Service |
| **proxy**         | `<addr>` |
| **topic**         | `<service_name>` |
| **data**          | `serviceReportData?<event_count>` |

### Start `done` reporting

| Field | Value |
| ----- | ----- |
| **parameters**    | `<service_name>`, `<event_count>` |
| **actor**         | Service |
| **proxy**         | `<addr>` |
| **topic**         | `<service_name>` |
| **data**          | `serviceReportDone?<event_count>` |

### Stop data reporting

| Field | Value |
| ----- | ----- |
| **parameters**    | `<service_name>` |
| **actor**         | Service |
| **proxy**         | `<addr>` |
| **topic**         | `<service_name>` |
| **data**          | `serviceReportData?0` |

### Stop `done` reporting

| Field | Value |
| ----- | ----- |
| **parameters**    | `<service_name>` |
| **actor**         | Service |
| **proxy**         | `<addr>` |
| **topic**         | `<service_name>` |
| **data**          | `serviceReportDone?0` |


## Subscriptions

### Subscribe DPE alive messages

| Field | Value |
| ----- | ----- |
| **parameters**    | `<author>` |
| **actor**         | Front End |
| **proxy**         | `<addr>` |
| **subscription**  | `dpeAlive:` or `dpeAlive:<author>:` |

__Notes__: `<author>` is optional.

### Subscribe service status reports

| Field | Value |
| ----- | ----- |
| **parameters**    | `<service_name>`, `<service_status>` |
| **actor**         | Front End |
| **proxy**         | `<addr>` |
| **subscription**  | `<service_status>:<service_name>` |

### Subscribe service data reports

| Field | Value |
| ----- | ----- |
| **parameters**    | `<service_name>` |
| **actor**         | Front End |
| **proxy**         | `<addr>` |
| **subscription**  | `data:<service_name>` |

### Subscribe service `done` reports

| Field | Value |
| ----- | ----- |
| **parameters**    | `<service_name>` |
| **actor**         | Front End |
| **proxy**         | `<addr>` |
| **subscription**  | `done:<service_name>` |


# Reports

## Registration report

```json
{
  "DPERegistration": {
    "name": "10.1.1.10_java",
    "language": "java",
    "start_time": "2015-06-20 12:30:00",
    "clara_home": "/home/user/clara",
    "n_cores": 8,
    "memory_size": 1908932608,
    "containers": [
      {
        "ContainerRegistration": {
          "name": "10.1.1.10_java:trevor",
          "language": "java",
          "start_time": "2015-06-20 12:30:30",
          "author": "Trevor",
          "services": [
            {
              "ServiceRegistration": {
                "class_name": "org.jlab.coda.clara.examples.Engine1",
                "engine_name": "Engine1",
                "language": "java",
                "pool_size": 4,
                "start_time": "2015-06-20 12:30:50",
                "author": "Trevor",
                "version": "1.0",
                "description": "Some long description of what it does."
              }
            },
            {
              "ServiceRegistration": {
                "class_name": "org.jlab.coda.clara.examples.Engine2",
                "engine_name": "Engine2",
                "language": "java",
                "pool_size": 4,
                "start_time": "2015-06-20 12:40:00",
                "author": "Trevor",
                "version": "1.0",
                "description": "Some description of what it does."
              }
            }
          ]
        }
      }
    ]
  }
}
```

## Runtime report

```json
{
  "DPERuntime": {
    "name": "10.1.1.10_java",
    "snapshot_time": "2015-06-20 12:35:00",
    "cpu_usage" : 45.2,
    "memory_usage" : 631222786,
    "load" : 2.3879,
    "containers" : [
      {
        "ContainerRuntime" : {
          "name": "10.1.1.10_java:trevor",
          "snapshot_time": "2015-06-20 12:35:00",
          "n_requests" : 1500,
          "services" : [
            {
              "ServiceRuntime" : {
                "name": "10.1.1.10_java:trevor:Engine1",
                "snapshot_time": "2015-06-20 12:35:00",
                "n_requests" : 1000,
                "n_failures" : 0,
                "shm_reads" : 1000,
                "shm_writes" : 1000,
                "bytes_recv" : 0,
                "bytes_sent" : 0,
                "exec_time" : 243235243543
              }
            },
            {
              "ServiceRuntime" : {
                "name": "10.1.1.10_java:trevor:Engine2",
                "snapshot_time": "2015-06-20 12:35:00",
                "n_requests" : 500,
                "n_failures" : 10,
                "shm_reads" :  500,
                "shm_writes" : 490,
                "bytes_recv" : 0,
                "bytes_sent" : 0,
                "exec_time" : 129735841127
              }
            }
          ]
        }
      }
    ]
  }
}
```
