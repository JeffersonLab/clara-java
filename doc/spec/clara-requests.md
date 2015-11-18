# Definitions

| Name | Value |
| ---- | ----- |
| `<addr>`              | `<host>%<port>` |
| `<lang>`              | `java` or `cpp` or `python` |
| `<dpe_name>`          | `<addr>_<lang>` |
| `<container_name>`    | `<addr>_<lang>:<container>` |
| `<service_name>`      | `<addr>_<lang>:<container>:<service>` |
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
| **actor**         | Container |
| **proxy**         | `<addr>` |
| **topic**         | `container:<container_name>` |
| **data**          | `startService?<engine>?<classs_path>?<pool_size>?<description>` |

__Notes__: `pool_size` and `<description>` are optional.

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
| **actor**         | Container |
| **proxy**         | `<addr>` |
| **topic**         | `container:<container_name>` |
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
| **topic**         | `<service name>` of every service that start the composition |
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
| **topic**         | `<service name>` |
| **data**          | `serviceReportData?<event_count>` |

### Start `done` reporting

| Field | Value |
| ----- | ----- |
| **parameters**    | `<service_name>`, `<event_count>` |
| **actor**         | Service |
| **proxy**         | `<addr>` |
| **topic**         | `<service name>` |
| **data**          | `serviceReportDone?<event_count>` |

### Stop data reporting

| Field | Value |
| ----- | ----- |
| **parameters**    | `<service_name>` |
| **actor**         | Service |
| **proxy**         | `<addr>` |
| **topic**         | `<service name>` |
| **data**          | `serviceReportData?0` |

### Stop `done` reporting

| Field | Value |
| ----- | ----- |
| **parameters**    | `<service_name>` |
| **actor**         | Service |
| **proxy**         | `<addr>` |
| **topic**         | `<service name>` |
| **data**          | `serviceReportDone?0` |


## Subscriptions

### Subscribe DPE alive messages

| Field | Value |
| ----- | ----- |
| **parameters**    | None |
| **actor**         | Front End |
| **proxy**         | `<addr>` |
| **subscription**  | `<service_status>:<service name>` |

### Subscribe service status reports

| Field | Value |
| ----- | ----- |
| **parameters**    | `<service_name>`, `<service_status>` |
| **actor**         | Service |
| **proxy**         | `<addr>` |
| **subscription**  | `<service_status>:<service name>` |

### Subscribe service data reports

| Field | Value |
| ----- | ----- |
| **parameters**    | `<service_name>` |
| **actor**         | Service |
| **proxy**         | `<addr>` |
| **subscription**  | `data:<service name>` |

### Subscribe service `done` reports

| Field | Value |
| ----- | ----- |
| **parameters**    | `<service_name>` |
| **actor**         | Service |
| **proxy**         | `<addr>` |
| **subscription**  | `done:<service name>` |
