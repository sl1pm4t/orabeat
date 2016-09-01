# Orabeat - Elastic beat for Oracle Database

Current status: **alpha**.

## Description

This is a beat for Oracle Database system and session metrics. Orabeat polls the Oracle `v$sysstat` and `v$sesstat` views for statistics.

## Prerequisites

This beat uses the [rana/ora](github.com/rana/ora) database driver, which depends on the Oracle instant client libraries.
Make sure `LD_LIBRARY_PATH` includes the location of `libclntsh.so` library.

## Exported fields

Orabeat exports the following types of JSON documents based on the statistics
that it's configured to capture:

- `type: system` for system-wide statistics
- `type: session` for per-session statistics

By default, Orabeat captures both types of statistics. As you can see in
the following examples, the content of the JSON document varies depending on the
type.

### System-Wide Statistics

Orabeat exports one JSON document for the system. For example:

```json
{
    "@timestamp":"2016-03-16T01:34:14.719Z",
    "beat":
    {
        "hostname":"hostname",
        "name":"hostname"
    },
    "count":1,
    "machine":"hostname",
    "serial#":1,
    "sid":1,
    "stat_name":"redo size",
    "username":"",
    "value":123456
    "type":"sesstat"
}    
```

### Per-Session Statistics

Orabeat exports one document per session. For example:


More about beats platform: https://www.elastic.co/products/beats

## To apply Orabeat template:

```bash
curl -XPUT 'http://localhost:9200/_template/orabeat' -d@orabeat.template.json
```

## Known Issues

- If the database restarts, Orabeat will shutdown.
- Stat filter not implemented yet.
