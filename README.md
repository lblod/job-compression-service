# JOB COMPRESSION SERVICE

Service responsible for compressing (gzip) files within an harvesting job.

## How to

- Add to your docker-compose.yml file:
```yaml
 harvest_singleton-job:
    image: lblod/harvesting-singleton-job-service:1.0.0
    labels:
      - "logging=true"
    restart: always
    logging: *default-logging
```
- Add to your config/delta/rules.js:
```js
{
    match: {
      predicate: {
        type: 'uri',
        value: 'http://www.w3.org/ns/adms#status',
      },
      object: {
        type: 'uri',
        value: 'http://redpencil.data.gift/id/concept/JobStatus/scheduled',
      },
    },
    callback: {
      method: 'POST',
      url: 'http://harvest_compression/delta',
    },
    options: {
      resourceFormat: 'v0.0.1',
      gracePeriod: 1000,
      ignoreFromSelf: true,
      optOutMuScopeIds: ['http://redpencil.data.gift/id/concept/muScope/deltas/initialSync'],
    }
  }
```
- Add to your job controller config:
```json
  {
        "currentOperation": "http://lblod.data.gift/id/jobs/concept/TaskOperation/checking-urls",
        "nextOperation": "http://lblod.data.gift/id/jobs/concept/TaskOperation/compressFiles",
        "nextIndex": "9"
  }
```
