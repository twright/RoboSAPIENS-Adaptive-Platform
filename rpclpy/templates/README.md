## Event Keys Reference

| Event Key | Published By | Consumed By | Meaning |
|---|---|---|---|
| `sensor_data_received` | Managed System / Sensor | Monitor | New sensor reading arrived |
| `observation_recorded` | Monitor | Analysis | Observation stored, ready for anomaly check |
| `anomaly_detected` | Analysis | Plan | Detection window exceeded threshold |
| `plan_generated` | Plan | Legitimate | New plan/model is ready for validation |
| `plan_rejected` | Legitimate | Plan | Validation failed, re-planning required |
| `plan_validated` | Legitimate | Execute | Plan passed validation, safe to deploy |
| `plan_executed` | Execute | Managed System | Plan deployed, monitoring resumed |

