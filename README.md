# state-m
---
<h5>
  The library implements convenient state distribution and management mechanisms, facilitating collaborative work between components.
</h5>

```toml
[dependencies]
state-m = "0.1.0"
```

## Features
* **Separation of read-write**, means that initiators and responders of state changes hold different data structures, making state maintenance more convenient.
* **Timing control**, supports waiting for all responders to complete their responses
