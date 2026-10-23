<script lang="ts">
  export let value: string;
</script>

<span
  class="state"
  class:warning={[
    "incomplete",
    "failed",
    "failing",
    "unavailable",
    "throttled",
    "stopped_incomplete",
    "retry_waiting",
    "unknown",
    "canceled",
  ].includes(value)}
  class:good={[
    "applied",
    "complete",
    "connected",
    "available",
    "enabled",
  ].includes(value)}>{value.replaceAll("_", " ")}</span
>
