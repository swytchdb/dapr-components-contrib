# Swytch lock component

Implements Dapr's `TryLock` and `Unlock` using transactional scalar writes and
engine expiration metadata. Acquisition requires a positive `expiryInSeconds`
and fails when a live lock exists, including for the same owner. Unlock checks
the owner before removing the lock.

State and lock components with matching normalized Swytch configurations share
one runtime within a Dapr process. The runtime stops after its last component
closes. Clustered components with different configurations need distinct local
cluster ports. Dapr supplies the lock key prefix.

Local mode is process-local and in-memory. Cross-process locks require a shared
Swytch cluster configuration. Lock retention inherits the engine's storage and
eviction behavior; a memory target that evicts the sole copy of a lock can lose
it before expiration. Do not treat local in-memory validation as proof of
distributed mutual exclusion under partitions, eviction, or node failure.
