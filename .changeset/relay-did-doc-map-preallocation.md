---
'hypercerts-relay': patch
---

DID-document parsing no longer sizes internal maps from the document's declared array lengths. A `did:web` document of a few megabytes could otherwise make the Relay eagerly allocate ~100 MB of empty map capacity per lookup and keep it cached for 24 hours. No operator action is required.
