# kadabra

A library that adds transaction management, rate limiting, and retries around a 
Hikari connection pool.
It also features convenience methods for, generating SQL, working with prepared statements, and 
marshaling objects.

## Design Goals

1. No annotations.
2. Maximum simplicity at call site.
