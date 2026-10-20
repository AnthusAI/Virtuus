# Changelog

## 0.2.0

Breaking: `ObjectStore` has two new required methods, so external implementors must add them.

- Added `ObjectStore::list_control(scope, prefix, cursor, limit)`, a paged, lexically ordered listing of control objects, and `ObjectStore::delete_control(scope, path, expected_revision)`, a conditional control delete. Implemented for `FileStore` and `S3Store` (same `_virtuus/<tenant>/<project>/` layout as `put_control`).
- Added `JournaledStore::purge_scope(scope, operation_id)` returning `PurgeResult`: a resumable, idempotent removal of all canonical objects, catalog rows and control objects of one scope.
- `FileStore::delete` now also removes the deleted object's metadata sidecars.

## 0.1.0

Initial portable object store, journal and catalog.
