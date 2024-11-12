# Newsware utils

# Compatibility
nwfs and filewatcher modules only run on Linux since they rely on inotify API.

# Testing

To also run integration test set INTEGRATION env variable to anything:

```bash
INTEGRATION=true go test ./...
```