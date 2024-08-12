Start a valkey docker container with the following command:
```bash
docker run -p 6379:6379 --rm valkey/valkey:7.2.6
```

To run it detached, just add the `-d` flag:
```bash
docker run -p 6379:6379 --rm -d valkey/valkey:7.2.6
```
