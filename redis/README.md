# TLS Support
This library supports TLS. Follow the code snippet below to get started.

```bash
chmod +x tls/gen-test-certs.sh && tls/gen-test-certs.sh
docker compose up -d
# run tests/bench via TLS
export REDIS_HOST=127.0.0.1 REDIS_PORT=6380 REDIS_TLS=1 REDIS_TLS_VERIFY=0
ctest --test-dir ../build --output-on-failure  # for example
```