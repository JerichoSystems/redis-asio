#!/usr/bin/env bash
set -euo pipefail
mkdir -p tls
openssl req -x509 -nodes -newkey rsa:2048 -days 365 \
  -keyout tls/ca.key -out tls/ca.crt -subj "/CN=redis-asio-test-ca"
openssl req -new -nodes -newkey rsa:2048 \
  -keyout tls/server.key -out tls/server.csr -subj "/CN=localhost"
openssl x509 -req -in tls/server.csr -CA tls/ca.crt -CAkey tls/ca.key -CAcreateserial \
  -out tls/server.crt -days 365 -sha256
echo "Generated tls/ca.crt, tls/server.crt, tls/server.key"
