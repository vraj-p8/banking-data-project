#!/usr/bin/env sh
set -e

echo "Waiting for MinIO to be ready..."
until mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do
  sleep 2
done

mc mb "local/${MINIO_BUCKET}" --ignore-existing

mc event remove "local/${MINIO_BUCKET}" --force >/dev/null 2>&1 || true
mc event add "local/${MINIO_BUCKET}" arn:minio:sqs::minio:webhook --event put

echo "MinIO bucket and event notifications configured."
