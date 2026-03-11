# Building Custom Airbyte Components

## Prerequisites
- **Java 21** (min required: 14+). Install: `brew install openjdk@21`
- **Docker** running locally
- **Python 3.9+** and **Pip 20.1+**

### Verify Environment
```bash
./tools/bin/check_requirements.sh
```

---

## airbyte-workload-launcher

### 1. Compile
```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@21 ./gradlew :oss:airbyte-workload-launcher:distTar \
  -Pversion=dev-custom -x test -x spotlessCheck -x spotlessApply \
  -x :oss:airbyte-db:jooq:generateConfigsDatabaseJooq \
  -x :oss:airbyte-db:jooq:generateJobsDatabaseJooq --no-daemon
```

### 2. Build Docker Image
```bash
cp airbyte-workload-launcher/build/distributions/airbyte-app.tar airbyte-workload-launcher/airbyte-app.tar
docker build --no-cache -t rocknfund/airbyte-workload-launcher:latest airbyte-workload-launcher/
```

### 3. Publish
```bash
docker login -u rocknfund
docker push rocknfund/airbyte-workload-launcher:latest
```

---

## airbyte-bootloader-k8s

### 1. Build Docker Image
```bash
docker build --no-cache -t rocknfund/airbyte-bootloader-k8s:latest airbyte-bootloader-k8s/
```

### 2. Publish
```bash
docker push rocknfund/airbyte-bootloader-k8s:latest
```

---

## Database Schemas (if needed)

If database migrations changed, regenerate jOOQ classes before compiling:

```bash
./tools/bin/make-jooq-schema.sh
```

---

## Run

```bash
# Production
docker compose up -d

# Cold start (reset all data)
docker compose down -v && docker compose up -d
```

---

## Production Security

> ⚠️ Before deploying, regenerate secrets in `.env`:
>
> ```bash
> # Generate new values:
> openssl rand -hex 16   # DATABASE_PASSWORD
> openssl rand -hex 48   # AIRBYTE_INTERNAL_API_AUTH_SIGNATURE_SECRET
> openssl rand -hex 48   # AB_JWT_SIGNATURE_SECRET
> openssl rand -hex 48   # DATAPLANE_CLIENT_SECRET
> ```
