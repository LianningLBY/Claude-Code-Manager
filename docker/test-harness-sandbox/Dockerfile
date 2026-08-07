FROM node:22-bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        python3 \
        python3-pip \
        python3-venv \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 sandbox \
    && useradd --uid 10001 --gid 10001 --create-home sandbox

COPY backend/services/test_harness_egress_proxy.py /opt/ccm/egress_proxy.py
RUN chmod 0555 /opt/ccm/egress_proxy.py

USER 10001:10001
WORKDIR /workspace
ENTRYPOINT ["/usr/bin/tail", "-f", "/dev/null"]
