FROM mambaorg/micromamba:0.22.0

LABEL org.opencontainers.image.title="nastyprisms"
LABEL org.opencontainers.image.url="https://github.com/brews/nastyprisms"
LABEL org.opencontainers.image.source="https://github.com/brews/nastyprisms"

COPY --chown=$MAMBA_USER:$MAMBA_USER environment.yaml /tmp/env.yaml
RUN micromamba install -y -f /tmp/env.yaml && \
    micromamba clean --all --yes
COPY --chown=$MAMBA_USER:$MAMBA_USER download_prism.py /opt/src/download_prism.py

ENTRYPOINT ["/usr/local/bin/_entrypoint.sh", "python", "/opt/src/download_prism.py"]
