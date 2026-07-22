# Match the Slurm version deployed by SUNK so sacctmgr speaks the same RPC
# version as slurmdbd.
FROM ghcr.io/coreweave/slurm-containers/controller:v25.05.3-coreweave.5-ubuntu22.04

COPY --from=ghcr.io/astral-sh/uv:0.9.24 /uv /uvx /usr/local/bin/

ENV UV_PYTHON_INSTALL_DIR=/opt/uv/python
WORKDIR /opt/pw-slurm-group-sync

RUN uv python install 3.12

COPY pyproject.toml uv.lock ./
RUN uv sync --locked

COPY sync_groups.py entrypoint.sh ./

ENTRYPOINT ["/opt/pw-slurm-group-sync/entrypoint.sh"]
