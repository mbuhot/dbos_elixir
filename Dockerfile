FROM hexpm/elixir:1.19.0-erlang-28.0-debian-bookworm-slim

ENV MIX_ENV=test
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential git curl ca-certificates postgresql-client docker.io && \
    mkdir -p /root/.docker/cli-plugins && \
    curl -fsSL \
      https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-x86_64 \
      -o /root/.docker/cli-plugins/docker-compose && \
    chmod +x /root/.docker/cli-plugins/docker-compose && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN mix local.hex --force && mix local.rebar --force

COPY mix.exs mix.lock ./
RUN mix deps.get

COPY . .
RUN mix deps.compile && mix compile

CMD ["mix", "run", "--no-halt"]
