# Darwin Docker Deployment

Docker dedicado para ejecutar Darwin/Hermes en Linux sin depender de `python3-venv` del host.

## Principios

- La imagen contiene solo código y dependencias.
- Los conocimientos viven fuera de la imagen en `LocalHermes/home` montado como `/data/hermes`.
- Secrets (`.env`, `auth.json`) no se copian al build context ni se bakean.
- `Darwin` sin argumentos abre el TUI: `hermes -p default --tui`.
- `Darwin <args>` delega a `hermes <args>`.

## Build

```bash
cd /home/juan/LocalHermes/deploy/darwin-docker
docker compose build darwin
```

## Instalar comando host

```bash
/home/juan/LocalHermes/deploy/darwin-docker/install-host-wrapper.sh
# volver a abrir shell o: source ~/.profile
Darwin --version
Darwin
```

## Gateway opcional

```bash
cd /home/juan/LocalHermes/deploy/darwin-docker
docker compose --profile gateway up -d darwin-gateway
docker compose --profile gateway logs -f darwin-gateway
```

## Datos persistentes importantes

- `/home/juan/LocalHermes/home/state.db` — memoria/sesiones SQLite principal.
- `/home/juan/LocalHermes/home/memories/` — MEMORY.md / USER.md fallback.
- `/home/juan/LocalHermes/home/skills/` — skills instaladas/creadas.
- `/home/juan/LocalHermes/home/profiles/` — perfiles aislados.
- `/home/juan/LocalHermes/home/cron/` — jobs programados.
- `/home/juan/LocalHermes/home/kanban/` y `kanban.db` — tableros/cola multiagente.
