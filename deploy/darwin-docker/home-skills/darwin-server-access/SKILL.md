---
name: darwin-server-access
description: Acceso operativo de Darwin a Hermes OVH y Server Alemán/Hetzner.
---

# Darwin Server Access

Usar esta skill cuando Juan pida operar, verificar, entrar, diagnosticar o sincronizar:

- **Hermes OVH**: alias SSH `ovh` / `hermes-ovh`.
- **Server Alemán / Hetzner**: alias SSH `server-aleman` / `aleman` / `hetzner`.

## Comandos disponibles dentro de Darwin

```bash
ssh-ovh
ssh-aleman
server-access-check
```

Equivalentes directos:

```bash
ssh ovh
ssh server-aleman
```

## Endpoints de memoria compartida

- Honcho OVH: `http://100.92.211.3:18800`
- Engram OVH: `http://100.92.211.3:17437`

## Reglas de seguridad

1. No imprimir claves privadas, tokens, `.env`, `auth.json` ni secretos.
2. Antes de mutar datos de Honcho/Engram, crear backup o snapshot verificable.
3. Preferir Tailscale/private IPs; no abrir puertos públicos salvo pedido explícito.
4. Para demostrar acceso, usar `server-access-check`.
5. Para operar Docker/servicios del host alemán, entrar por `ssh-aleman` y ejecutar en el host.
6. Para operar Hermes OVH, entrar por `ssh-ovh`; usar `sudo -n` sólo si hace falta y verificar antes de mutar.

## Identidad esperada

- `ssh-aleman` debe responder desde host `ubuntu-8gb-fsn1-2` como usuario `juan`.
- `ssh-ovh` debe responder desde host OVH como usuario `ubuntu`.
