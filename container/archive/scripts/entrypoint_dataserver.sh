#!/usr/bin/env bash
set -euo pipefail

# Create dataserver directory structure
mkdir -p /root/space/dataserver/{sdata,cdata,tdata,pdata,idata,ddata,xdata,ydata}

# Generate self-signed SSL certificate (dataserver requires HTTPS)
cd /root/scripts
openssl genrsa -out mylocalhost.key 2048 2>/dev/null
openssl req -new -key mylocalhost.key -out mylocalhost.csr \
    -subj "/CN=localhost" 2>/dev/null
openssl x509 -req -days 365 -in mylocalhost.csr \
    -signkey mylocalhost.key -out mylocalhost.crt 2>/dev/null
cat mylocalhost.key mylocalhost.crt > mylocalhost.pem

echo "Starting dataserver on port 8013..."
exec python -u -m dataserver.main --config /root/scripts/ds.cfg
