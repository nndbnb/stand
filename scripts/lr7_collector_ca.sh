#!/usr/bin/env bash
# Prepare a CA bundle readable by the unprivileged MOEX collector.
set -euo pipefail
cd "$(dirname "$0")/.."

dir=data/lr7/moex-ca
mkdir -p "$dir"
tmp=$(mktemp -d "$dir/.prepare.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

root="$dir/root.crt"
if [[ -s conf/moex-ca/root.crt ]]; then
    root=conf/moex-ca/root.crt
elif [[ ! -s "$root" ]]; then
    curl --fail --silent --show-error --location --proto '=https' --proto-redir '=https' \
        --retry 2 --max-time 60 --output "$tmp/download.crt" \
        https://gu-st.ru/content/lending/russian_trusted_root_ca_pem.crt
    root="$tmp/download.crt"
fi

# Normalize to one PEM certificate even if the input file contains extra text.
openssl x509 -in "$root" -out "$tmp/root.crt"
openssl verify -check_ss_sig -CAfile "$tmp/root.crt" "$tmp/root.crt" >/dev/null
cat /etc/ssl/certs/ca-certificates.crt "$tmp/root.crt" > "$tmp/bundle.pem"
chmod 644 "$tmp/root.crt" "$tmp/bundle.pem"
mv "$tmp/root.crt" "$dir/root.crt"
if ! cmp -s "$tmp/bundle.pem" "$dir/bundle.pem"; then
    mv "$tmp/bundle.pem" "$dir/bundle.pem"
fi
echo "Готов набор сертификатов для collector: $dir/bundle.pem"
