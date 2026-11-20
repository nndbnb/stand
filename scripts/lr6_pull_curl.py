#!/usr/bin/env python3
"""Download public images needed by lab6 via curl, verify and load into Docker."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
from urllib.parse import urlencode

ACCEPT = ', '.join((
    'application/vnd.oci.image.index.v1+json',
    'application/vnd.docker.distribution.manifest.list.v2+json',
    'application/vnd.oci.image.manifest.v1+json',
    'application/vnd.docker.distribution.manifest.v2+json',
))


def digest_hex(value):
    if not re.fullmatch(r'sha256:[0-9a-f]{64}', value):
        raise ValueError('Unsupported or invalid digest')
    return value.split(':')[1]


def valid_blob(path, descriptor):
    expected = digest_hex(descriptor['digest'])
    if not path.is_file() or path.stat().st_size != descriptor['size']:
        return False
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest() == expected


def select_manifest(document, os_name, arch):
    candidates = [d for d in document['manifests']
                  if d.get('platform', {}).get('os') == os_name
                  and d.get('platform', {}).get('architecture') == arch
                  and d.get('platform', {}).get('variant', '') in ('', 'v8')]
    if len(candidates) != 1:
        raise ValueError(f'Expected one manifest for {os_name}/{arch}, found {len(candidates)}')
    return candidates[0]


def curl(url, headers=(), output=None, tls12=False):
    # Headers go through stdin, keeping bearer tokens out of process arguments.
    command = ['curl', '--disable', '--noproxy', '*', '-4', '--http1.1',
               '--fail', '--silent', '--show-error', '--location',
               '--proto', '=https', '--proto-redir', '=https',
               '--connect-timeout', '15', '--max-time', '1800',
               '--retry', '3', '--retry-all-errors', '--retry-max-time', '90']
    if tls12:
        command += ['--tlsv1.2', '--tls-max', '1.2']
    if headers:
        command += ['--header', '@-']
    if output is not None:
        command += ['--output', str(output)]
    command.append(url)
    result = subprocess.run(command, input=('\n'.join(headers)+'\n').encode(),
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode:
        # Do not print redirect URLs or request headers on errors.
        error = result.stderr.decode(errors='replace')
        error = re.sub(r'https://\S+', '[URL]', error)
        raise RuntimeError(f'curl failed ({result.returncode}): {error.strip()}')
    return result.stdout


def parse_ref(ref):
    if ref.startswith('ghcr.io/'):
        registry, token_url, service = 'ghcr.io', 'https://ghcr.io/token', 'ghcr.io'
        name = ref[len('ghcr.io/'):]
    else:
        registry, token_url, service = 'registry-1.docker.io', 'https://auth.docker.io/token', 'registry.docker.io'
        name = ref.removeprefix('docker.io/')
        if '/' not in name:
            name = 'library/' + name
    match = re.fullmatch(r'([a-z0-9_-]+(?:/[a-z0-9._-]+)*):([A-Za-z0-9_.-]+)', name)
    if not match:
        raise ValueError('Expected an explicit Docker Hub or GHCR image tag: ' + ref)
    return registry, token_url, service, match[1], match[2]


def verify_local(ref, cache):
    manifest = cache/'images'/hashlib.sha256(ref.encode()).hexdigest()/'manifest.json'
    if not manifest.is_file():
        return  # A regular docker pull has no curl cache.
    descriptor = json.loads(manifest.read_bytes())['config']
    blob = cache/'blobs'/digest_hex(descriptor['digest'])
    if not valid_blob(blob, descriptor):
        raise ValueError('Invalid cached config for ' + ref)
    source = json.loads(blob.read_bytes())
    image = json.loads(subprocess.check_output(['docker', 'image', 'inspect', ref]))[0]
    expected = source['rootfs']['diff_ids']
    if (image['Os'] != source['os'] or image['Architecture'] != source['architecture']
            or image['RootFS']['Layers'] != expected):
        raise ValueError('Loaded platform or uncompressed layers differ for ' + ref)
    for key in ('Entrypoint', 'Cmd', 'Env', 'User', 'WorkingDir'):
        if (image['Config'].get(key) or None) != (source['config'].get(key) or None):
            raise ValueError(f'Loaded config differs for {ref}: {key}')
    print(f'Проверен {ref}: {image["Os"]}/{image["Architecture"]}, слоёв: {len(expected)}', flush=True)


def pull(ref, cache, os_name, arch, tls12=False):
    registry, token_url, service, repository, tag = parse_ref(ref)
    token_data = json.loads(curl(token_url + '?' + urlencode({
        'service': service, 'scope': f'repository:{repository}:pull'}), tls12=tls12))
    token = token_data.get('token') or token_data.get('access_token')
    if not isinstance(token, str) or not token or '\n' in token or '\r' in token:
        raise ValueError('Registry did not return a valid anonymous token')
    headers = [f'Authorization: Bearer {token}', f'Accept: {ACCEPT}']
    base = f'https://{registry}/v2/{repository}'
    raw = curl(f'{base}/manifests/{tag}', headers, tls12=tls12)
    doc = json.loads(raw)
    if 'manifests' in doc:
        descriptor = select_manifest(doc, os_name, arch)
        raw = curl(f'{base}/manifests/{descriptor["digest"]}', headers, tls12=tls12)
        if (len(raw) != descriptor['size'] or
                hashlib.sha256(raw).hexdigest() != digest_hex(descriptor['digest'])):
            raise ValueError('Platform manifest digest or size mismatch')
        doc = json.loads(raw)
    if doc.get('schemaVersion') != 2 or 'config' not in doc or 'layers' not in doc:
        raise ValueError('Unsupported image manifest')
    image_dir = cache/'images'/hashlib.sha256(ref.encode()).hexdigest()
    blob_dir = cache/'blobs'
    image_dir.mkdir(parents=True, exist_ok=True)
    blob_dir.mkdir(parents=True, exist_ok=True)
    descriptors = [doc['config'], *doc['layers']]
    for number, descriptor in enumerate(descriptors, 1):
        digest = descriptor['digest']
        blob = blob_dir/digest_hex(digest)
        if valid_blob(blob, descriptor):
            print(f'  [{number}/{len(descriptors)}] cached {digest[:23]}', flush=True)
        else:
            print(f'  [{number}/{len(descriptors)}] downloading {digest[:23]} ({descriptor["size"]} bytes)', flush=True)
            partial = blob.with_suffix('.part')
            curl(f'{base}/blobs/{digest}', headers, partial, tls12)
            if not valid_blob(partial, descriptor):
                partial.unlink(missing_ok=True)
                raise ValueError('Blob digest or size mismatch: '+digest)
            partial.replace(blob)
        target = image_dir/blob.name
        if target.exists():
            if target.samefile(blob):
                continue
            target.unlink()
        os.link(blob, target)
    config = json.loads((blob_dir/digest_hex(doc['config']['digest'])).read_bytes())
    if config.get('os') != os_name or config.get('architecture') != arch:
        raise ValueError('Image config platform does not match the Docker server')
    (image_dir/'manifest.json').write_bytes(raw)
    # All network work is complete. skopeo reads only the verified local blobs.
    # Use an archive rather than skopeo's Docker API client: older Ubuntu
    # skopeo versions can use an API version no longer accepted by Docker 29.
    with tempfile.TemporaryDirectory(prefix='import-', dir=cache) as temporary:
        archive = Path(temporary)/'image.tar'
        subprocess.run(['skopeo', 'copy', 'dir:'+str(image_dir),
                        'docker-archive:'+str(archive)+':'+ref], check=True)
        subprocess.run(['docker', 'load', '--input', str(archive)], check=True)
    verify_local(ref, cache)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify-local', action='store_true', help='Check an imported image against its cached config without network access')
    parser.add_argument('images', nargs='+', help='Explicit public image tags')
    parser.add_argument('--tls12', action='store_true', help='Force TLS 1.2 for curl')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    os.chdir(root)
    cache = root/'data/lr6/image-cache'
    if args.verify_local:
        for ref in args.images:
            verify_local(ref, cache)
        return
    for command in ('curl', 'skopeo', 'docker'):
        if not shutil.which(command):
            raise RuntimeError('Missing command: '+command)
    platform = subprocess.check_output(['docker', 'info', '--format', '{{.OSType}}/{{.Architecture}}'], text=True).strip()
    os_name, arch = platform.split('/')
    arch = {'x86_64': 'amd64', 'aarch64': 'arm64'}.get(arch, arch)
    images = args.images
    cache.mkdir(parents=True, exist_ok=True)
    with (cache/'download.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for ref in images:
            if subprocess.run(['docker', 'image', 'inspect', ref], stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL).returncode == 0:
                verify_local(ref, cache)
                print('Already present: '+ref, flush=True)
                continue
            print('Loading: '+ref, flush=True)
            pull(ref, cache, os_name, arch, args.tls12)
    print('All requested images are available in Docker.', flush=True)


if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, ValueError, KeyError, OSError, subprocess.CalledProcessError) as exc:
        print('ERROR: '+str(exc), file=sys.stderr)
        sys.exit(1)
