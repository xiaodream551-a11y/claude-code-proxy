# Rust project checks

set positional-arguments
set shell := ["bash", "-euo", "pipefail", "-c"]

# List available commands
default:
    @just --list

# Run project checks through checkle
check:
    checkle run all

# Run check and fail if there are uncommitted changes for CI
check-ci: check
    #!/usr/bin/env bash
    set -euo pipefail
    if ! git diff --quiet || ! git diff --cached --quiet; then
        echo "Error: check caused uncommitted changes"
        echo "Run 'just check' locally and commit the results"
        git diff --stat
        exit 1
    fi

# Install shims into the Git hooks directory
install-hooks:
    scripts/install-git-hook-shims

# Check Rust formatting through checkle
format:
    checkle run format-check

# Check clippy through checkle
clippy:
    checkle run clippy

# Check the build through checkle
build:
    checkle run build

# Run tests through checkle
test:
    checkle run test

# Install release binary globally
install:
    cargo install --offline --path . --locked

# Build, deploy, restart, and verify the Homebrew-managed macOS service
deploy-homebrew:
    #!/usr/bin/env bash
    set -eEuo pipefail
    if [[ -n "$(git status --porcelain --untracked-files=normal)" ]]; then
        echo "refusing to deploy a dirty worktree; commit or remove local changes first" >&2
        exit 1
    fi
    cargo build --release --locked
    prefix="$(brew --prefix claude-code-proxy)"
    target="$prefix/bin/claude-code-proxy"
    source="$(pwd)/target/release/claude-code-proxy"
    health_url="${CCP_DEPLOY_HEALTH_URL:-http://127.0.0.1:${PORT:-18765}/healthz}"
    case "$health_url" in
        */healthz) version_url="${health_url%/healthz}/version" ;;
        *) echo "CCP_DEPLOY_HEALTH_URL must end with /healthz" >&2; exit 1 ;;
    esac
    lock_dir="$target.deploy.lock"
    lock_owned=false
    acquire_lock() {
        if mkdir "$lock_dir" 2>/dev/null; then
            lock_owned=true
            echo "$$" > "$lock_dir/owner"
            return 0
        fi
        owner="$(cat "$lock_dir/owner" 2>/dev/null || true)"
        if [[ "$owner" =~ ^[0-9]+$ ]] && kill -0 "$owner" 2>/dev/null; then
            echo "another deployment (pid $owner) holds $lock_dir" >&2
            return 1
        fi
        stale="$lock_dir.stale.$$"
        if ! mv "$lock_dir" "$stale" 2>/dev/null; then
            echo "could not recover stale deployment lock $lock_dir" >&2
            return 1
        fi
        rm -f "$stale/owner"
        rmdir "$stale" 2>/dev/null || {
            mv "$stale" "$lock_dir" 2>/dev/null || true
            echo "stale deployment lock contains unexpected files: $stale" >&2
            return 1
        }
        mkdir "$lock_dir"
        lock_owned=true
        echo "$$" > "$lock_dir/owner"
    }
    release_lock() {
        owner="$(cat "$lock_dir/owner" 2>/dev/null || true)"
        if [[ "$lock_owned" == true ]] && [[ -z "$owner" || "$owner" == "$$" ]]; then
            rm -f "$lock_dir/owner"
            rmdir "$lock_dir" 2>/dev/null || true
        fi
    }
    sync_file() {
        python3 -c 'import os,sys; fd=os.open(sys.argv[1], os.O_RDONLY); os.fsync(fd); os.close(fd)' "$1"
    }
    sync_parent() {
        python3 -c 'import os,sys; p=os.path.dirname(sys.argv[1]); fd=os.open(p, os.O_RDONLY); os.fsync(fd); os.close(fd)' "$1"
    }
    service_pid() {
        launchctl print "gui/$(id -u)/homebrew.mxcl.claude-code-proxy" 2>/dev/null \
            | awk '$1 == "pid" && $2 == "=" { print $3; exit }'
    }
    trap release_lock EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM HUP
    acquire_lock
    backup="$target.pre-deploy-$(date +%Y%m%d-%H%M%S)-$$"
    install -m 0755 "$target" "$backup"
    sync_file "$backup"
    sync_parent "$backup"
    old_sha="$(shasum -a 256 "$backup" | awk '{print $1}')"
    old_pid="$(service_pid || true)"
    rollback() {
        status="${1:-1}"
        trap - ERR INT TERM HUP
        set +e
        rollback_tmp="$(mktemp "$target.rollback.XXXXXX" 2>/dev/null)"
        if [[ -n "$rollback_tmp" ]] \
            && install -m 0755 "$backup" "$rollback_tmp" \
            && sync_file "$rollback_tmp"; then
            if mv -f "$rollback_tmp" "$target"; then
                sync_parent "$target" || echo "rollback parent fsync failed" >&2
            else
                rm -f "$rollback_tmp"
                echo "rollback could not replace $target" >&2
            fi
        else
            [[ -z "$rollback_tmp" ]] || rm -f "$rollback_tmp"
            echo "rollback could not prepare a verified replacement" >&2
        fi
        restored_target_sha="$(shasum -a 256 "$target" 2>/dev/null | awk '{print $1}')"
        if [[ "$restored_target_sha" != "$old_sha" ]]; then
            echo "rollback file verification failed: expected $old_sha, found ${restored_target_sha:-unknown}" >&2
        fi
        brew services restart claude-code-proxy >/dev/null || true
        for _ in $(seq 1 30); do
            if curl -fsS --max-time 2 "$health_url" >/dev/null 2>&1; then
                break
            fi
            sleep 1
        done
        restored_pid="$(service_pid || true)"
        restored_mapping="$(lsof -a -p "$restored_pid" -d txt -FDi 2>/dev/null || true)"
        if python3 -c 'import os,sys; lines=sys.argv[1].splitlines(); d=next((x[1:] for x in lines if x.startswith("D")), ""); i=next((x[1:] for x in lines if x.startswith("i")), ""); s=os.stat(sys.argv[2]); raise SystemExit(0 if d and i and int(d,0)==s.st_dev and int(i)==s.st_ino else 1)' "$restored_mapping" "$target" 2>/dev/null; then
            restored_running_sha="$(shasum -a 256 "$target" 2>/dev/null | awk '{print $1}')"
        else
            restored_running_sha=""
        fi
        if [[ "$restored_running_sha" != "$old_sha" ]]; then
            echo "rollback runtime verification failed: expected $old_sha, running ${restored_running_sha:-unknown}" >&2
        fi
        exit "$status"
    }
    trap 'rollback $?' ERR
    trap 'rollback 130' INT
    trap 'rollback 143' TERM HUP
    deploy_tmp="$(mktemp "$target.deploy.XXXXXX")"
    install -m 0755 "$source" "$deploy_tmp"
    sync_file "$deploy_tmp"
    mv -f "$deploy_tmp" "$target"
    sync_parent "$target"
    brew services restart claude-code-proxy >/dev/null
    expected="$(shasum -a 256 "$source" | awk '{print $1}')"
    expected_git="$(git rev-parse HEAD)"
    target_real="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$target")"
    runtime=""
    for _ in $(seq 1 30); do
        runtime="$(curl -fsS --max-time 2 "$version_url" 2>/dev/null || true)"
        if python3 -c 'import json,os,sys; d=json.loads(sys.argv[1]); old=sys.argv[4]; ok=d.get("binarySha256")==sys.argv[2] and d.get("gitSha")==sys.argv[3] and d.get("gitDirty") is False and os.path.realpath(d.get("executable", ""))==sys.argv[5] and (not old or str(d.get("pid", ""))!=old); raise SystemExit(0 if ok else 1)' "$runtime" "$expected" "$expected_git" "$old_pid" "$target_real" 2>/dev/null; then
            break
        fi
        sleep 1
    done
    curl -fsS --max-time 2 "$health_url" >/dev/null
    python3 -c 'import json,os,sys; d=json.loads(sys.argv[1]); old=sys.argv[4]; ok=d.get("binarySha256")==sys.argv[2] and d.get("gitSha")==sys.argv[3] and d.get("gitDirty") is False and os.path.realpath(d.get("executable", ""))==sys.argv[5] and (not old or str(d.get("pid", ""))!=old); raise SystemExit(0 if ok else 1)' "$runtime" "$expected" "$expected_git" "$old_pid" "$target_real"
    running_pid="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["pid"])' "$runtime")"
    trap - ERR INT TERM HUP
    echo "deployed pid=$running_pid sha256=$expected git=$expected_git backup=$backup"

# Install debug binary globally via symlink
install-dev:
    cargo build && ln -sf $(pwd)/target/debug/claude-code-proxy ~/.cargo/bin/claude-code-proxy

# Run the application
run *ARGS:
    cargo run -- "$@"

# Internal release helper
_release bump *ARGS:
    @cargo-release {{bump}} {{ARGS}}

# Release a new patch version
release *ARGS:
    @just _release patch --skip-publish {{ARGS}}
