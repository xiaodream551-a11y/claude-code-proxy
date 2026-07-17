#!/usr/bin/env bash
#
# claude-code-proxy installation script
# Usage: curl -fsSL https://raw.githubusercontent.com/xiaodream551-a11y/claude-code-proxy/main/scripts/install.sh | bash
#
# Environment variables:
#   CLAUDE_CODE_PROXY_VERSION      - Pin a specific version (e.g., v0.1.0)
#   CLAUDE_CODE_PROXY_INSTALL_DIR  - Override install directory (default: /usr/local/bin or ~/.local/bin)
#   CLAUDE_CODE_PROXY_REPOSITORY   - Override the GitHub release repository
#
# Examples:
#   CLAUDE_CODE_PROXY_VERSION=v0.1.0 bash install.sh
#   CLAUDE_CODE_PROXY_INSTALL_DIR=/opt/bin bash install.sh
#

set -e

BIN_NAME="claude-code-proxy"
REPO="${CLAUDE_CODE_PROXY_REPOSITORY:-xiaodream551-a11y/claude-code-proxy}"
EXPECTED_GIT_SHA=""
EXPECTED_BINARY_SHA=""

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}==>${NC} $1"; }
log_success() { echo -e "${GREEN}==>${NC} $1"; }
log_warning() { echo -e "${YELLOW}==>${NC} $1"; }
log_error()   { echo -e "${RED}Error:${NC} $1" >&2; }

detect_platform() {
	local os arch

	case "$(uname -s)" in
	Darwin) os="darwin" ;;
	Linux)  os="linux" ;;
	*)
		log_error "Unsupported operating system: $(uname -s)"
		echo ""
		echo "${BIN_NAME} supports macOS and Linux."
		echo "For other platforms, build from source with Rust/Cargo:"
		echo "  git clone https://github.com/${REPO}"
		echo ""
		exit 1
		;;
	esac

	case "$(uname -m)" in
	x86_64 | amd64)   arch="amd64" ;;
	aarch64 | arm64)  arch="arm64" ;;
	*)
		log_error "Unsupported architecture: $(uname -m)"
		echo ""
		echo "${BIN_NAME} prebuilt binaries are available for amd64 and arm64."
		echo "For other architectures, build from source with Rust/Cargo:"
		echo "  git clone https://github.com/${REPO}"
		echo ""
		exit 1
		;;
	esac

	echo "${os}-${arch}"
}

install_from_release() {
	log_info "Installing ${BIN_NAME} from GitHub releases..."

	local platform=$1
	local tmp_dir
	tmp_dir=$(mktemp -d)
	trap 'rm -rf "$tmp_dir"' EXIT

	local version="${CLAUDE_CODE_PROXY_VERSION:-}"

	if [ -z "$version" ]; then
		log_info "Fetching latest release..."
		local latest_url="https://api.github.com/repos/${REPO}/releases/latest"
		local release_json

		if command -v curl &>/dev/null; then
			release_json=$(curl -fsSL --retry 3 --retry-connrefused --connect-timeout 10 --max-time 30 "$latest_url")
		elif command -v wget &>/dev/null; then
			release_json=$(wget --tries=3 --timeout=30 -qO- "$latest_url")
		else
			log_error "Neither curl nor wget found. Please install one of them."
			exit 1
		fi

		version=$(echo "$release_json" | grep '"tag_name"' | sed -E 's/.*"tag_name": "([^"]+)".*/\1/')

		if [ -z "$version" ]; then
			log_error "Failed to fetch latest version"
			echo ""
			echo "This might be due to network issues or GitHub API rate limits."
			echo "You can specify a version manually:"
			echo "  CLAUDE_CODE_PROXY_VERSION=v0.1.0 bash install.sh"
			echo ""
			exit 1
		fi
	fi

	log_info "Installing version: $version"

	local archive_name="${BIN_NAME}-${platform}.tar.gz"
	local download_url="https://github.com/${REPO}/releases/download/${version}/${archive_name}"

	log_info "Downloading $archive_name..."

	cd "$tmp_dir"
	if command -v curl &>/dev/null; then
		if ! curl -fsSL --retry 3 --retry-connrefused --connect-timeout 10 --max-time 120 -o "$archive_name" "$download_url"; then
			log_error "Download failed"
			echo ""
			echo "The release may not have a prebuilt binary for your platform."
			echo ""
			cd - >/dev/null || cd "$HOME"
			exit 1
		fi
	elif command -v wget &>/dev/null; then
		if ! wget --tries=3 --timeout=120 -q -O "$archive_name" "$download_url"; then
			log_error "Download failed"
			cd - >/dev/null || cd "$HOME"
			exit 1
		fi
	fi

	log_info "Verifying checksum..."
	local checksum_file="${BIN_NAME}-${platform}.sha256"
	local checksum_url="https://github.com/${REPO}/releases/download/${version}/${checksum_file}"
	local provenance_file="${BIN_NAME}-${platform}.provenance.json"
	local provenance_url="https://github.com/${REPO}/releases/download/${version}/${provenance_file}"

	if command -v curl &>/dev/null; then
		if ! curl -fsSL --retry 3 --retry-connrefused --connect-timeout 10 --max-time 30 -o "$checksum_file" "$checksum_url"; then
			log_error "Failed to download checksum file"
			cd - >/dev/null || cd "$HOME"
			exit 1
		fi
	elif command -v wget &>/dev/null; then
		if ! wget --tries=3 --timeout=30 -q -O "$checksum_file" "$checksum_url"; then
			log_error "Failed to download checksum file"
			cd - >/dev/null || cd "$HOME"
			exit 1
		fi
	fi

	if command -v sha256sum &>/dev/null; then
		if ! sha256sum -c "$checksum_file" &>/dev/null; then
			log_error "Checksum verification failed"
			cd - >/dev/null || cd "$HOME"
			exit 1
		fi
	elif command -v shasum &>/dev/null; then
		if ! shasum -a 256 -c "$checksum_file" &>/dev/null; then
			log_error "Checksum verification failed"
			cd - >/dev/null || cd "$HOME"
			exit 1
		fi
	else
		log_error "Neither sha256sum nor shasum found; refusing an unverified install"
		exit 1
	fi

	log_success "Checksum verified"

	if ! command -v python3 &>/dev/null; then
		log_error "python3 is required to verify release provenance"
		exit 1
	fi
	if command -v curl &>/dev/null; then
		curl -fsSL --retry 3 --retry-connrefused --connect-timeout 10 --max-time 30 \
			-o "$provenance_file" "$provenance_url"
	else
		wget --tries=3 --timeout=30 -q -O "$provenance_file" "$provenance_url"
	fi
	local archive_sha
	if command -v sha256sum &>/dev/null; then
		archive_sha=$(sha256sum "$archive_name" | awk '{print $1}')
	else
		archive_sha=$(shasum -a 256 "$archive_name" | awk '{print $1}')
	fi
	local provenance_identity
	provenance_identity=$(python3 -c 'import json,re,sys; d=json.load(open(sys.argv[1],encoding="utf-8")); git=d.get("gitSha",""); binary=d.get("binarySha256",""); ok=d.get("repository")==sys.argv[2] and d.get("tag")==sys.argv[3] and d.get("platform")==sys.argv[4] and d.get("archiveSha256","").lower()==sys.argv[5].lower() and re.fullmatch(r"[0-9a-fA-F]{40}",git) and re.fullmatch(r"[0-9a-fA-F]{64}",binary); print(git.lower(),binary.lower()) if ok else sys.exit(1)' "$provenance_file" "$REPO" "$version" "$platform" "$archive_sha") || {
		log_error "Release provenance does not match repository, tag, platform, or archive"
		exit 1
	}
	read -r EXPECTED_GIT_SHA EXPECTED_BINARY_SHA <<<"$provenance_identity"

	log_info "Extracting archive..."
	if ! tar -xzf "$archive_name"; then
		log_error "Failed to extract archive"
		exit 1
	fi
	local extracted_sha identity_json
	if command -v sha256sum &>/dev/null; then
		extracted_sha=$(sha256sum "$BIN_NAME" | awk '{print $1}')
	else
		extracted_sha=$(shasum -a 256 "$BIN_NAME" | awk '{print $1}')
	fi
	identity_json=$("./$BIN_NAME" version --json)
	python3 -c 'import json,sys; identity=json.loads(sys.argv[1]); ok=sys.argv[2].lower()==sys.argv[3].lower() and identity.get("binarySha256","").lower()==sys.argv[2].lower() and identity.get("gitSha","").lower()==sys.argv[4].lower() and identity.get("gitDirty") is False; raise SystemExit(0 if ok else 1)' "$identity_json" "$EXPECTED_BINARY_SHA" "$extracted_sha" "$EXPECTED_GIT_SHA" || {
		log_error "Extracted binary does not match release provenance"
		exit 1
	}

	local install_dir="${CLAUDE_CODE_PROXY_INSTALL_DIR:-}"
	if [ -z "$install_dir" ]; then
		if [[ -w /usr/local/bin ]]; then
			install_dir="/usr/local/bin"
		else
			install_dir="$HOME/.local/bin"
			mkdir -p "$install_dir"
		fi
	fi

	if [ -f "$install_dir/${BIN_NAME}" ]; then
		local existing_version
		existing_version=$("$install_dir/${BIN_NAME}" --version 2>/dev/null | grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' || echo "unknown")
		log_info "Existing installation found: $existing_version"
		log_info "Upgrading to: $version"
	fi

	log_info "Installing to $install_dir..."
	local tmp_binary="$install_dir/${BIN_NAME}.tmp.$$"

	if [[ -w "$install_dir" ]]; then
		cp "${BIN_NAME}" "$tmp_binary"
		chmod +x "$tmp_binary"
		mv -f "$tmp_binary" "$install_dir/${BIN_NAME}"
	else
		if ! sudo cp "${BIN_NAME}" "$tmp_binary"; then
			log_error "Failed to install to $install_dir (sudo required)"
			exit 1
		fi
		sudo chmod +x "$tmp_binary"
		sudo mv -f "$tmp_binary" "$install_dir/${BIN_NAME}"
	fi

	if [[ "$(uname -s)" == "Darwin" ]]; then
		if command -v xattr &>/dev/null; then
			xattr -d com.apple.quarantine "$install_dir/${BIN_NAME}" 2>/dev/null || true
		fi
	fi

	log_success "${BIN_NAME} installed to $install_dir/${BIN_NAME}"

	if [[ ":$PATH:" != *":$install_dir:"* ]]; then
		log_warning "$install_dir is not in your PATH"
		echo ""
		echo "Add this to your shell profile (~/.bashrc, ~/.zshrc, etc.):"
		echo "  export PATH=\"\$PATH:$install_dir\""
		echo ""
	fi

	cd - >/dev/null || cd "$HOME"

	INSTALL_DIR="$install_dir"
}

verify_installation() {
	local install_dir="$1"

	if [ ! -x "$install_dir/${BIN_NAME}" ]; then
		log_error "${BIN_NAME} binary not found or not executable at $install_dir/${BIN_NAME}"
		exit 1
	fi

	if ! "$install_dir/${BIN_NAME}" --version &>/dev/null; then
		log_error "${BIN_NAME} binary exists but failed to run"
		exit 1
	fi
	local installed_sha installed_identity
	if command -v sha256sum &>/dev/null; then
		installed_sha=$(sha256sum "$install_dir/${BIN_NAME}" | awk '{print $1}')
	else
		installed_sha=$(shasum -a 256 "$install_dir/${BIN_NAME}" | awk '{print $1}')
	fi
	installed_identity=$("$install_dir/${BIN_NAME}" version --json)
	python3 -c 'import json,sys; d=json.loads(sys.argv[1]); expected=sys.argv[2].lower(); installed=sys.argv[3].lower(); ok=installed==expected and d.get("gitSha","").lower()==sys.argv[4].lower() and d.get("gitDirty") is False and d.get("binarySha256","").lower()==expected; raise SystemExit(0 if ok else 1)' "$installed_identity" "$EXPECTED_BINARY_SHA" "$installed_sha" "$EXPECTED_GIT_SHA" || {
		log_error "Installed binary identity does not match release provenance"
		exit 1
	}

	log_success "${BIN_NAME} is installed and ready!"
	echo ""
	"$install_dir/${BIN_NAME}" --version
	echo ""

	echo "Get started:"
	echo "  ${BIN_NAME} codex auth login    # authenticate with your ChatGPT account, or"
	echo "  ${BIN_NAME} grok auth login     # authenticate with your grok.com account"
	echo "  ${BIN_NAME} serve               # start the proxy"
	echo ""
	echo "Documentation: https://github.com/${REPO}"
	echo ""
}

main() {
	echo ""
	echo "${BIN_NAME} installer"
	echo ""

	log_info "Detecting platform..."
	local platform
	platform=$(detect_platform)
	log_info "Platform: $platform"

	install_from_release "$platform"

	verify_installation "$INSTALL_DIR"
}

main "$@"
