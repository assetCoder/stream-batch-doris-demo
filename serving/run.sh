#!/usr/bin/env bash
# 启动开发服务器；如果本机默认 Java 不是 21，会自动切到 21。
set -euo pipefail
cd "$(dirname "$0")"

if [ -z "${JAVA_HOME:-}" ]; then
  if [ -x /usr/libexec/java_home ]; then
    JAVA_HOME="$(/usr/libexec/java_home -v 21 2>/dev/null || true)"
    export JAVA_HOME
  fi
fi

exec mvn spring-boot:run "$@"
