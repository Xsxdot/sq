#!/usr/bin/env bash
#
# quickstart.sh 的测试脚本。
#
# 职责：source 被测脚本、逐个调用其函数、断言行为。
# 边界：不需要 root、不写 /etc 与 /var（全部路径重定向到临时目录），
#       不联网（取包分支用 file:// 打桩）。
#
# 用法：./deploy/quickstart_test.sh

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PASS=0
FAIL=0

ok()   { PASS=$((PASS + 1)); printf '  ✓ %s\n' "$*"; }
bad()  { FAIL=$((FAIL + 1)); printf '  ✗ %s\n' "$*" >&2; }
check() {
  if [[ "$2" == "$3" ]]; then
    ok "$1"
  else
    bad "$1（期望 %s，得到 %s）"
    printf '     期望: %s\n     实际: %s\n' "$3" "$2" >&2
  fi
}

# 在子 shell 里跑一次被测函数，回显退出码。source 后再调，避免执行 main。
run_in_sub() {
  ( set +e
    # shellcheck source=/dev/null
    source "${HERE}/quickstart.sh" >/dev/null 2>&1
    "$@" >/dev/null 2>&1
    echo $?
  )
}

echo "== 参数解析 =="

# 单机档：零参数应当通过校验
check "零参数（单机档）通过校验" "$(run_in_sub eval 'parse_args && validate_args')" "0"

echo "== 参数校验的拒绝路径 =="

expect_reject() {
  local desc="$1"
  shift
  local code
  code="$( ( set +e
    # shellcheck source=/dev/null
    source "${HERE}/quickstart.sh" >/dev/null 2>&1
    parse_args "$@" >/dev/null 2>&1 && validate_args >/dev/null 2>&1
    echo $?
  ) )"
  if [[ "${code}" != "0" ]]; then ok "${desc}"; else bad "${desc}：应当被拒绝却通过了"; fi
}

expect_reject "--cluster 缺 --node-id"        --cluster --peers 10.0.0.1,10.0.0.2,10.0.0.3
expect_reject "--cluster 缺 --peers"          --cluster --node-id 1
expect_reject "--node-id 越界（0）"            --cluster --node-id 0 --peers 10.0.0.1,10.0.0.2,10.0.0.3
expect_reject "--node-id 越界（4/3 台）"       --cluster --node-id 4 --peers 10.0.0.1,10.0.0.2,10.0.0.3
expect_reject "--peers 有重复地址"             --cluster --node-id 1 --peers 10.0.0.1,10.0.0.1,10.0.0.3
expect_reject "非集群档却给了 --node-id"       --node-id 1
expect_reject "非集群档却给了 --peers"         --peers 10.0.0.1
expect_reject "--no-admin-auth 与 --admin-password 同给" --no-admin-auth --admin-password pw
expect_reject "未知参数"                       --bogus

echo
printf '通过 %d，失败 %d\n' "${PASS}" "${FAIL}"
[[ "${FAIL}" -eq 0 ]]
