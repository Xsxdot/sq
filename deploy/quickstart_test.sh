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

echo "== 凭据 =="

# 生成的口令：24 位，只含大小写字母与数字
gen_out="$( ( # shellcheck source=/dev/null
  source "${HERE}/quickstart.sh" >/dev/null 2>&1
  gen_password ) )"
check "生成口令长度为 24" "${#gen_out}" "24"
if [[ "${gen_out}" =~ ^[A-Za-z0-9]{24}$ ]]; then ok "生成口令字符集合法"; else bad "生成口令含非法字符：${gen_out}"; fi

# 两次生成必须不同（随机源真的在随机）
gen2="$( ( # shellcheck source=/dev/null
  source "${HERE}/quickstart.sh" >/dev/null 2>&1
  gen_password ) )"
if [[ "${gen_out}" != "${gen2}" ]]; then ok "两次生成的口令不同"; else bad "两次生成的口令相同，随机源可疑"; fi

echo "== 配置生成 =="

# 用固定的口令生成器桩，让配置文本可逐字比对
render() {
  ( # shellcheck source=/dev/null
    source "${HERE}/quickstart.sh" >/dev/null 2>&1
    # shellcheck disable=SC2317 # 由被测 resolve_credentials 间接调用
    gen_password() { printf 'FIXEDPASSWORD0123456789A'; }
    parse_args "$@" && validate_args && resolve_credentials && resolve_advertise_host && render_config
  )
}

single="$(render)"
for want in 'data_dir: "/var/lib/sq"' 'advertise_host: "127.0.0.1"' 'admin_username: "admin"' 'admin_password: "FIXEDPASSWORD0123456789A"'; do
  if grep -qF "${want}" <<< "${single}"; then ok "单机配置含 ${want}"; else bad "单机配置缺 ${want}：\n${single}"; fi
done
if grep -q '^cluster:' <<< "${single}"; then bad "单机配置不应有 cluster 段"; else ok "单机配置无 cluster 段"; fi

cl="$(render --cluster --node-id 2 --peers 10.0.0.1,10.0.0.2,10.0.0.3)"
for want in 'advertise_host: "10.0.0.2"' 'node_id: 2' 'raft_listen: ":9081"' 'data_groups: 3' 'ack: quorum-mem' \
            '{ id: 1, raft_addr: "10.0.0.1:9081", advertise_host: "10.0.0.1", advertise_port: 8081, admin_port: 8082 }' \
            '{ id: 3, raft_addr: "10.0.0.3:9081", advertise_host: "10.0.0.3", advertise_port: 8081, admin_port: 8082 }'; do
  if grep -qF "${want}" <<< "${cl}"; then ok "集群配置含 ${want}"; else bad "集群配置缺 ${want}：\n${cl}"; fi
done

noauth="$(render --no-admin-auth)"
if grep -q 'admin_password' <<< "${noauth}"; then bad "--no-admin-auth 时不应出现 admin_password"; else ok "--no-admin-auth 时无凭据行"; fi

explicit="$(render --admin-user ops --admin-password 'sekrit')"
for want in 'admin_username: "ops"' 'admin_password: "sekrit"'; do
  if grep -qF "${want}" <<< "${explicit}"; then ok "显式凭据原样落盘：${want}"; else bad "显式凭据未落盘：${want}"; fi
done

echo
printf '通过 %d，失败 %d\n' "${PASS}" "${FAIL}"
[[ "${FAIL}" -eq 0 ]]
