#!/usr/bin/env bash
#
# sq 一键安装脚本。
#
# 职责：取包（本地或下载）→ 生成 /etc/sq/sq.yaml → 委托 install.sh 装盘
#       → 收紧配置权限 → 打印下一步。单机与三节点集群都走这一个脚本。
# 边界：不启动服务（与 install.sh 同源的理由：安装器擅自起服务在编排系统里
#       会与其调度打架）；不 SSH、不分发、不做多机编排——三节点是三台各跑
#       一次本脚本，只差 --node-id；不实现装盘逻辑，那是 install.sh 的事。
#
# 与 install.sh 的衔接（承重，不要改顺序）：本脚本**先写配置、再调
# install.sh**。install.sh 对已存在的 /etc/sq/sq.yaml 一律不覆盖，走到那个
# 分支自然让路。反过来会让 install.sh 先铺一份 sq.example.yaml 的副本、
# 本脚本再去覆盖它，「不覆盖」那条保护就形同虚设。
#
# 必须以 root 运行，且只支持带 systemd 的 Linux。
#
# 可测性：末尾的 main 入口被 BASH_SOURCE 守卫包住，测试脚本 source 本文件
# 即可单独调用其中的函数而不触发安装。全部写盘路径都是可覆盖变量。

set -euo pipefail

# 以下全局变量是后续安装阶段与测试脚本消费的接口；骨架阶段先显式触碰它们，
# 让 shellcheck 知道这些是有意保留的接口而不是漏写的局部变量。

# —— 路径约定（与 deploy/install.sh 必须一致，改一处就要改两处）——
# 用 := 形式让测试把它们重定向到临时目录。
: "${SQ_CFG_DIR:=/etc/sq}"
: "${SQ_DATA_DIR:=/var/lib/sq}"
: "${SQ_BIN_DST:=/usr/local/bin/sq}"
: "${SQ_UNIT_DST:=/etc/systemd/system/sq.service}"
: "${SQ_USER:=sq}"

CFG_DST="${SQ_CFG_DIR}/sq.yaml"
EXAMPLE_DST="${SQ_CFG_DIR}/sq.example.yaml"

# —— 写死的端口（spec §2「不做」：不提供端口 flag）——
# 三台机器端口不一致会静默凑出一个连不上的集群，而脚本看不到另两台、
# 无从校验。端口被占的用户自行改 /etc/sq/sq.yaml 后重启。
GRPC_PORT=8081
ADMIN_PORT=8082
RAFT_PORT=9081

: "${CFG_DST}" "${EXAMPLE_DST}" "${GRPC_PORT}" "${ADMIN_PORT}" "${RAFT_PORT}"

# —— 下载来源 ——
: "${SQ_REPO:=Xsxdot/sq}"
: "${SQ_RELEASE_API:=https://api.github.com/repos/${SQ_REPO}/releases/latest}"
: "${SQ_DOWNLOAD_BASE:=https://github.com/${SQ_REPO}/releases/download}"

# log/warn/fail 是本脚本的日志机制：stdout 就是安装器的界面。
# 统一前缀是为了让用户能把安装输出与系统其他输出区分开；fail 一律写
# stderr 并退出非零，绝不静默失败。
log()  { printf '[quickstart] %s\n' "$*"; }
warn() { printf '[quickstart] ⚠ %s\n' "$*" >&2; }
fail() { printf '[quickstart] 失败：%s\n' "$*" >&2; exit 1; }

# —— 参数（由 parse_args 填充）——
OPT_CLUSTER=0
OPT_NODE_ID=""
OPT_PEERS=()
OPT_ADVERTISE_HOST=""
OPT_ADMIN_USER=""
OPT_ADMIN_PASS=""
OPT_NO_ADMIN_AUTH=0
OPT_VERSION=""
OPT_TARBALL=""
OPT_FORCE=0

: "${OPT_ADVERTISE_HOST}" "${OPT_VERSION}" "${OPT_TARBALL}" "${OPT_FORCE}"

usage() {
  cat <<'USAGE'
用法：quickstart.sh [选项]

单机：
  sudo ./quickstart.sh

三节点（三台机器各跑一次，只差 --node-id）：
  sudo ./quickstart.sh --cluster --node-id 1 --peers 10.0.0.1,10.0.0.2,10.0.0.3

选项：
  --cluster                 集群档（不给 = 单机档）
  --node-id N               本机是 peers 里的第几个，集群档必填
  --peers ip1,ip2,ip3       全体成员地址，顺序即 node id，集群档必填
  --advertise-host HOST     对外地址；单机档默认 127.0.0.1，集群档自动取 peers[node-id-1]
  --admin-user U            控制台用户名，默认 admin
  --admin-password P        控制台密码；不给则自动生成
  --no-admin-auth           显式关闭管理面鉴权（不推荐）
  --version X.Y.Z           指定下载版本，默认拉最新 release
  --tarball PATH|URL        直接指定发布包，绕开 GitHub
  --force                   已有配置时备份后覆盖（从不碰数据目录）
  -h, --help                显示本帮助

端口固定为 gRPC 8081 / 控制台 8082 / raft 9081，需要改请装完后编辑
/etc/sq/sq.yaml 再重启。
USAGE
}

# parse_args 解析命令行到 OPT_* 全局变量。
#
# 参数：全部命令行参数
# 注意：只做解析不做校验——校验集中在 validate_args，便于单独测试，
# 也保证「任何盘都没动之前，所有参数问题都已暴露」。
parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --cluster)          OPT_CLUSTER=1; shift ;;
      --node-id)          OPT_NODE_ID="${2:-}"; shift 2 ;;
      --peers)            IFS=',' read -r -a OPT_PEERS <<< "${2:-}"; shift 2 ;;
      --advertise-host)   OPT_ADVERTISE_HOST="${2:-}"; shift 2 ;;
      --admin-user)       OPT_ADMIN_USER="${2:-}"; shift 2 ;;
      --admin-password)   OPT_ADMIN_PASS="${2:-}"; shift 2 ;;
      --no-admin-auth)    OPT_NO_ADMIN_AUTH=1; shift ;;
      --version)          OPT_VERSION="${2:-}"; shift 2 ;;
      --tarball)          OPT_TARBALL="${2:-}"; shift 2 ;;
      --force)            OPT_FORCE=1; shift ;;
      -h|--help)          usage; exit 0 ;;
      *)                  fail "未知参数 $1（用 --help 看用法）" ;;
    esac
  done
}

# validate_args 校验参数自洽性。
#
# 全部校验在动任何盘之前完成：安装到一半才发现参数错，现场比一开始就
# 拒绝难收拾得多。
validate_args() {
  if [[ ${OPT_CLUSTER} -eq 1 ]]; then
    [[ -n "${OPT_NODE_ID}" ]] || fail "--cluster 需要 --node-id"
    [[ ${#OPT_PEERS[@]} -ge 1 ]] || fail "--cluster 需要 --peers（形如 --peers 10.0.0.1,10.0.0.2,10.0.0.3）"
    [[ "${OPT_NODE_ID}" =~ ^[0-9]+$ ]] || fail "--node-id 必须是正整数，得到 ${OPT_NODE_ID}"
    if [[ "${OPT_NODE_ID}" -lt 1 || "${OPT_NODE_ID}" -gt ${#OPT_PEERS[@]} ]]; then
      fail "--node-id 须落在 1..${#OPT_PEERS[@]}（--peers 给了 ${#OPT_PEERS[@]} 个地址），得到 ${OPT_NODE_ID}"
    fi
    # 重复地址必然是复制粘贴错误：raft 成员表要求各成员地址唯一，
    # 重复会让两个 id 指向同一台机器，集群永远选不出稳定的 leader
    local uniq
    uniq="$(printf '%s\n' "${OPT_PEERS[@]}" | sort -u | wc -l | tr -d ' ')"
    [[ "${uniq}" -eq ${#OPT_PEERS[@]} ]] || fail "--peers 里有重复地址：${OPT_PEERS[*]}"
    # 偶数节点无容错价值（2 节点任一挂即失 quorum），与 config.go 同款：
    # 警告但不拒绝，留给运维判断
    if [[ ${#OPT_PEERS[@]} -gt 1 && $(( ${#OPT_PEERS[@]} % 2 )) -eq 0 ]]; then
      warn "集群节点数为偶数（${#OPT_PEERS[@]}），无容错价值，建议奇数"
    fi
  else
    [[ -z "${OPT_NODE_ID}" ]] || fail "--node-id 只在 --cluster 时有意义（是不是漏了 --cluster？）"
    [[ ${#OPT_PEERS[@]} -eq 0 ]] || fail "--peers 只在 --cluster 时有意义（是不是漏了 --cluster？）"
  fi

  if [[ ${OPT_NO_ADMIN_AUTH} -eq 1 ]]; then
    [[ -z "${OPT_ADMIN_USER}" && -z "${OPT_ADMIN_PASS}" ]] \
      || fail "--no-admin-auth 与 --admin-user/--admin-password 互斥：要么关鉴权，要么设凭据"
  fi
}

# —— 凭据与地址 ——
CRED_USER=""
CRED_PASS=""
CRED_GENERATED=0
ADVERTISE_HOST=""

# gen_password 生成一个 24 位随机口令（大小写字母 + 数字）写 stdout。
#
# 用 /dev/urandom 而不是 $RANDOM：后者是 16 位 LCG，不是密码学随机源。
# 不用 openssl rand：openssl 不是所有精简发行版都装。
# LC_ALL=C 是必须的——某些 locale 下 tr 的字符类会按多字节解释而吐出乱码。
gen_password() {
  local password=""
  local chunk
  while [[ ${#password} -lt 24 ]]; do
    if ! chunk="$(head -c 128 /dev/urandom | LC_ALL=C tr -dc 'A-Za-z0-9')"; then
      fail "生成管理面口令失败（读取 /dev/urandom 失败）"
    fi
    password+="${chunk}"
  done
  printf '%s' "${password:0:24}"
}

# resolve_credentials 定下最终的管理面凭据，设置 CRED_USER/CRED_PASS/CRED_GENERATED。
#
# 默认生成、不默认敞开：一键脚本默默开一个无鉴权管理面是不可接受的默认值。
# 要免鉴权必须显式 --no-admin-auth。
resolve_credentials() {
  if [[ ${OPT_NO_ADMIN_AUTH} -eq 1 ]]; then
    log "按 --no-admin-auth 关闭管理面鉴权"
    return 0
  fi
  CRED_USER="${OPT_ADMIN_USER:-admin}"
  if [[ -n "${OPT_ADMIN_PASS}" ]]; then
    CRED_PASS="${OPT_ADMIN_PASS}"
    log "使用显式传入的管理面凭据（用户名 ${CRED_USER}）"
  else
    CRED_PASS="$(gen_password)"
    CRED_GENERATED=1
    log "已自动生成管理面口令（用户名 ${CRED_USER}）"
  fi
  [[ -n "${CRED_PASS}" ]] || fail "生成管理面口令失败（/dev/urandom 不可读？）"
  : "${CRED_GENERATED}"
}

# resolve_advertise_host 定下本机对外地址，设置 ADVERTISE_HOST。
#
# 集群档自动取 peers[node-id-1]——成员表里已经写着本机地址，再让用户
# 传一次只会制造不一致。单机档默认 127.0.0.1（与进程内默认一致），
# 此时远程客户端连不上，必须显式警告：这是最常见的「装完连不上」原因。
resolve_advertise_host() {
  if [[ -n "${OPT_ADVERTISE_HOST}" ]]; then
    ADVERTISE_HOST="${OPT_ADVERTISE_HOST}"
    return 0
  fi
  if [[ ${OPT_CLUSTER} -eq 1 ]]; then
    ADVERTISE_HOST="${OPT_PEERS[$(( OPT_NODE_ID - 1 ))]}"
    return 0
  fi
  ADVERTISE_HOST="127.0.0.1"
  warn "advertise_host 取默认值 127.0.0.1：远程客户端将连不上本机。需要远程访问请加 --advertise-host <本机对外地址>"
}

# render_config 把完整的 sq.yaml 写到 stdout。
#
# 只写与本机部署形态相关的字段——进程内默认值（internal/config/config.go
# 的 Load）与 sq.example.yaml 逐字段一致，省略的字段行为完全可预测。
# 抄那份 100 行注释版会让「脚本决定的」和「默认值」混成一片，出问题时
# 看不出这台机器的身份是什么。
render_config() {
  local shape="单机"
  [[ ${OPT_CLUSTER} -eq 1 ]] && shape="集群，本机 node_id=${OPT_NODE_ID}"
  printf '# 本文件由 sq quickstart.sh 生成，形态：%s\n' "${shape}"
  printf '# 未列出的字段走进程内默认值，字段说明见 %s\n' "${EXAMPLE_DST}"
  printf 'data_dir: "%s"\n' "${SQ_DATA_DIR}"
  printf 'advertise_host: "%s"\n' "${ADVERTISE_HOST}"
  if [[ ${OPT_NO_ADMIN_AUTH} -ne 1 ]]; then
    printf 'admin_username: "%s"\n' "${CRED_USER}"
    printf 'admin_password: "%s"\n' "${CRED_PASS}"
  fi
  [[ ${OPT_CLUSTER} -eq 1 ]] || return 0
  printf 'cluster:\n'
  printf '  node_id: %s\n' "${OPT_NODE_ID}"
  printf '  raft_listen: ":%s"\n' "${RAFT_PORT}"
  printf '  data_groups: 3\n'
  printf '  ack: quorum-mem\n'
  printf '  peers:\n'
  local i host
  for i in "${!OPT_PEERS[@]}"; do
    host="${OPT_PEERS[$i]}"
    # admin_port 显式写上而不是留空：留空会走「回落取本机端口」的兼容
    # 路径，那条路径隐含各节点端口一致的假设。生成的配置没有理由依赖假设。
    printf '    - { id: %d, raft_addr: "%s:%s", advertise_host: "%s", advertise_port: %s, admin_port: %s }\n' \
      "$(( i + 1 ))" "${host}" "${RAFT_PORT}" "${host}" "${GRPC_PORT}" "${ADMIN_PORT}"
  done
}

# main 是完整安装流程。Task 5/6 会逐步填充它。
main() {
  parse_args "$@"
  validate_args
  log "参数校验通过"
  resolve_credentials
  resolve_advertise_host
  log "即将写入的配置："
  render_config | sed 's/^/[quickstart]   /'
}

# 守卫：被 source 时不执行安装，只导出函数供测试调用。
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  main "$@"
fi
