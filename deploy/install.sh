#!/usr/bin/env bash
#
# sq 安装脚本。
#
# 职责：把发布包里的二进制、配置样例与 systemd 单元装到 FHS 约定位置，
#       建专用系统用户与数据目录。
# 边界：不 enable、不 start——装完只打印下一步命令让用户自己执行。
#       安装脚本擅自起服务是不礼貌的，且在编排系统里会与其调度打架。
#       也不负责升级配置格式：已存在的 /etc/sq/sq.yaml 一律不覆盖。
#
# 必须以 root 运行。同目录下需有 sq、sq.example.yaml、sq.service。

set -euo pipefail

BIN_DST=/usr/local/bin/sq
CFG_DIR=/etc/sq
CFG_DST="${CFG_DIR}/sq.yaml"
DATA_DIR=/var/lib/sq
UNIT_DST=/etc/systemd/system/sq.service
SQ_USER=sq

SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log()  { printf '[install] %s\n' "$*"; }
fail() { printf '[install] 失败：%s\n' "$*" >&2; exit 1; }

log "开始安装 sq，来源目录 ${SRC_DIR}"

[[ ${EUID} -eq 0 ]] || fail "需要 root 权限（建用户、写 /usr/local/bin 与 /etc）"
command -v systemctl >/dev/null 2>&1 || fail "未找到 systemctl，本脚本只支持 systemd 系统"

for f in sq sq.example.yaml sq.service; do
  [[ -f "${SRC_DIR}/${f}" ]] || fail "发布包缺少 ${f}（期望在 ${SRC_DIR} 下）"
done

# 专用系统用户：--system 不建家目录、不可登录，降低被当作跳板的面。
if id -u "${SQ_USER}" >/dev/null 2>&1; then
  log "系统用户 ${SQ_USER} 已存在，跳过创建"
else
  useradd --system --no-create-home --shell /usr/sbin/nologin "${SQ_USER}" \
    || fail "创建系统用户 ${SQ_USER} 失败"
  log "已创建系统用户 ${SQ_USER}"
fi

install -d -m 0755 "${CFG_DIR}" || fail "创建 ${CFG_DIR} 失败"
install -d -m 0750 -o "${SQ_USER}" -g "${SQ_USER}" "${DATA_DIR}" || fail "创建 ${DATA_DIR} 失败"
log "目录就绪：${CFG_DIR}（配置）、${DATA_DIR}（数据，属主 ${SQ_USER}）"

install -m 0755 "${SRC_DIR}/sq" "${BIN_DST}" || fail "安装二进制到 ${BIN_DST} 失败"
log "二进制已安装：${BIN_DST}"

# 已存在的配置一律不覆盖——升级不能吃掉用户改过的配置。
if [[ -f "${CFG_DST}" ]]; then
  log "配置 ${CFG_DST} 已存在，保留不动（样例见 ${SRC_DIR}/sq.example.yaml）"
else
  install -m 0644 "${SRC_DIR}/sq.example.yaml" "${CFG_DST}" || fail "写入 ${CFG_DST} 失败"
  log "已写入默认配置：${CFG_DST}"
fi

install -m 0644 "${SRC_DIR}/sq.service" "${UNIT_DST}" || fail "安装单元到 ${UNIT_DST} 失败"
systemctl daemon-reload || fail "systemctl daemon-reload 失败"
log "systemd 单元已安装并 daemon-reload：${UNIT_DST}"

# 装完自证：版本打得出来说明二进制可执行且架构没装错。
installed_version="$("${BIN_DST}" --version 2>&1 | head -1)" || fail "${BIN_DST} --version 执行失败，二进制可能与本机架构不符"
log "安装完成，版本：${installed_version}"

cat <<'NEXT'
[install] 下一步（本脚本刻意不自动启动）：
[install]   1. 按需编辑 /etc/sq/sq.yaml（注意把 data_dir 设为 /var/lib/sq）
[install]   2. systemctl enable --now sq
[install]   3. systemctl status sq
NEXT
