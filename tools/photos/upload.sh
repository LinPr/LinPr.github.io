#!/usr/bin/env bash
# 把本地照片上传到阿里云 OSS（bucket: linpr-github-io，目录 photography/），并打印 data/photos.yaml 条目。
#
# 用法：tools/photos/upload.sh <照片目录或文件...> [-p 子目录] [-a 相册]
#   -p  OSS 中 photography/ 下的子目录，例如 2026/，默认按当前年份
#   -a  相册名，会写进生成的条目
#
# 依赖：aws CLI（凭证读取 ~/.aws/credentials 的 [oss] profile）、Python + Pillow。
# 对象以 public-read 上传（bucket 本身私有，不能匿名列目录），上传原图即可，
# 缩略图和大图由 OSS 图片处理按需生成（见 hugo.yaml 的 params.photography）。
set -euo pipefail

BUCKET=linpr-github-io
ENDPOINT=https://oss-cn-hangzhou.aliyuncs.com
SUB="$(date +%Y)/"
ALBUM=""
FILES=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p) SUB="${2%/}/"; shift 2 ;;
    -a) ALBUM="$2"; shift 2 ;;
    *) FILES+=("$1"); shift ;;
  esac
done
[[ ${#FILES[@]} -gt 0 ]] || { sed -n '2,10p' "$0"; exit 1; }

# OSS 只支持虚拟主机风格访问，且不支持 aws CLI 新版默认的分块校验上传
CFG=$(mktemp); trap 'rm -f "$CFG"' EXIT
printf '[profile oss]\nregion = cn-hangzhou\ns3 =\n    addressing_style = virtual\n' > "$CFG"
oss() {
  AWS_CONFIG_FILE="$CFG" AWS_REQUEST_CHECKSUM_CALCULATION=when_required \
  AWS_RESPONSE_CHECKSUM_VALIDATION=when_required \
  aws --profile oss --endpoint-url "$ENDPOINT" "$@"
}

LIST=()
for f in "${FILES[@]}"; do
  if [[ -d "$f" ]]; then
    while IFS= read -r x; do LIST+=("$x"); done < <(find "$f" -maxdepth 1 -type f \( -iname '*.jpg' -o -iname '*.jpeg' -o -iname '*.png' -o -iname '*.webp' \) | sort)
  else
    LIST+=("$f")
  fi
done

for f in "${LIST[@]}"; do
  name=$(basename "$f")
  case "${name,,}" in
    *.png) ct=image/png ;; *.webp) ct=image/webp ;; *) ct=image/jpeg ;;
  esac
  oss s3api put-object --bucket "$BUCKET" --key "photography/${SUB}${name}" --body "$f" \
    --content-type "$ct" --acl public-read --cache-control "public, max-age=31536000" >/dev/null
  echo "uploaded photography/${SUB}${name}" >&2
done

python3 "$(dirname "$0")/photos_yaml.py" "${LIST[@]}" --prefix "$SUB" ${ALBUM:+--album "$ALBUM"}
