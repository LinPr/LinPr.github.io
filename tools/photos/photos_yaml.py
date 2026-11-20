"""从本地照片读取 EXIF，生成 data/photos.yaml 里的条目。

用法：
    python3 tools/photos/photos_yaml.py <照片目录或文件...> [--prefix 2026/] [--album 城市]

- --prefix：这些照片上传到 OSS 后所在的目录（对象路径前缀），例如 "2026/"；
- --album：给这批照片统一设置相册名；
- 输出打印到标准输出，检查、补上 title/location/desc 后粘贴到 data/photos.yaml 的 photos: 下面。
  别忘了把 data/photos.yaml 末尾的 `photos: []` 改成 `photos:`。

依赖 Pillow（pip install Pillow）。只读文件，不会修改照片，也不会上传。
"""
import argparse
import os
import sys

try:
    from PIL import Image
    from PIL.ExifTags import TAGS
except ImportError:  # pragma: no cover
    sys.exit("需要 Pillow：pip install Pillow")

EXTS = {".jpg", ".jpeg", ".png", ".webp", ".tif", ".tiff"}


def exif_of(path):
    with Image.open(path) as im:
        w, h = im.size
        raw = {}
        try:
            data = im._getexif() or {}
            raw = {TAGS.get(k, k): v for k, v in data.items()}
        except Exception:
            pass
    # 竖拍照片：EXIF Orientation 为 6/8 时宽高互换
    if raw.get("Orientation") in (5, 6, 7, 8):
        w, h = h, w
    return w, h, raw


def num(v):
    try:
        if isinstance(v, tuple):
            return v[0] / v[1] if v[1] else 0
        return float(v)
    except Exception:
        return 0


def shutter(v):
    t = num(v)
    if not t:
        return ""
    return f"1/{round(1 / t)}s" if t < 1 else f"{t:g}s"


def q(s):
    s = str(s).strip().replace('"', "'")
    return f'"{s}"'


def entry(path, prefix, album):
    w, h, e = exif_of(path)
    name = os.path.basename(path)
    lines = [f"- src: {prefix}{name}", f"  title: {q(os.path.splitext(name)[0])}"]
    dt = str(e.get("DateTimeOriginal") or e.get("DateTime") or "")
    if len(dt) >= 10:
        lines.append(f"  date: {dt[:10].replace(':', '-')}")
    if album:
        lines.append(f"  album: {q(album)}")
    cam = " ".join(str(e.get(k, "")).strip() for k in ("Make", "Model")).strip()
    lens = str(e.get("LensModel", "")).strip()
    if cam or lens:
        lines.append(f"  camera: {q(' · '.join(x for x in (cam, lens) if x))}")
    parts = []
    if num(e.get("FocalLength")):
        parts.append(f"{num(e['FocalLength']):g}mm")
    if num(e.get("FNumber")):
        parts.append(f"f/{num(e['FNumber']):g}")
    if e.get("ExposureTime"):
        parts.append(shutter(e["ExposureTime"]))
    iso = e.get("ISOSpeedRatings") or e.get("PhotographicSensitivity")
    if iso:
        parts.append(f"ISO {iso[0] if isinstance(iso, tuple) else iso}")
    if parts:
        lines.append(f"  exif: {q(' · '.join(parts))}")
    lines += [f"  width: {w}", f"  height: {h}"]
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--prefix", default="")
    ap.add_argument("--album", default="")
    a = ap.parse_args()
    files = []
    for p in a.paths:
        if os.path.isdir(p):
            files += [os.path.join(p, f) for f in sorted(os.listdir(p)) if os.path.splitext(f)[1].lower() in EXTS]
        else:
            files.append(p)
    for f in files:
        print(entry(f, a.prefix, a.album))


if __name__ == "__main__":
    main()
