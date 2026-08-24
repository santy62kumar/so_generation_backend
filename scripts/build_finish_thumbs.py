"""
scripts/build_finish_thumbs.py
-----------------------
Regenerates small WebP thumbnails for the color-finish catalogs from full
resolution master images. Run this once per new catalog image, not at
request time.

    pip install pillow

Usage:
    python scripts/build_finish_thumbs.py <masters_dir> <category> <out_dir>

Example:
    python scripts/build_finish_thumbs.py ./masters/cabinet cabinet ./frontend/public/finishes/cabinet

On the sample set used for this project (5 cabinet + 23 glass masters,
~19MB total) this drops total size to ~48KB (99.7% smaller) at 160x160,
quality 75 — plenty for a dropdown swatch. Bump `SIZE`/`QUALITY` if the
color-family PDF slide ever needs to show a bigger preview.
"""
import sys
import re
from pathlib import Path
from PIL import Image

SIZE = 160
QUALITY = 75


def slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def make_thumb(src: Path, dst: Path, size: int = SIZE, quality: int = QUALITY) -> int:
    im = Image.open(src).convert("RGB")
    w, h = im.size
    m = min(w, h)
    im = im.crop(((w - m) // 2, (h - m) // 2, (w - m) // 2 + m, (h - m) // 2 + m))
    im = im.resize((size, size), Image.LANCZOS)
    dst.parent.mkdir(parents=True, exist_ok=True)
    im.save(dst, "WEBP", quality=quality, method=6)
    return dst.stat().st_size


def main():
    if len(sys.argv) != 4:
        print(__doc__)
        sys.exit(1)

    masters_dir, category, out_dir = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3])
    total_before = total_after = 0

    for src in sorted(masters_dir.glob("*.*")):
        if src.suffix.lower() not in (".jpg", ".jpeg", ".png"):
            continue
        # Strip a leading "12_" ordering prefix if present, then slugify.
        raw_name = re.sub(r"^\d+_", "", src.stem)
        color_id = slugify(raw_name)
        dst = out_dir / f"{color_id}.webp"
        before = src.stat().st_size
        after = make_thumb(src, dst)
        total_before += before
        total_after += after
        print(f"  {src.name:50s} -> {dst.name}  ({before/1024:.0f}KB -> {after/1024:.1f}KB)")

    print(f"\n[{category}] {total_before/1024:.0f}KB -> {total_after/1024:.0f}KB "
          f"({100*(1-total_after/max(total_before,1)):.1f}% smaller)")


if __name__ == "__main__":
    main()
