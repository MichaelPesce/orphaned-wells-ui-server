"""
whitespace_detector.py

Detects the percentage of "non-ink" (near-white) pixels in an image.
Supports configurable thresholds for both strictness and target percentage.
"""

from PIL import Image
import numpy as np


def detect_whitespace(
    image_path: str,
    threshold: int = 240,
    min_whitespace_pct: float = None,
    channel_mode: str = "all",
) -> dict:
    """
    Detect the percentage of near-white (non-ink) pixels in an image.

    Parameters
    ----------
    image_path : str
        Path to the image file. Supports JPEG, PNG, TIFF, BMP, etc.

    threshold : int (0–255), default 240
        Strictness of what counts as "white/non-ink".
        Higher = stricter (only very bright pixels qualify).
        Lower = more lenient (includes off-white, light grey, cream, etc.).

        Suggested presets:
          255         — Pure white only (extremely strict)
          245–254     — Very strict: near-pure white
          230–244     — Strict: bright white + very light grey  ← default ~240
          200–229     — Moderate: includes light grey, ivory
          150–199     — Lax: light colors, washed-out backgrounds
          <150        — Very lax: mid-tones also count as "non-ink"

    min_whitespace_pct : float or None, default None
        If provided, the function will also return a boolean `meets_threshold`
        indicating whether the detected whitespace percentage >= this value.
        Example: pass 10.0 to check if at least 10% of the image is whitespace.

    channel_mode : str, default "all"
        How to evaluate each pixel across R, G, B channels:
          "all"  — ALL channels must be >= threshold (strict: only grey/white tones)
          "any"  — ANY channel >= threshold (lenient: catches near-white in one channel)
          "mean" — The MEAN of channels must be >= threshold (balanced)
          "luma" — Uses perceptual luminance (0.299R + 0.587G + 0.114B) >= threshold

    Returns
    -------
    dict with keys:
        whitespace_pct   : float  — Percentage of pixels classified as non-ink (0–100)
        ink_pct          : float  — Percentage of pixels classified as ink (0–100)
        total_pixels     : int    — Total pixel count
        white_pixels     : int    — Count of non-ink pixels
        threshold        : int    — The threshold used
        channel_mode     : str    — The channel mode used
        meets_threshold  : bool   — (only if min_whitespace_pct is provided)
        min_whitespace_pct: float — (only if min_whitespace_pct is provided)

    Examples
    --------
    # Basic usage — default settings
    result = detect_whitespace("scan.png")
    print(f"{result['whitespace_pct']:.1f}% non-ink")

    # Strict: only near-pure white counts, check if >= 5%
    result = detect_whitespace("scan.png", threshold=250, min_whitespace_pct=5.0)

    # Lax: light greys + cream count, check if >= 10%
    result = detect_whitespace("scan.png", threshold=200, min_whitespace_pct=10.0)

    # Use perceptual luminance for more visually accurate results
    result = detect_whitespace("scan.png", threshold=235, channel_mode="luma")
    """
    img = Image.open(image_path).convert("RGB")
    pixels = np.array(img, dtype=np.float32)  # shape: (H, W, 3)

    r, g, b = pixels[:, :, 0], pixels[:, :, 1], pixels[:, :, 2]

    if channel_mode == "all":
        mask = (r >= threshold) & (g >= threshold) & (b >= threshold)
    elif channel_mode == "any":
        mask = (r >= threshold) | (g >= threshold) | (b >= threshold)
    elif channel_mode == "mean":
        mean = (r + g + b) / 3.0
        mask = mean >= threshold
    elif channel_mode == "luma":
        luma = 0.299 * r + 0.587 * g + 0.114 * b
        mask = luma >= threshold
    else:
        raise ValueError(
            f"Invalid channel_mode '{channel_mode}'. "
            "Choose from: 'all', 'any', 'mean', 'luma'."
        )

    total_pixels = pixels.shape[0] * pixels.shape[1]
    white_pixels = int(np.sum(mask))
    whitespace_pct = (white_pixels / total_pixels) * 100.0

    result = {
        "whitespace_pct": round(whitespace_pct, 4),
        "ink_pct": round(100.0 - whitespace_pct, 4),
        "total_pixels": total_pixels,
        "white_pixels": white_pixels,
        "threshold": threshold,
        "channel_mode": channel_mode,
    }

    if min_whitespace_pct is not None:
        result["min_whitespace_pct"] = min_whitespace_pct
        result["meets_threshold"] = whitespace_pct >= min_whitespace_pct

    return result


# ---------------------------------------------------------------------------
# Convenience wrappers for common use-cases
# ---------------------------------------------------------------------------

def is_mostly_whitespace(
    image_path: str,
    min_whitespace_pct: float = 50.0,
    threshold: int = 240,
    channel_mode: str = "all",
) -> bool:
    """Return True if the image has >= min_whitespace_pct non-ink pixels."""
    print(f"min whitespace: {min_whitespace_pct}")
    result = detect_whitespace(
        image_path,
        threshold=threshold,
        min_whitespace_pct=min_whitespace_pct,
        channel_mode=channel_mode,
    )
    return result["meets_threshold"]


def whitespace_pct(
    image_path: str,
    threshold: int = 240,
    channel_mode: str = "luma",
) -> float:
    """Return just the whitespace percentage as a float."""
    return detect_whitespace(image_path, threshold=threshold, channel_mode=channel_mode)[
        "whitespace_pct"
    ]


# ---------------------------------------------------------------------------
# CLI demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python whitespace_detector.py <image_path> [threshold] [min_pct]")
        sys.exit(1)

    path = sys.argv[1]
    thr = int(sys.argv[2]) if len(sys.argv) > 2 else 240
    min_pct = float(sys.argv[3]) if len(sys.argv) > 3 else None

    for mode in ("all", "mean", "luma"):
        r = detect_whitespace(path, threshold=thr, min_whitespace_pct=min_pct, channel_mode=mode)
        meets = f"  ✓ meets {min_pct}%" if min_pct and r.get("meets_threshold") else (
                f"  ✗ below {min_pct}%" if min_pct else ""
        )
        print(f"[{mode:4s}] whitespace={r['whitespace_pct']:6.2f}%  ink={r['ink_pct']:6.2f}%{meets}")