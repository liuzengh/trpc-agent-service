"""Create deterministic, public-safe acceptance screenshots.

The script only crops and paints fixed rectangles. It never uses generative
editing, OCR, or the network. Raw captures stay in the OS temporary directory;
only the cropped/redacted outputs are written under docs/demo-evidence.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from PIL import Image, ImageDraw


WHITE = (255, 255, 255)
REPLY = (242, 243, 245)


def open_rgb(path: Path) -> Image.Image:
    return Image.open(path).convert("RGB")


def stack(parts: list[Image.Image], *, gap: int = 8, background=WHITE) -> Image.Image:
    width = max(part.width for part in parts)
    height = sum(part.height for part in parts) + gap * (len(parts) - 1)
    result = Image.new("RGB", (width, height), background)
    y = 0
    for part in parts:
        result.paste(part, (0, y))
        y += part.height + gap
    return result


def redact_feishu_private(source: Path) -> Image.Image:
    image = open_rgb(source)
    header = image.crop((640, 0, 1275, 100))
    body = image.crop((696, 150, 1275, 650))
    draw = ImageDraw.Draw(body)
    # Remove the user's display name from Feishu's quoted-reply headers.
    for top in (30, 205, 390):
        draw.rectangle((5, top, 178, top + 30), fill=REPLY)
    return stack([header, body])


def redact_feishu_group(source: Path) -> Image.Image:
    image = open_rgb(source)
    header = image.crop((640, 0, 1275, 92))
    first = image.crop((695, 225, 1130, 430))
    second = image.crop((695, 455, 1130, 660))
    for part in (first, second):
        draw = ImageDraw.Draw(part)
        # Remove the user's display name from the bot's quoted reply.
        draw.rectangle((8, 126, 150, 154), fill=REPLY)
    return stack([header, first, second])


def redact_wecom_private(source: Path) -> Image.Image:
    image = open_rgb(source)
    parts = [
        image.crop((326, 18, 950, 83)),
        # Tight bubble crops exclude the enterprise watermark layer and the
        # user's display-name rows while preserving the exact messages.
        image.crop((342, 359, 940, 394)),
        image.crop((342, 437, 660, 470)),
        image.crop((342, 510, 630, 544)),
        image.crop((342, 580, 525, 623)),
    ]
    return stack(parts)


def redact_wecom_group(source: Path) -> Image.Image:
    image = open_rgb(source)
    header = image.crop((326, 18, 720, 83))
    request = image.crop((342, 408, 632, 443))
    bot_label = image.crop((342, 454, 430, 478))
    result = image.crop((342, 548, 690, 625))
    return stack([header, request, bot_label, result])


def crop_jaeger(source: Path) -> Image.Image:
    image = open_rgb(source)
    return image.crop((0, 0, image.width, min(520, image.height)))


def save(image: Image.Image, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    image.save(output, format="PNG", optimize=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--temp-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    sources = args.temp_dir
    output = args.output_dir
    save(
        redact_feishu_private(sources / "feishu-private-acceptance-raw.png"),
        output / "feishu-private-runtime-isolation.png",
    )
    save(
        redact_feishu_group(sources / "feishu-group-acceptance-raw.png"),
        output / "feishu-group-tool.png",
    )
    save(
        redact_wecom_private(sources / "wecom-private-acceptance-raw.png"),
        output / "wecom-private-isolation-failover.png",
    )
    save(
        redact_wecom_group(sources / "wecom-group-acceptance-raw.png"),
        output / "wecom-group-tool.png",
    )
    save(
        crop_jaeger(sources / "jaeger-feishu-trace-good.png"),
        output / "jaeger-feishu-trace.png",
    )
    save(
        crop_jaeger(sources / "jaeger-wecom-trace-good.png"),
        output / "jaeger-wecom-trace.png",
    )


if __name__ == "__main__":
    main()
