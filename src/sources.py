import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass(frozen=True)
class Source:
    name: str
    url: str
    kind: str  # "list" | "html"
    selector: Optional[str] = None  # CSS selector for HTML extraction
    attribute: Optional[str] = None  # attribute to pull from selector (e.g., href)


BUILTIN_SOURCES: List[Source] = [
    Source(
        name="github_disposable_domains_primary",
        url="https://raw.githubusercontent.com/disposable-email-domains/disposable-email-domains/master/domains.txt",
        kind="list",
    ),
    Source(
        name="github_disposable_domains_additional",
        url="https://raw.githubusercontent.com/andreis/disposable-email-domains/master/domains.txt",
        kind="list",
    ),
    Source(
        name="temp_mail_blogroll",
        url="https://temp-mail.org/en/option/change",
        kind="html",
        selector="a",
        attribute="href",
    ),
    Source(
        name="guerrillamail_domains",
        url="https://www.guerrillamail.com/faq",
        kind="html",
        selector="a",
        attribute="href",
    ),
    Source(
        name="throwawaymail_domains",
        url="https://www.throwawaymail.com/en",
        kind="html",
        selector="a",
        attribute="href",
    ),
]


def load_sources(extra_path: Optional[str]) -> List[Source]:
    if not extra_path:
        return BUILTIN_SOURCES

    path = Path(extra_path)
    if not path.exists():
        raise FileNotFoundError(f"SOURCES_JSON path not found: {extra_path}")

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    extras: List[Source] = []
    for item in data:
        extras.append(
            Source(
                name=item["name"],
                url=item["url"],
                kind=item.get("kind", "list"),
                selector=item.get("selector"),
                attribute=item.get("attribute"),
            )
        )
    return BUILTIN_SOURCES + extras
