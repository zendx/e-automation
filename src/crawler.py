import asyncio
import logging
import re
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse

import dns.asyncresolver
import dns.exception
import httpx
from selectolax.parser import HTMLParser
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from . import config
from .db import (
    get_source_state,
    init_pool,
    init_schema,
    persist_domains,
    update_source_metadata,
    upsert_sources,
)
from .sources import Source, load_sources

logger = logging.getLogger("crawler")

DOMAIN_RE = re.compile(r"([a-zA-Z0-9-]{1,63}\.)+[a-zA-Z]{2,}", re.IGNORECASE)
ALLOWED_CONTENT_TYPES = ("text/plain", "text/html", "application/json")


class FetchError(Exception):
    pass


def is_probable_domain(candidate: str) -> bool:
    if not candidate:
        return False
    candidate = candidate.strip().lower()
    if len(candidate) < 4:
        return False
    if "@" in candidate:
        candidate = candidate.split("@", 1)[-1]
    return bool(DOMAIN_RE.fullmatch(candidate))


def extract_domains_from_list(text: str) -> List[str]:
    domains: Set[str] = set()
    for line in text.splitlines():
        candidate = line.strip()
        if "#" in candidate:
            candidate = candidate.split("#", 1)[0].strip()
        if is_probable_domain(candidate):
            domains.add(candidate.lower())
    return sorted(domains)


def extract_domains_from_html(html: str, selector: Optional[str], attribute: Optional[str]) -> List[str]:
    parser = HTMLParser(html)
    domains: Set[str] = set()

    def collect(candidate: str) -> None:
        if not candidate:
            return
        candidate = candidate.strip()
        if candidate.startswith("mailto:"):
            candidate = candidate[len("mailto:") :]
        if candidate.startswith("http"):
            parsed = urlparse(candidate)
            candidate = parsed.netloc
        if is_probable_domain(candidate):
            domains.add(candidate.lower())

    if selector:
        for node in parser.css(selector):
            if attribute and node.attributes.get(attribute):
                collect(node.attributes.get(attribute))
            else:
                collect(node.text())
    else:
        collect(parser.text())

    # Fallback: regex scan of full HTML
    for match in DOMAIN_RE.finditer(html):
        collect(match.group(0))

    return sorted(domains)


async def _resolve_domain(
    resolver: dns.asyncresolver.Resolver,
    domain: str,
    timeout: float,
) -> bool:
    try:
        await resolver.resolve(domain, "MX", lifetime=timeout)
        return True
    except dns.exception.DNSException:
        try:
            await resolver.resolve(domain, "A", lifetime=timeout)
            return True
        except dns.exception.DNSException:
            return False


async def filter_active_domains(
    domains: Iterable[str],
    resolver: Optional[dns.asyncresolver.Resolver],
    semaphore: Optional[asyncio.Semaphore],
    timeout: float,
) -> List[str]:
    if resolver is None or semaphore is None:
        return list(domains)

    async def check(domain: str) -> Optional[str]:
        try:
            async with semaphore:
                return domain if await _resolve_domain(resolver, domain, timeout) else None
        except Exception as exc:  # noqa: BLE001
            logger.debug("DNS check failed for %s: %s", domain, exc)
            return None

    tasks = [asyncio.create_task(check(d)) for d in domains]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    return [d for d in results if d]


async def _validate_content_type(resp: httpx.Response) -> None:
    content_type = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
    if content_type and all(not content_type.startswith(ct) for ct in ALLOWED_CONTENT_TYPES):
        raise FetchError(f"Unsupported content type: {content_type}")


async def fetch_with_retries(
    client: httpx.AsyncClient,
    url: str,
    headers: Dict[str, str],
    max_retries: int,
    max_bytes: int,
) -> httpx.Response:
    async for attempt in AsyncRetrying(
        stop=stop_after_attempt(max_retries),
        wait=wait_exponential_jitter(initial=1, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, FetchError)),
        reraise=True,
    ):
        with attempt:
            async with client.stream("GET", url, headers=headers) as resp:
                if resp.status_code == 304:
                    return resp
                if resp.status_code in {429, 500, 502, 503, 504}:
                    raise FetchError(f"Retryable status {resp.status_code}")
                resp.raise_for_status()
                await _validate_content_type(resp)
                content_length = resp.headers.get("Content-Length")
                if content_length:
                    try:
                        if int(content_length) > max_bytes:
                            raise FetchError(f"Response too large: {content_length} bytes")
                    except ValueError:
                        pass

                chunks: List[bytes] = []
                total = 0
                async for chunk in resp.aiter_bytes():
                    total += len(chunk)
                    if total > max_bytes:
                        raise FetchError(f"Response exceeded limit {max_bytes} bytes")
                    chunks.append(chunk)
                resp._content = b"".join(chunks)
                return resp

    raise FetchError("Exhausted retries")


async def crawl_source(
    client: httpx.AsyncClient,
    pool,
    src: Source,
    host_limiter: Dict[str, asyncio.Semaphore],
    max_retries: int,
    max_bytes: int,
    dns_resolver: Optional[dns.asyncresolver.Resolver],
    dns_semaphore: Optional[asyncio.Semaphore],
    dns_timeout: float,
) -> Tuple[str, int]:
    parsed = urlparse(src.url)
    host = parsed.netloc
    limiter = host_limiter[host]

    state = await get_source_state(pool, src.name)
    headers = {}
    if state:
        etag, last_modified = state["etag"], state["last_modified"]
        if etag:
            headers["If-None-Match"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified

    async with limiter:
        try:
            resp = await fetch_with_retries(client, src.url, headers, max_retries, max_bytes)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 304:
                logger.info("Unchanged %s (304)", src.name)
                await update_source_metadata(
                    pool,
                    src.name,
                    exc.response.headers.get("ETag"),
                    exc.response.headers.get("Last-Modified"),
                )
                return src.name, 0
            logger.warning("Failed %s: %s", src.name, exc)
            return src.name, 0
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed %s: %s", src.name, exc)
            return src.name, 0

    if resp.status_code == 304:
        await update_source_metadata(
            pool,
            src.name,
            resp.headers.get("ETag"),
            resp.headers.get("Last-Modified"),
        )
        return src.name, 0

    text = resp.text
    if src.kind == "list":
        domains = extract_domains_from_list(text)
    else:
        domains = extract_domains_from_html(text, src.selector, src.attribute)

    active_domains = await filter_active_domains(
        domains,
        dns_resolver,
        dns_semaphore,
        dns_timeout,
    )

    saved = await persist_domains(pool, active_domains, src.name)
    await update_source_metadata(
        pool,
        src.name,
        resp.headers.get("ETag"),
        resp.headers.get("Last-Modified"),
    )
    logger.info(
        "Source %s -> %d/%d active domains",
        src.name,
        saved,
        len(domains),
    )
    return src.name, saved


async def run() -> None:
    settings = config.load_settings()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    sources = load_sources(settings.sources_json)
    host_limiters: Dict[str, asyncio.Semaphore] = defaultdict(
        lambda: asyncio.Semaphore(settings.per_host_limit)
    )

    dns_resolver = dns.asyncresolver.Resolver() if settings.dns_validate else None
    dns_semaphore = asyncio.Semaphore(settings.dns_concurrency) if settings.dns_validate else None
    if dns_resolver:
        dns_resolver.lifetime = settings.dns_timeout

    async with httpx.AsyncClient(
        headers={"User-Agent": settings.user_agent},
        timeout=httpx.Timeout(
            connect=settings.connect_timeout,
            read=settings.request_timeout,
            write=settings.request_timeout,
            pool=settings.connect_timeout,
        ),
        limits=httpx.Limits(
            max_connections=settings.concurrency,
            max_keepalive_connections=settings.concurrency,
            keepalive_expiry=30,
        ),
        follow_redirects=True,
        verify=True,
    ) as client:
        pool = await init_pool(settings.database_url)
        await init_schema(pool)
        await upsert_sources(pool, sources)

        tasks = [
            asyncio.create_task(
                crawl_source(
                    client,
                    pool,
                    src,
                    host_limiters,
                    settings.max_retries,
                    settings.max_response_bytes,
                    dns_resolver,
                    dns_semaphore,
                    settings.dns_timeout,
                )
            )
            for src in sources
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)
        total = 0
        for result in results:
            if isinstance(result, Exception):
                logger.error("Task failed: %s", result)
                continue
            _, count = result
            total += count
        logger.info("Finished crawl. Sources: %d, new/updated domains: %d", len(results), total)
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run())
