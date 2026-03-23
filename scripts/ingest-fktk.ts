#!/usr/bin/env tsx
/**
 * FKTK / Latvijas Banka ingestion crawler.
 *
 * Crawls the FKTK (fktk.lv) and Latvijas Banka supervision portal
 * (uzraudziba.bank.lv) for:
 *   1. Noteikumi  — binding regulations
 *   2. Ieteikumi  — recommendations
 *   3. Vadlinijas — guidelines
 *   4. Enforcement actions (sankcijas / sodi / lemumi)
 *
 * Writes directly to the SQLite database used by the MCP server.
 *
 * Usage:
 *   npx tsx scripts/ingest-fktk.ts
 *   npx tsx scripts/ingest-fktk.ts --dry-run        # parse only, no DB writes
 *   npx tsx scripts/ingest-fktk.ts --resume          # skip already-ingested URLs
 *   npx tsx scripts/ingest-fktk.ts --force           # drop all data and re-ingest
 *   npx tsx scripts/ingest-fktk.ts --sector kredit   # only crawl kreditiestades sector
 *   npx tsx scripts/ingest-fktk.ts --limit 10        # stop after N provisions
 */

import Database from "better-sqlite3";
import * as cheerio from "cheerio";
import { existsSync, mkdirSync, unlinkSync, writeFileSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { SCHEMA_SQL } from "../src/db.js";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const DB_PATH = process.env["FKTK_DB_PATH"] ?? "data/fktk.db";
const CACHE_DIR = resolve(__dirname, "../data/cache");
const PROGRESS_FILE = resolve(__dirname, "../data/ingest-progress.json");

const RATE_LIMIT_MS = 1500;
const MAX_RETRIES = 3;
const RETRY_BACKOFF_MS = 3000;
const REQUEST_TIMEOUT_MS = 30_000;

const USER_AGENT =
  "AnsvarFKTKCrawler/1.0 (+https://ansvar.eu; compliance research)";

// ---------------------------------------------------------------------------
// FKTK site structure — sector index pages
// ---------------------------------------------------------------------------

/**
 * Each sector has a listing page on fktk.lv (or uzraudziba.bank.lv after the
 * FKTK merger into Latvijas Banka) that contains links to individual
 * regulation pages.
 */
interface SectorDef {
  id: string;
  sourcebookId: string;
  name: string;
  /** Category pages that list regulation links. */
  indexUrls: string[];
  type: "noteikumi" | "ieteikumi" | "vadlinijas";
}

const SECTORS: SectorDef[] = [
  // --- Noteikumi (binding regulations) by sector ---
  {
    id: "vispareja",
    sourcebookId: "FKTK_NOTEIKUMI",
    name: "Vispārējie FKTK noteikumi",
    indexUrls: [
      "https://www.fktk.lv/tiesibu-akti/vispareja/fktk-izdotie-noteikumi/",
    ],
    type: "noteikumi",
  },
  {
    id: "kreditiestades",
    sourcebookId: "FKTK_NOTEIKUMI",
    name: "Kredītiestāžu noteikumi",
    indexUrls: [
      "https://www.fktk.lv/tiesibu-akti/kreditiestades/fktk-izdotie-noteikumi-2/regulejosas-prasibas/",
      "https://www.fktk.lv/tiesibu-akti/kreditiestades/fktk-izdotie-noteikumi-2/",
    ],
    type: "noteikumi",
  },
  {
    id: "apdrosinasana",
    sourcebookId: "FKTK_NOTEIKUMI",
    name: "Apdrošināšanas noteikumi",
    indexUrls: [
      "https://www.fktk.lv/tiesibu-akti/apdrosinasana/fktk-izdotie-noteikumi/",
    ],
    type: "noteikumi",
  },
  {
    id: "maksajumu-iestades",
    sourcebookId: "FKTK_NOTEIKUMI",
    name: "Maksājumu iestāžu noteikumi",
    indexUrls: [
      "https://www.fktk.lv/tiesibu-akti/maksajumu-iestades/fktk-izdotie-noteikumi/",
    ],
    type: "noteikumi",
  },
  {
    id: "finansu-instrumenti",
    sourcebookId: "FKTK_NOTEIKUMI",
    name: "Finanšu instrumentu tirgus noteikumi",
    indexUrls: [
      "https://www.fktk.lv/tiesibu-akti/finansu-instrumentu-tirgus/fktk-izdotie-noteikumi/",
    ],
    type: "noteikumi",
  },
  {
    id: "alternativie-fondi",
    sourcebookId: "FKTK_NOTEIKUMI",
    name: "Alternatīvo ieguldījumu fondu noteikumi",
    indexUrls: [
      "https://www.fktk.lv/tiesibu-akti/alternativo-ieguldijumu-fondu-parvaldnieki/fktk-izdotie-noteikumi-10/",
    ],
    type: "noteikumi",
  },

  // --- Ieteikumi & Vadlīnijas ---
  {
    id: "ieteikumi-vadlinijas",
    sourcebookId: "FKTK_IETEIKUMI",
    name: "FKTK ieteikumi un vadlīnijas (vispārējie)",
    indexUrls: [
      "https://www.fktk.lv/tiesibu-akti/vispareja/fktk-ieteikumi-un-vadlinijas/",
      "https://uzraudziba.bank.lv/tiesibu-akti/vispareja/fktk-ieteikumi-un-vadlinijas/",
    ],
    type: "ieteikumi",
  },
  {
    id: "vadlinijas-kreditiestades",
    sourcebookId: "FKTK_VADLINIJAS",
    name: "Kredītiestāžu vadlīnijas",
    indexUrls: [
      "https://www.fktk.lv/tiesibu-akti/kreditiestades/fktk-ieteikumi-un-vadlinijas/",
    ],
    type: "vadlinijas",
  },
];

/** Enforcement listing pages. */
const ENFORCEMENT_INDEX_URLS = [
  "https://uzraudziba.bank.lv/tirgus-dalibnieki/sankcijas/",
  "https://www.fktk.lv/tirgus-dalibnieki/sankcijas/",
];

// ---------------------------------------------------------------------------
// CLI argument parsing
// ---------------------------------------------------------------------------

interface CliArgs {
  dryRun: boolean;
  resume: boolean;
  force: boolean;
  sector: string | null;
  limit: number | null;
}

function parseArgs(): CliArgs {
  const args = process.argv.slice(2);
  const result: CliArgs = {
    dryRun: false,
    resume: false,
    force: false,
    sector: null,
    limit: null,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--dry-run") {
      result.dryRun = true;
    } else if (arg === "--resume") {
      result.resume = true;
    } else if (arg === "--force") {
      result.force = true;
    } else if (arg === "--sector" && args[i + 1]) {
      result.sector = args[i + 1]!;
      i++;
    } else if (arg === "--limit" && args[i + 1]) {
      const n = Number.parseInt(args[i + 1]!, 10);
      if (!Number.isNaN(n) && n > 0) result.limit = n;
      i++;
    } else {
      console.error(`Unknown argument: ${arg}`);
      console.error(
        "Usage: npx tsx scripts/ingest-fktk.ts [--dry-run] [--resume] [--force] [--sector <id>] [--limit <n>]",
      );
      process.exit(1);
    }
  }

  return result;
}

// ---------------------------------------------------------------------------
// Progress tracking (for --resume)
// ---------------------------------------------------------------------------

interface ProgressState {
  /** URLs already fully ingested. */
  completedUrls: string[];
  /** Timestamp of last run. */
  lastRun: string | null;
}

function loadProgress(): ProgressState {
  if (existsSync(PROGRESS_FILE)) {
    try {
      const raw = readFileSync(PROGRESS_FILE, "utf-8");
      return JSON.parse(raw) as ProgressState;
    } catch {
      // Corrupted file — start fresh.
    }
  }
  return { completedUrls: [], lastRun: null };
}

function saveProgress(state: ProgressState): void {
  const dir = dirname(PROGRESS_FILE);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  writeFileSync(PROGRESS_FILE, JSON.stringify(state, null, 2));
}

// ---------------------------------------------------------------------------
// HTTP fetching with rate limiting and retry
// ---------------------------------------------------------------------------

let lastFetchTime = 0;

async function rateLimitedFetch(url: string): Promise<{ status: number; body: string }> {
  const now = Date.now();
  const elapsed = now - lastFetchTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }

  let lastError: Error | null = null;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      lastFetchTime = Date.now();
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

      const response = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "lv,en;q=0.5",
        },
        signal: controller.signal,
        // Node 20+ fetch: skip certificate validation for fktk.lv self-signed certs
        // @ts-expect-error Node-specific TLS option
        dispatcher: undefined,
      });
      clearTimeout(timeout);

      const body = await response.text();
      return { status: response.status, body };
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));
      const isLastAttempt = attempt === MAX_RETRIES;
      if (isLastAttempt) break;

      const backoff = RETRY_BACKOFF_MS * attempt;
      console.warn(
        `  [retry ${attempt}/${MAX_RETRIES}] ${url} — ${lastError.message} (waiting ${backoff}ms)`,
      );
      await sleep(backoff);
    }
  }

  throw new Error(`Failed after ${MAX_RETRIES} attempts: ${url} — ${lastError?.message}`);
}

/**
 * Fetch with local file cache. Cache files are stored in data/cache/ keyed by
 * URL hash. Useful for development and --resume runs.
 */
async function cachedFetch(url: string): Promise<string> {
  if (!existsSync(CACHE_DIR)) mkdirSync(CACHE_DIR, { recursive: true });

  const cacheKey = Buffer.from(url).toString("base64url").slice(0, 120);
  const cachePath = resolve(CACHE_DIR, `${cacheKey}.html`);

  if (existsSync(cachePath)) {
    return readFileSync(cachePath, "utf-8");
  }

  const { status, body } = await rateLimitedFetch(url);

  if (status !== 200) {
    throw new Error(`HTTP ${status} for ${url}`);
  }

  writeFileSync(cachePath, body);
  return body;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

// ---------------------------------------------------------------------------
// HTML parsing — regulation index pages
// ---------------------------------------------------------------------------

interface RegulationLink {
  url: string;
  title: string;
  sector: SectorDef;
}

/**
 * Parse an FKTK/bank.lv index page and extract links to individual regulation
 * pages. The site uses WordPress with a mix of:
 *   - <article> elements with <h2><a> titles
 *   - <div class="entry-content"> with <ul><li><a> lists
 *   - <div class="wp-block-*"> content blocks
 *   - plain <a> links inside content containers
 *
 * We look for all internal links under the main content area that point to
 * regulation detail pages.
 */
function parseIndexPage(html: string, sector: SectorDef, baseUrl: string): RegulationLink[] {
  const $ = cheerio.load(html);
  const links: RegulationLink[] = [];
  const seen = new Set<string>();

  // WordPress content selectors — try multiple patterns
  const contentSelectors = [
    ".entry-content a",
    "article a",
    ".page-content a",
    ".post-content a",
    ".wp-block-list a",
    "main a",
    "#content a",
    ".content-area a",
    // bank.lv uses different templates
    ".act-list a",
    ".accordion a",
    ".legislation-list a",
  ];

  const baseHost = new URL(baseUrl).hostname;

  for (const selector of contentSelectors) {
    $(selector).each((_i, el) => {
      const $a = $(el);
      let href = $a.attr("href");
      if (!href) return;

      // Resolve relative URLs
      if (href.startsWith("/")) {
        href = `https://${baseHost}${href}`;
      }

      // Only follow links to fktk.lv or bank.lv regulation pages
      if (!isRegulationUrl(href)) return;

      // Skip anchors, PDFs, and external links
      if (href.includes("#") && href.indexOf("#") < href.length - 1) {
        href = href.split("#")[0]!;
      }
      if (href.endsWith(".pdf") || href.endsWith(".xlsx") || href.endsWith(".docx")) return;

      // Deduplicate
      const normalized = href.replace(/\/$/, "");
      if (seen.has(normalized)) return;
      seen.add(normalized);

      const title = cleanText($a.text());
      if (!title || title.length < 5) return;

      links.push({ url: normalized, title, sector });
    });
  }

  return links;
}

function isRegulationUrl(url: string): boolean {
  try {
    const u = new URL(url);
    const host = u.hostname;
    const path = u.pathname;

    // Must be fktk.lv or bank.lv domain
    if (!host.endsWith("fktk.lv") && !host.endsWith("bank.lv")) return false;

    // Must be under tiesibu-akti (legal acts) path, or a noteikumi/ieteikumi/vadlinijas page
    if (
      path.includes("/tiesibu-akti/") ||
      path.includes("/noteikumi") ||
      path.includes("/ieteikumi") ||
      path.includes("/vadlinijas")
    ) {
      return true;
    }

    return false;
  } catch {
    return false;
  }
}

// ---------------------------------------------------------------------------
// HTML parsing — individual regulation pages
// ---------------------------------------------------------------------------

interface ParsedProvision {
  sourcebookId: string;
  reference: string;
  title: string;
  text: string;
  type: string;
  status: string;
  effectiveDate: string | null;
  chapter: string | null;
  section: string | null;
  sourceUrl: string;
}

/**
 * Parse an individual FKTK regulation page into one or more provisions.
 *
 * FKTK regulation pages typically contain:
 *   - Title in <h1> or <h2>
 *   - Regulation number in the title (e.g., "Noteikumi Nr. 125")
 *   - Body text in <div class="entry-content"> or similar
 *   - Chapters/sections marked with headings or numbered paragraphs
 */
function parseRegulationPage(
  html: string,
  link: RegulationLink,
): ParsedProvision[] {
  const $ = cheerio.load(html);
  const provisions: ParsedProvision[] = [];

  // Extract title — prefer <h1>, fallback to link title
  const pageTitle = cleanText(
    $("h1.entry-title, h1.page-title, h1").first().text() ||
    $("h2.entry-title").first().text() ||
    link.title,
  );

  // Extract reference number from title
  const reference = extractReference(pageTitle) || extractReference(link.title) || link.url;

  // Extract effective date from page metadata or text
  const effectiveDate = extractDate($, html);

  // Extract regulation body text
  const contentSelectors = [
    ".entry-content",
    ".page-content",
    ".post-content",
    "article .content",
    "main .content",
    "#content",
  ];

  let bodyHtml = "";
  for (const sel of contentSelectors) {
    const content = $(sel).first();
    if (content.length > 0) {
      bodyHtml = content.html() || "";
      break;
    }
  }

  if (!bodyHtml) {
    // Fallback: grab all text from <main> or <article>
    bodyHtml = $("main").html() || $("article").html() || $("body").html() || "";
  }

  // Try to split into chapters/sections
  const sections = splitIntoSections($, bodyHtml);

  if (sections.length === 0) {
    // Single provision for the entire page
    const fullText = cleanText($(bodyHtml).text() || $.text());
    if (fullText.length > 20) {
      provisions.push({
        sourcebookId: link.sector.sourcebookId,
        reference,
        title: pageTitle,
        text: fullText,
        type: link.sector.type,
        status: "in_force",
        effectiveDate,
        chapter: null,
        section: null,
        sourceUrl: link.url,
      });
    }
  } else {
    for (const sec of sections) {
      const sectionRef = sec.number
        ? `${reference} §${sec.number}`
        : reference;

      provisions.push({
        sourcebookId: link.sector.sourcebookId,
        reference: sectionRef,
        title: sec.heading || pageTitle,
        text: sec.text,
        type: link.sector.type,
        status: "in_force",
        effectiveDate,
        chapter: sec.chapter || null,
        section: sec.number || null,
        sourceUrl: link.url,
      });
    }
  }

  return provisions;
}

interface SectionPart {
  heading: string | null;
  text: string;
  chapter: string | null;
  number: string | null;
}

/**
 * Split regulation body HTML into logical sections. FKTK regulations use a
 * mix of:
 *   - Roman numeral chapters (I, II, III…)
 *   - Arabic numeral sections (1., 2., 3.…)
 *   - <h2>/<h3> headings within the content
 *   - Numbered paragraphs in flowing text
 */
function splitIntoSections(_$: cheerio.CheerioAPI, bodyHtml: string): SectionPart[] {
  const $body = cheerio.load(bodyHtml);
  const sections: SectionPart[] = [];

  let currentChapter: string | null = null;

  // Strategy 1: Split on headings (h2, h3, h4, strong in p)
  const headings = $body("h2, h3, h4");

  if (headings.length >= 2) {
    headings.each((i, el) => {
      const $h = $body(el);
      const heading = cleanText($h.text());

      // Detect chapter markers (Roman numerals, "nodaļa", "daļa")
      const chapterMatch = heading.match(
        /^(I{1,3}V?|VI{0,3}|IX|X{0,3}I{0,3})[\.\s]/,
      );
      if (chapterMatch) {
        currentChapter = chapterMatch[1] || null;
      }

      // Collect all text until the next heading
      let text = "";
      let next = $h.next();
      while (next.length > 0 && !next.is("h2, h3, h4")) {
        text += " " + cleanText(next.text());
        next = next.next();
      }
      text = text.trim();

      if (text.length > 20) {
        const sectionNum = extractSectionNumber(heading);
        sections.push({
          heading,
          text,
          chapter: currentChapter,
          number: sectionNum,
        });
      }
    });
  }

  // Strategy 2: Split on numbered paragraphs if no headings found
  if (sections.length === 0) {
    const fullText = cleanText($body.text());
    const paragraphs = fullText.split(/(?=\d+\.\s)/);

    for (const para of paragraphs) {
      const trimmed = para.trim();
      if (trimmed.length < 20) continue;

      const numMatch = trimmed.match(/^(\d+)\.\s/);
      sections.push({
        heading: null,
        text: trimmed,
        chapter: null,
        number: numMatch ? numMatch[1]! : null,
      });
    }
  }

  return sections;
}

// ---------------------------------------------------------------------------
// HTML parsing — enforcement actions
// ---------------------------------------------------------------------------

interface ParsedEnforcement {
  firmName: string;
  referenceNumber: string | null;
  actionType: string | null;
  amount: number | null;
  date: string | null;
  summary: string;
  sourcebookReferences: string | null;
  sourceUrl: string;
}

/**
 * Parse the sanctions/enforcement listing page. The page at
 * uzraudziba.bank.lv/tirgus-dalibnieki/sankcijas/ uses a filterable
 * list/table with year tabs and segment filters.
 *
 * Each entry typically contains:
 *   - Firm name
 *   - Date of decision
 *   - Type (sods/naudas sods, brīdinājums, aizliegums, ierobežojums)
 *   - Amount (EUR)
 *   - Summary text
 */
function parseEnforcementPage(html: string, sourceUrl: string): ParsedEnforcement[] {
  const $ = cheerio.load(html);
  const actions: ParsedEnforcement[] = [];

  // Try multiple selectors for enforcement items
  const itemSelectors = [
    ".sanction-item",
    ".enforcement-item",
    ".decision-item",
    "table tbody tr",
    ".accordion-item",
    ".act-item",
    ".act",
    "article",
  ];

  for (const selector of itemSelectors) {
    const items = $(selector);
    if (items.length === 0) continue;

    items.each((_i, el) => {
      const $item = $(el);
      const text = cleanText($item.text());
      if (text.length < 30) return;

      const action = extractEnforcementData($item, $, text, sourceUrl);
      if (action) actions.push(action);
    });

    if (actions.length > 0) break;
  }

  // Fallback: look for links to individual enforcement decision pages
  if (actions.length === 0) {
    const links: string[] = [];
    $("a").each((_i, el) => {
      const href = $(el).attr("href");
      if (
        href &&
        (href.includes("sankcij") ||
          href.includes("lemum") ||
          href.includes("sod") ||
          href.includes("penalty") ||
          href.includes("enforcement"))
      ) {
        links.push(href.startsWith("http") ? href : `https://uzraudziba.bank.lv${href}`);
      }
    });

    // The caller should follow these links for detail pages
    for (const link of links) {
      actions.push({
        firmName: "See detail page",
        referenceNumber: null,
        actionType: null,
        amount: null,
        date: null,
        summary: `Enforcement decision link: ${link}`,
        sourcebookReferences: null,
        sourceUrl: link,
      });
    }
  }

  return actions;
}

function extractEnforcementData(
  $item: cheerio.Cheerio<cheerio.Element>,
  $: cheerio.CheerioAPI,
  text: string,
  sourceUrl: string,
): ParsedEnforcement | null {
  // Extract firm name — usually in a heading or bold text
  const firmName =
    cleanText(
      $item.find("h2, h3, h4, strong, .firm-name, .title").first().text(),
    ) || extractFirmName(text);

  if (!firmName || firmName.length < 3) return null;

  // Extract date
  const dateMatch = text.match(
    /(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})/,
  );
  const date = dateMatch
    ? `${dateMatch[3]}-${dateMatch[2]!.padStart(2, "0")}-${dateMatch[1]!.padStart(2, "0")}`
    : null;

  // Extract amount (EUR)
  const amountMatch = text.match(
    /(\d[\d\s,.]*)\s*(?:EUR|euro|eiro)/i,
  );
  const amount = amountMatch
    ? parseFloat(amountMatch[1]!.replace(/[\s.]/g, "").replace(",", "."))
    : null;

  // Determine action type
  const actionType = classifyEnforcementType(text);

  // Extract reference number
  const refMatch = text.match(
    /(?:FKTK|LB)[-\s]?(?:ENF|SAN|LEM)[-\s]?\d{4}[-\s]?\d+/i,
  );
  const referenceNumber = refMatch ? refMatch[0] : null;

  // Build summary — use the full text, truncated
  const summary = text.length > 2000 ? text.slice(0, 2000) + "…" : text;

  return {
    firmName,
    referenceNumber,
    actionType,
    amount: amount && !Number.isNaN(amount) ? amount : null,
    date,
    summary,
    sourcebookReferences: null,
    sourceUrl,
  };
}

function classifyEnforcementType(text: string): string {
  const lower = text.toLowerCase();
  if (lower.includes("naudas sod") || lower.includes("sod") || lower.includes("fine")) {
    return "fine";
  }
  if (lower.includes("aizliegum") || lower.includes("ban") || lower.includes("aptur")) {
    return "ban";
  }
  if (lower.includes("ierobežo") || lower.includes("restrict")) {
    return "restriction";
  }
  if (lower.includes("brīdinā") || lower.includes("warn")) {
    return "warning";
  }
  return "decision";
}

function extractFirmName(text: string): string {
  // Try to find an entity name pattern: AS/SIA/... "Name"
  const entityMatch = text.match(
    /(?:AS|SIA|SE|VAS|ABLV|JSC)\s+[""«]?([^""»\n]{3,60})[""»]?/i,
  );
  if (entityMatch) return entityMatch[0]!.trim();

  // Fallback: first meaningful chunk (up to first period or 80 chars)
  const firstChunk = text.slice(0, 120).split(/[.\n]/)[0];
  return firstChunk ? firstChunk.trim() : "Unknown";
}

// ---------------------------------------------------------------------------
// Text utilities
// ---------------------------------------------------------------------------

function cleanText(s: string): string {
  return s
    .replace(/\s+/g, " ")
    .replace(/\n+/g, " ")
    .trim();
}

/**
 * Extract FKTK regulation reference number from a title string.
 *
 * Examples:
 *   "Noteikumi Nr. 125 …" → "FKTK noteikumi 125"
 *   "Normatīvie noteikumi Nr. 93 …" → "FKTK noteikumi 93"
 *   "FKTK ieteikumi 2022/1 …" → "FKTK ieteikumi 2022/1"
 *   "2021-04-01 Vadlīnijas par …" → "FKTK vadlinijas 2021-04-01"
 */
function extractReference(text: string): string | null {
  // Pattern: "Noteikumi Nr. NNN"
  const noteikumiMatch = text.match(
    /[Nn]oteikumi\s+(?:Nr\.?\s*)?(\d+)/,
  );
  if (noteikumiMatch) return `FKTK noteikumi ${noteikumiMatch[1]}`;

  // Pattern: "Ieteikumi YYYY/N"
  const ieteikumiMatch = text.match(
    /[Ii]eteikumi\s+(\d{4}\/\d+)/,
  );
  if (ieteikumiMatch) return `FKTK ieteikumi ${ieteikumiMatch[1]}`;

  // Pattern: date-prefixed regulation "YYYY-MM-DD …"
  const datePrefix = text.match(/^(\d{4}-\d{2}-\d{2})\s+/);
  if (datePrefix) return `FKTK ${datePrefix[1]}`;

  // Pattern: "Vadlinijas YYYY/N" or "Vadlīnijas YYYY/N"
  const vadlinijasMatch = text.match(
    /[Vv]adl[iī]nijas\s+(\d{4}\/\d+)/,
  );
  if (vadlinijasMatch) return `FKTK vadlinijas ${vadlinijasMatch[1]}`;

  // Pattern: "Nr. NNN"
  const nrMatch = text.match(/Nr\.?\s*(\d+)/);
  if (nrMatch) return `FKTK ${nrMatch[1]}`;

  return null;
}

function extractSectionNumber(heading: string): string | null {
  // "3.1. Riska pārvaldība" → "3.1"
  const dotMatch = heading.match(/^(\d+(?:\.\d+)*)\.\s/);
  if (dotMatch) return dotMatch[1]!;

  // "§5" or "5." at start
  const numMatch = heading.match(/^[§]?(\d+)\.\s/);
  if (numMatch) return numMatch[1]!;

  return null;
}

/**
 * Try to extract an effective/adoption date from the page.
 */
function extractDate($: cheerio.CheerioAPI, html: string): string | null {
  // Look for structured metadata
  const metaDate = $('meta[property="article:published_time"]').attr("content");
  if (metaDate) return metaDate.slice(0, 10);

  // Look for "Spēkā no:" or "Pieņemts:" patterns in text
  const datePatterns = [
    /[Ss]pēkā\s+(?:no|ar)\s*:?\s*(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})/,
    /[Pp]ieņemts\s*:?\s*(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})/,
    /[Ss]tājas\s+spēkā\s*:?\s*(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})/,
    /(\d{4})[.\-](\d{1,2})[.\-](\d{1,2})/,
  ];

  const text = $.text();
  for (const pattern of datePatterns) {
    const match = text.match(pattern);
    if (match) {
      // Handle both DD.MM.YYYY and YYYY-MM-DD
      if (match[1]!.length === 4) {
        return `${match[1]}-${match[2]!.padStart(2, "0")}-${match[3]!.padStart(2, "0")}`;
      }
      return `${match[3]}-${match[2]!.padStart(2, "0")}-${match[1]!.padStart(2, "0")}`;
    }
  }

  // Fallback: date in the URL path
  const urlDate = html.match(/\/(\d{4}-\d{2}-\d{2})-/);
  if (urlDate) return urlDate[1]!;

  return null;
}

// ---------------------------------------------------------------------------
// Database operations
// ---------------------------------------------------------------------------

function openDb(force: boolean): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });

  if (force && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    console.log(`Deleted existing database at ${DB_PATH}`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);
  return db;
}

function ensureSourcebooks(db: Database.Database): void {
  const insert = db.prepare(
    "INSERT OR IGNORE INTO sourcebooks (id, name, description) VALUES (?, ?, ?)",
  );

  insert.run(
    "FKTK_NOTEIKUMI",
    "FKTK Noteikumi",
    "Finanšu un kapitāla tirgus komisijas normatīvie noteikumi — saistošie regulējumi kredītiestādēm, apdrošināšanai, maksājumu iestādēm, finanšu instrumentu tirgum un alternatīvo ieguldījumu fondiem.",
  );

  insert.run(
    "FKTK_IETEIKUMI",
    "FKTK Ieteikumi",
    "Finanšu un kapitāla tirgus komisijas ieteikumi — labās prakses standarti, nesaistošas rekomendācijas un brīvprātīgas atbilstības vadlīnijas.",
  );

  insert.run(
    "FKTK_VADLINIJAS",
    "FKTK Vadlīnijas",
    "Finanšu un kapitāla tirgus komisijas vadlīnijas — uzraudzības gaidas, piemērotības nosacījumi un skaidrojoši materiāli.",
  );
}

function insertProvision(db: Database.Database, p: ParsedProvision): void {
  db.prepare(
    `INSERT INTO provisions
       (sourcebook_id, reference, title, text, type, status, effective_date, chapter, section)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  ).run(
    p.sourcebookId,
    p.reference,
    p.title,
    p.text,
    p.type,
    p.status,
    p.effectiveDate,
    p.chapter,
    p.section,
  );
}

function insertEnforcement(db: Database.Database, e: ParsedEnforcement): void {
  db.prepare(
    `INSERT INTO enforcement_actions
       (firm_name, reference_number, action_type, amount, date, summary, sourcebook_references)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
  ).run(
    e.firmName,
    e.referenceNumber,
    e.actionType,
    e.amount,
    e.date,
    e.summary,
    e.sourcebookReferences,
  );
}

function provisionExists(db: Database.Database, reference: string): boolean {
  const row = db
    .prepare("SELECT 1 FROM provisions WHERE reference = ? LIMIT 1")
    .get(reference) as { 1: number } | undefined;
  return row !== undefined;
}

// ---------------------------------------------------------------------------
// Main pipeline
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const args = parseArgs();
  const progress = args.resume ? loadProgress() : { completedUrls: [], lastRun: null };
  const completedSet = new Set(progress.completedUrls);

  console.log("=== FKTK / Latvijas Banka ingestion crawler ===");
  console.log(`Database: ${DB_PATH}`);
  console.log(
    `Options: dry-run=${args.dryRun}, resume=${args.resume}, force=${args.force}` +
      (args.sector ? `, sector=${args.sector}` : "") +
      (args.limit ? `, limit=${args.limit}` : ""),
  );
  console.log();

  // Open database (unless dry-run)
  let db: Database.Database | null = null;
  if (!args.dryRun) {
    db = openDb(args.force);
    ensureSourcebooks(db);
  }

  // Filter sectors if --sector provided
  const sectors = args.sector
    ? SECTORS.filter((s) => s.id.includes(args.sector!))
    : SECTORS;

  if (sectors.length === 0) {
    console.error(`No sectors match filter: ${args.sector}`);
    console.error(`Available: ${SECTORS.map((s) => s.id).join(", ")}`);
    process.exit(1);
  }

  let totalProvisions = 0;
  let totalEnforcement = 0;
  let totalErrors = 0;
  let limitReached = false;

  // --- Phase 1: Crawl regulation index pages and collect links ---
  console.log("Phase 1: Discovering regulation links from index pages…");
  const allLinks: RegulationLink[] = [];

  for (const sector of sectors) {
    console.log(`\n  [${sector.id}] ${sector.name}`);

    for (const indexUrl of sector.indexUrls) {
      if (completedSet.has(`index:${indexUrl}`)) {
        console.log(`    Skipping (already indexed): ${indexUrl}`);
        continue;
      }

      try {
        console.log(`    Fetching index: ${indexUrl}`);
        const html = await cachedFetch(indexUrl);
        const links = parseIndexPage(html, sector, indexUrl);
        console.log(`    Found ${links.length} regulation links`);
        allLinks.push(...links);

        completedSet.add(`index:${indexUrl}`);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        console.error(`    ERROR fetching index ${indexUrl}: ${msg}`);
        totalErrors++;
      }
    }
  }

  // Deduplicate links by URL
  const uniqueLinks = new Map<string, RegulationLink>();
  for (const link of allLinks) {
    if (!uniqueLinks.has(link.url)) {
      uniqueLinks.set(link.url, link);
    }
  }

  console.log(`\nTotal unique regulation links: ${uniqueLinks.size}`);

  // --- Phase 2: Fetch and parse individual regulation pages ---
  console.log("\nPhase 2: Fetching and parsing individual regulations…");

  for (const [url, link] of uniqueLinks) {
    if (limitReached) break;

    if (args.resume && completedSet.has(`reg:${url}`)) {
      console.log(`  Skipping (already ingested): ${url}`);
      continue;
    }

    try {
      process.stdout.write(`  Fetching: ${link.title.slice(0, 70)}… `);
      const html = await cachedFetch(url);
      const provisions = parseRegulationPage(html, link);

      if (provisions.length === 0) {
        console.log("[no provisions extracted]");
        continue;
      }

      let inserted = 0;
      for (const p of provisions) {
        // Skip duplicates on --resume
        if (args.resume && db && provisionExists(db, p.reference)) {
          continue;
        }

        if (args.dryRun) {
          console.log(
            `    [dry-run] ${p.reference} — ${p.title?.slice(0, 50) ?? "(no title)"}`,
          );
        } else if (db) {
          insertProvision(db, p);
        }
        inserted++;
        totalProvisions++;

        if (args.limit && totalProvisions >= args.limit) {
          limitReached = true;
          console.log(`\n  Limit reached (${args.limit} provisions)`);
          break;
        }
      }

      console.log(`[${inserted} provisions]`);
      completedSet.add(`reg:${url}`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`\n  ERROR: ${url}: ${msg}`);
      totalErrors++;
    }
  }

  // --- Phase 3: Enforcement actions ---
  if (!limitReached) {
    console.log("\nPhase 3: Crawling enforcement actions…");

    for (const enfUrl of ENFORCEMENT_INDEX_URLS) {
      if (args.resume && completedSet.has(`enf:${enfUrl}`)) {
        console.log(`  Skipping (already ingested): ${enfUrl}`);
        continue;
      }

      try {
        console.log(`  Fetching: ${enfUrl}`);
        const html = await cachedFetch(enfUrl);
        const actions = parseEnforcementPage(html, enfUrl);
        console.log(`  Found ${actions.length} enforcement actions`);

        for (const action of actions) {
          if (action.firmName === "See detail page" && action.sourceUrl) {
            // Follow links to individual decision pages
            try {
              process.stdout.write(`    Following: ${action.sourceUrl.slice(0, 70)}… `);
              const detailHtml = await cachedFetch(action.sourceUrl);
              const detailActions = parseEnforcementDetailPage(detailHtml, action.sourceUrl);

              for (const da of detailActions) {
                if (args.dryRun) {
                  console.log(`      [dry-run] ${da.firmName} — ${da.actionType} — ${da.amount ?? "N/A"} EUR`);
                } else if (db) {
                  insertEnforcement(db, da);
                }
                totalEnforcement++;
              }

              console.log(`[${detailActions.length} actions]`);
            } catch (err) {
              const msg = err instanceof Error ? err.message : String(err);
              console.error(`\n    ERROR: ${action.sourceUrl}: ${msg}`);
              totalErrors++;
            }
            continue;
          }

          if (args.dryRun) {
            console.log(
              `    [dry-run] ${action.firmName} — ${action.actionType} — ${action.amount ?? "N/A"} EUR`,
            );
          } else if (db) {
            insertEnforcement(db, action);
          }
          totalEnforcement++;
        }

        completedSet.add(`enf:${enfUrl}`);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        console.error(`  ERROR: ${enfUrl}: ${msg}`);
        totalErrors++;
      }
    }
  }

  // --- Save progress ---
  if (args.resume || !args.dryRun) {
    progress.completedUrls = [...completedSet];
    progress.lastRun = new Date().toISOString();
    saveProgress(progress);
  }

  // --- Summary ---
  console.log("\n=== Ingestion complete ===");
  console.log(`  Provisions inserted: ${totalProvisions}`);
  console.log(`  Enforcement actions: ${totalEnforcement}`);
  console.log(`  Errors:             ${totalErrors}`);

  if (db) {
    const provCount = (
      db.prepare("SELECT count(*) as cnt FROM provisions").get() as { cnt: number }
    ).cnt;
    const sbCount = (
      db.prepare("SELECT count(*) as cnt FROM sourcebooks").get() as { cnt: number }
    ).cnt;
    const enfCount = (
      db.prepare("SELECT count(*) as cnt FROM enforcement_actions").get() as { cnt: number }
    ).cnt;
    const ftsCount = (
      db.prepare("SELECT count(*) as cnt FROM provisions_fts").get() as { cnt: number }
    ).cnt;

    console.log(`\nDatabase totals (${DB_PATH}):`);
    console.log(`  Sourcebooks:          ${sbCount}`);
    console.log(`  Provisions:           ${provCount}`);
    console.log(`  Enforcement actions:  ${enfCount}`);
    console.log(`  FTS entries:          ${ftsCount}`);

    db.close();
  }

  if (totalErrors > 0) {
    console.warn(`\n${totalErrors} error(s) occurred. Check output above.`);
    process.exit(1);
  }
}

// ---------------------------------------------------------------------------
// Enforcement detail page parser
// ---------------------------------------------------------------------------

/**
 * Parse an individual enforcement decision page (linked from the sanctions
 * index). These are news-style pages with a title, date, and body text
 * describing the decision.
 */
function parseEnforcementDetailPage(
  html: string,
  sourceUrl: string,
): ParsedEnforcement[] {
  const $ = cheerio.load(html);

  const title = cleanText(
    $("h1.entry-title, h1.page-title, h1").first().text() || "",
  );

  const bodyText = cleanText(
    $(".entry-content, .page-content, article .content, main .content")
      .first()
      .text() || $.text(),
  );

  if (bodyText.length < 30) return [];

  const fullText = title ? `${title}. ${bodyText}` : bodyText;
  const firmName = extractFirmName(fullText) || title.slice(0, 80);

  // Extract date
  const dateMatch = fullText.match(
    /(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})/,
  );
  const date = dateMatch
    ? `${dateMatch[3]}-${dateMatch[2]!.padStart(2, "0")}-${dateMatch[1]!.padStart(2, "0")}`
    : null;

  // Extract amount
  const amountMatch = fullText.match(
    /(\d[\d\s,.]*)\s*(?:EUR|euro|eiro)/i,
  );
  const amount = amountMatch
    ? parseFloat(amountMatch[1]!.replace(/[\s.]/g, "").replace(",", "."))
    : null;

  const actionType = classifyEnforcementType(fullText);

  // Extract sourcebook references
  const sbRefs: string[] = [];
  const noteikumiRefs = fullText.matchAll(/FKTK\s+noteikumi\s+(?:Nr\.?\s*)?(\d+)/gi);
  for (const m of noteikumiRefs) {
    sbRefs.push(`FKTK noteikumi ${m[1]}`);
  }

  return [
    {
      firmName,
      referenceNumber: null,
      actionType,
      amount: amount && !Number.isNaN(amount) ? amount : null,
      date,
      summary: fullText.length > 2000 ? fullText.slice(0, 2000) + "…" : fullText,
      sourcebookReferences: sbRefs.length > 0 ? sbRefs.join(", ") : null,
      sourceUrl,
    },
  ];
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

main().catch((err) => {
  console.error("Fatal error:", err instanceof Error ? err.message : String(err));
  process.exit(1);
});
