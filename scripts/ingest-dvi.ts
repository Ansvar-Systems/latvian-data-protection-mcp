#!/usr/bin/env tsx
/**
 * DVI (Datu valsts inspekcija — Latvian DPA) ingestion crawler.
 *
 * Scrapes dvi.gov.lv for:
 *   - Decisions (lēmumi) — sanctions, warnings, enforcement orders
 *   - Guidance documents (#DVIskaidro, vadlīnijas) — practical data protection guidance
 *
 * Populates the SQLite database used by the MCP server.
 *
 * Data sources:
 *   1. dvi.gov.lv/lv/lemumi     — Decisions table (with PDF download links)
 *   2. dvi.gov.lv/lv/dviskaidro — #DVIskaidro explanatory articles index
 *   3. dvi.gov.lv/lv/jaunums/*  — Individual news/guidance article pages
 *
 * Usage:
 *   npx tsx scripts/ingest-dvi.ts                # Full ingestion
 *   npx tsx scripts/ingest-dvi.ts --resume       # Skip already-ingested references
 *   npx tsx scripts/ingest-dvi.ts --dry-run      # Parse and log, do not write to DB
 *   npx tsx scripts/ingest-dvi.ts --force        # Drop existing data and re-ingest
 *
 * Environment:
 *   DVI_DB_PATH      — SQLite database path (default: data/dvi.db)
 *   DVI_USER_AGENT   — Custom User-Agent header (default: built-in)
 *   DVI_RATE_LIMIT   — Milliseconds between requests (default: 1500)
 *   DVI_MAX_RETRIES  — Max retry attempts per request (default: 3)
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

// ---------------------------------------------------------------------------
// cheerio — loaded dynamically so the script fails fast with a clear message
// ---------------------------------------------------------------------------

let cheerio: typeof import("cheerio");
try {
  cheerio = await import("cheerio");
} catch {
  console.error(
    "Missing dependency: cheerio\n" +
      "Install it with:  npm install --save-dev cheerio @types/cheerio\n",
  );
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DB_PATH = process.env["DVI_DB_PATH"] ?? "data/dvi.db";
const USER_AGENT =
  process.env["DVI_USER_AGENT"] ??
  "AnsvarDVICrawler/1.0 (+https://ansvar.eu; data-protection-research)";
const RATE_LIMIT_MS = parseInt(
  process.env["DVI_RATE_LIMIT"] ?? "1500",
  10,
);
const MAX_RETRIES = parseInt(
  process.env["DVI_MAX_RETRIES"] ?? "3",
  10,
);

const BASE_URL = "https://www.dvi.gov.lv";

// CLI flags
const args = new Set(process.argv.slice(2));
const FLAG_RESUME = args.has("--resume");
const FLAG_DRY_RUN = args.has("--dry-run");
const FLAG_FORCE = args.has("--force");

// ---------------------------------------------------------------------------
// Topic rules — maps Latvian keywords to topic IDs from seed-sample.ts
// ---------------------------------------------------------------------------

interface TopicRule {
  id: string;
  name_local: string;
  name_en: string;
  description: string;
  /** Keywords to match in title + summary + full_text (case-insensitive). */
  keywords: string[];
}

const TOPIC_RULES: TopicRule[] = [
  {
    id: "cookies",
    name_local: "Sīkdatnes un izsekotāji",
    name_en: "Cookies and trackers",
    description:
      "Sīkdatņu un citu izsekotāju izmantošana lietotāju ierīcēs (VDAR 6. pants).",
    keywords: [
      "sīkdatne", "sīkdatņu", "sīkdatnes", "cookie", "tracker",
      "izsekotāj", "analītikas sīkdatne", "reklāmas sīkdatne",
    ],
  },
  {
    id: "employee_monitoring",
    name_local: "Darbinieku uzraudzība",
    name_en: "Employee monitoring",
    description:
      "Darbinieku datu apstrāde un uzraudzība darba vietā.",
    keywords: [
      "darbiniek", "darba viet", "darba devēj", "uzraudzīb",
      "employee", "monitoring", "darba attiecīb", "darba līgum",
      "gps izsekošan", "darba laik",
    ],
  },
  {
    id: "video_surveillance",
    name_local: "Videonovērošana",
    name_en: "Video surveillance",
    description:
      "Videonovērošanas sistēmu izmantošana un personas datu aizsardzība (VDAR 6. pants).",
    keywords: [
      "videonovērošan", "kamera", "video surveillance",
      "novērošanas kamera", "drošības kamera",
    ],
  },
  {
    id: "data_breach",
    name_local: "Datu drošības pārkāpumi",
    name_en: "Data breach notification",
    description:
      "Paziņošana par personas datu drošības pārkāpumiem DVI un datu subjektiem (VDAR 33.–34. pants).",
    keywords: [
      "datu pārkāpum", "drošības pārkāpum", "data breach",
      "paziņojum", "incidents", "kiberuzbrukum",
      "72 stund", "pārkāpuma paziņošan",
    ],
  },
  {
    id: "consent",
    name_local: "Piekrišana",
    name_en: "Consent",
    description:
      "Piekrišanas personas datu apstrādei iegūšana, spēkā esamība un atsaukšana (VDAR 7. pants).",
    keywords: [
      "piekrišan", "consent", "opt-in", "opt-out",
      "atsaukšan", "atteikšan", "mārketinga piekrišan",
    ],
  },
  {
    id: "dpia",
    name_local: "Ietekmes novērtējums",
    name_en: "Data Protection Impact Assessment (DPIA)",
    description:
      "Ietekmes uz datu aizsardzību novērtējums augsta riska apstrādei (VDAR 35. pants).",
    keywords: [
      "ietekmes novērtējum", "dpia", "ietekmes uz datu aizsardzīb",
      "augsta riska", "impact assessment",
    ],
  },
  {
    id: "transfers",
    name_local: "Starptautiskie datu nosūtījumi",
    name_en: "International data transfers",
    description:
      "Personas datu nosūtīšana uz trešajām valstīm vai starptautiskajām organizācijām (VDAR 44.–49. pants).",
    keywords: [
      "datu nosūtīšan", "datu nosūtījum", "trešā valst",
      "starptautisk", "transfer", "adequacy",
      "standard contractual", "bcr",
    ],
  },
  {
    id: "data_subject_rights",
    name_local: "Datu subjektu tiesības",
    name_en: "Data subject rights",
    description:
      "Piekļuves, labošanas, dzēšanas un citu tiesību īstenošana (VDAR 15.–22. pants).",
    keywords: [
      "datu subjekt", "tiesības piekļūt", "tiesības labot",
      "tiesības dzēst", "pārnesamīb", "ierobežot apstrād",
      "data subject right", "right of access", "right to erasure",
      "tiesības iebilst", "aizmirst",
    ],
  },
  {
    id: "direct_marketing",
    name_local: "Tiešais mārketings",
    name_en: "Direct marketing",
    description:
      "Personas datu apstrāde tiešā mārketinga nolūkos un elektronisko sakaru noteikumi.",
    keywords: [
      "mārketinga", "tiešais mārketings", "direct marketing",
      "e-pasts", "reklāma", "komercziņojum",
      "nevēlam", "spam", "abonēšan",
    ],
  },
  {
    id: "data_security",
    name_local: "Datu drošība",
    name_en: "Data security",
    description:
      "Tehniskie un organizatoriskie pasākumi personas datu aizsardzībai (VDAR 32. pants).",
    keywords: [
      "datu drošīb", "data security", "šifrēšan", "encryption",
      "drošības pasākum", "aizsardzības pasākum",
      "parole", "password", "piekļuves kontrol",
    ],
  },
  {
    id: "health_data",
    name_local: "Veselības dati",
    name_en: "Health data",
    description:
      "Veselības datu apstrāde — īpašo kategoriju personas dati (VDAR 9. pants).",
    keywords: [
      "veselīb", "pacient", "medicīnisk", "slimnīc",
      "ārstniecīb", "health", "aptiek", "diagnoz",
    ],
  },
  {
    id: "children",
    name_local: "Bērnu datu aizsardzība",
    name_en: "Children's data protection",
    description:
      "Bērnu personas datu aizsardzība, jo īpaši tiešsaistes vidē (VDAR 8. pants).",
    keywords: [
      "bērn", "child", "nepilngadīg", "skolēn",
      "izglītīb", "skol",
    ],
  },
  {
    id: "profiling",
    name_local: "Profilēšana un automatizēta lēmumu pieņemšana",
    name_en: "Profiling and automated decision-making",
    description:
      "Automatizēta lēmumu pieņemšana un profilēšana (VDAR 22. pants).",
    keywords: [
      "profilēšan", "automatizēt", "profiling",
      "automated decision", "algoritm", "mākslīgais intelekt",
    ],
  },
  {
    id: "dpo",
    name_local: "Datu aizsardzības speciālists",
    name_en: "Data Protection Officer",
    description:
      "Datu aizsardzības speciālista iecelšana, statuss un pienākumi (VDAR 37.–39. pants).",
    keywords: [
      "datu aizsardzības speciālist", "data protection officer", "dpo",
      "das",
    ],
  },
  {
    id: "biometrics",
    name_local: "Biometriskie dati",
    name_en: "Biometric data",
    description:
      "Biometrisko datu apstrāde — pirkstu nospiedumi, sejas atpazīšana u.c.",
    keywords: [
      "biometrisk", "biometric", "pirkstu nospiedum",
      "sejas atpazīšan",
    ],
  },
];

// ---------------------------------------------------------------------------
// GDPR article detection — extracts article numbers from Latvian text
// ---------------------------------------------------------------------------

const GDPR_ARTICLE_PATTERNS = [
  // Latvian: "5. pants", "32. pantu", "33.–34. pants"
  /(\d+)\.\s*pant/gi,
  // Latvian: "VDAR 5., 6. un 13. pants"
  /(?:VDAR|regulas?)\s+([\d.,\s]+(?:un\s+\d+)?)\.\s*pant/gi,
  // English: "Article 5", "Art. 32"
  /\bArt(?:icle|\.)\s*(\d+(?:\s*(?:and|,\s*\d+))*)/gi,
  // Parenthetical: "(VDAR 33.–34. pants)"
  /\((?:VDAR|regula)\s*(\d+(?:\s*[.–\-,]\s*\d+)*)\s*\.?\s*pant/gi,
  // Standalone: "33. un 34. pantu"
  /(\d+)\.\s*un\s+(\d+)\.\s*pant/gi,
];

function extractGdprArticles(text: string): string[] {
  const articles = new Set<string>();

  for (const pattern of GDPR_ARTICLE_PATTERNS) {
    pattern.lastIndex = 0;
    let match: RegExpExecArray | null;
    while ((match = pattern.exec(text)) !== null) {
      // Handle "33. un 34." compound pattern (last regex)
      if (match[2]) {
        const p1 = parseInt(match[1] ?? "", 10);
        const p2 = parseInt(match[2], 10);
        if (!isNaN(p1) && p1 >= 1 && p1 <= 99) articles.add(String(p1));
        if (!isNaN(p2) && p2 >= 1 && p2 <= 99) articles.add(String(p2));
        continue;
      }

      const numStr = match[1];
      if (!numStr) continue;

      // Split compound references: "5., 6. un 13." or "33–34"
      const nums = numStr
        .split(/[,\s]+(?:un)\s*|[,.\s–\-]+/)
        .map((s) => s.trim())
        .filter(Boolean);
      for (const n of nums) {
        const parsed = parseInt(n, 10);
        if (!isNaN(parsed) && parsed >= 1 && parsed <= 99) {
          articles.add(String(parsed));
        }
      }
    }
  }

  return [...articles].sort((a, b) => parseInt(a, 10) - parseInt(b, 10));
}

// ---------------------------------------------------------------------------
// Topic detection
// ---------------------------------------------------------------------------

function detectTopics(text: string): string[] {
  const lower = text.toLowerCase();
  const matched: string[] = [];

  for (const rule of TOPIC_RULES) {
    const hit = rule.keywords.some((kw) => lower.includes(kw.toLowerCase()));
    if (hit) {
      matched.push(rule.id);
    }
  }

  return matched;
}

// ---------------------------------------------------------------------------
// Fine amount extraction — Latvian/EU format: "EUR 65 000,00" or "65 000 eiro"
// ---------------------------------------------------------------------------

const FINE_PATTERNS = [
  // "EUR 65 000,00", "EUR 500,00", "EUR 1 200 000"
  /EUR\s*(\d{1,3}(?:[\s.]\d{3})*(?:,\d{2})?)/gi,
  // "65 000 eiro", "7000 eiro", "1 200 000 eiro"
  /(\d{1,3}(?:[\s.]\d{3})*)\s*eiro/gi,
  // "naudas sods: EUR 500,00" (already covered above, but explicit)
  /naudas\s+sods[:\s]*EUR\s*(\d{1,3}(?:[\s.]\d{3})*(?:,\d{2})?)/gi,
  // Euro sign: "€ 65 000", "€500"
  /\u20ac\s*(\d{1,3}(?:[\s.]\d{3})*(?:,\d{2})?)/gi,
];

function extractFineAmount(text: string): number | null {
  let maxFine = 0;

  for (const pattern of FINE_PATTERNS) {
    pattern.lastIndex = 0;
    let match: RegExpExecArray | null;
    while ((match = pattern.exec(text)) !== null) {
      const rawNum = match[1];
      if (!rawNum) continue;

      // Parse Latvian/EU format: "65 000,00" → 65000, "1 200 000" → 1200000
      const normalized = rawNum.replace(/[\s.]/g, "").replace(",", ".");
      const amount = parseFloat(normalized);

      if (!isNaN(amount) && amount > maxFine) {
        maxFine = Math.round(amount);
      }
    }
  }

  return maxFine > 0 ? maxFine : null;
}

// ---------------------------------------------------------------------------
// Date extraction — Latvian date formats
// ---------------------------------------------------------------------------

const LATVIAN_MONTHS: Record<string, string> = {
  janvāris: "01", janvārī: "01", janvāra: "01",
  februāris: "02", februārī: "02", februāra: "02",
  marts: "03", martā: "03", marta: "03",
  aprīlis: "04", aprīlī: "04", aprīļa: "04",
  maijs: "05", maijā: "05", maija: "05",
  jūnijs: "06", jūnijā: "06", jūnija: "06",
  jūlijs: "07", jūlijā: "07", jūlija: "07",
  augusts: "08", augustā: "08", augusta: "08",
  septembris: "09", septembrī: "09", septembra: "09",
  oktobris: "10", oktobrī: "10", oktobra: "10",
  novembris: "11", novembrī: "11", novembra: "11",
  decembris: "12", decembrī: "12", decembra: "12",
};

function extractDate(text: string): string | null {
  // Latvian long format: "2023. gada 15. martā" or "15. marts 2023"
  const lvLong1 = text.match(
    /(\d{4})\.\s*gada\s+(\d{1,2})\.\s*(janvār[a-zāīū]*|februār[a-zāīū]*|mart[a-zāīū]*|aprīl[a-zāīū]*|maij[a-zāīū]*|jūnij[a-zāīū]*|jūlij[a-zāīū]*|august[a-zāīū]*|septemb[a-zāīū]*|oktob[a-zāīū]*|novemb[a-zāīū]*|decemb[a-zāīū]*)/i,
  );
  if (lvLong1) {
    const year = lvLong1[1];
    const day = (lvLong1[2] ?? "").padStart(2, "0");
    const monthKey = Object.keys(LATVIAN_MONTHS).find((k) =>
      (lvLong1[3] ?? "").toLowerCase().startsWith(k.slice(0, 4)),
    );
    const month = monthKey ? LATVIAN_MONTHS[monthKey] : null;
    if (month && year) return `${year}-${month}-${day}`;
  }

  // Latvian numeric: "15.03.2023" or "09.02.2026"
  const lvNum = text.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
  if (lvNum) {
    const day = (lvNum[1] ?? "").padStart(2, "0");
    const month = (lvNum[2] ?? "").padStart(2, "0");
    const year = lvNum[3];
    if (year) return `${year}-${month}-${day}`;
  }

  // ISO: "2023-01-30"
  const isoMatch = text.match(/(\d{4}-\d{2}-\d{2})/);
  if (isoMatch) return isoMatch[1] ?? null;

  return null;
}

// ---------------------------------------------------------------------------
// Decision type inference from corrective measure text
// ---------------------------------------------------------------------------

function inferDecisionType(correctiveMeasure: string, title: string): string {
  const combined = (correctiveMeasure + " " + title).toLowerCase();

  if (combined.includes("naudas sods") || combined.includes("soda piemērošan") || combined.includes("soda piemerosanu")) {
    return "sanction";
  }
  if (combined.includes("brīdinājum") || combined.includes("rājien")) {
    return "warning";
  }
  if (combined.includes("uzlikts pienākum") || combined.includes("korektīv")) {
    return "obligation";
  }
  if (combined.includes("aizliegum") || combined.includes("apturēšan")) {
    return "ban";
  }
  return "decision";
}

// ---------------------------------------------------------------------------
// Entity name cleanup
// ---------------------------------------------------------------------------

function cleanEntityName(raw: string): string {
  return raw
    .replace(/^[""]|[""]$/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

// ---------------------------------------------------------------------------
// HTTP fetch with retry, rate limiting, and proper headers
// ---------------------------------------------------------------------------

let lastFetchTime = 0;

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function rateLimitedFetch(url: string): Promise<Response | null> {
  const now = Date.now();
  const elapsed = now - lastFetchTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      lastFetchTime = Date.now();
      const res = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "lv-LV,lv;q=0.9,en;q=0.5",
        },
        redirect: "follow",
      });

      if (res.ok) {
        return res;
      }

      // 429 Too Many Requests — back off
      if (res.status === 429) {
        const retryAfter = parseInt(res.headers.get("Retry-After") ?? "10", 10);
        console.warn(`  Rate limited (429), waiting ${retryAfter}s before retry ${attempt}/${MAX_RETRIES}`);
        await sleep(retryAfter * 1000);
        continue;
      }

      // 403 Forbidden — skip
      if (res.status === 403) {
        console.warn(`  Blocked (403): ${url}`);
        return null;
      }

      // 404 Not Found
      if (res.status === 404) {
        console.warn(`  Not found (404): ${url}`);
        return null;
      }

      // Server errors — retry with backoff
      if (res.status >= 500) {
        console.warn(`  Server error (${res.status}), retry ${attempt}/${MAX_RETRIES}: ${url}`);
        await sleep(2000 * attempt);
        continue;
      }

      // Unexpected status
      console.warn(`  HTTP ${res.status} for ${url}`);
      return null;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(`  Network error (attempt ${attempt}/${MAX_RETRIES}): ${msg}`);
      if (attempt < MAX_RETRIES) {
        await sleep(2000 * attempt);
      }
    }
  }

  console.error(`  Failed after ${MAX_RETRIES} retries: ${url}`);
  return null;
}

// ---------------------------------------------------------------------------
// Parse decisions table at dvi.gov.lv/lv/lemumi
//
// Structure: year <h2> headers followed by <table> elements.
// Each table row contains:
//   - Column 0: Entity name (pārzinis/apstrādātājs)
//   - Column 1: Corrective measure (naudas sods, brīdinājums, etc.)
//   - Column 2: Decision document link (PDF download)
//   - Column 3: Adoption date (DD.MM.YYYY)
//   - Column 4: Status (spēkā / apstrīdēts / ierobežota pieejamība)
// ---------------------------------------------------------------------------

interface DecisionTableRow {
  entity_name: string;
  corrective_measure: string;
  download_url: string | null;
  date: string | null;
  status: string;
  reference: string;
  fine_amount: number | null;
  type: string;
}

function parseDecisionsPage(html: string): DecisionTableRow[] {
  const $ = cheerio.load(html);
  const rows: DecisionTableRow[] = [];

  $("table").each((_tableIdx, tableEl) => {
    const $table = $(tableEl);

    $table.find("tr").each((_rowIdx, rowEl) => {
      const cells = $(rowEl).find("td");
      if (cells.length < 4) return; // Skip header rows and malformed rows

      const entityRaw = $(cells[0]).text().trim();
      const correctiveMeasure = $(cells[1]).text().trim();

      // Extract download link from column 2
      const downloadLink = $(cells[2]).find("a[href]").first();
      let downloadUrl: string | null = null;
      if (downloadLink.length > 0) {
        const href = downloadLink.attr("href") ?? "";
        downloadUrl = href.startsWith("http") ? href : `${BASE_URL}${href}`;
      }

      const dateRaw = $(cells[3]).text().trim();
      const statusRaw = cells.length >= 5 ? $(cells[4]).text().trim() : "";

      if (!entityRaw || entityRaw.length < 2) return;

      const entity = cleanEntityName(entityRaw);
      const date = extractDate(dateRaw);
      const fineAmount = extractFineAmount(correctiveMeasure);
      const type = inferDecisionType(correctiveMeasure, entity);

      // Generate a stable reference from entity + date
      const dateSlug = date ?? "unknown";
      const entitySlug = entity
        .replace(/[^a-zA-ZāčēģīķļņšūžĀČĒĢĪĶĻŅŠŪŽ0-9\s-]/g, "")
        .replace(/\s+/g, "-")
        .slice(0, 40)
        .toUpperCase();
      const reference = `DVI-${dateSlug}-${entitySlug}`;

      // Map DVI status text to DB status values
      let status = "final";
      const statusLower = statusRaw.toLowerCase();
      if (statusLower.includes("apstrīdēt") || statusLower.includes("pārsūdzēt")) {
        status = "appealed";
      } else if (statusLower.includes("ierobežot")) {
        status = "restricted";
      }

      rows.push({
        entity_name: entity,
        corrective_measure: correctiveMeasure,
        download_url: downloadUrl,
        date,
        status,
        reference,
        fine_amount: fineAmount,
        type,
      });
    });
  });

  return rows;
}

// ---------------------------------------------------------------------------
// Parse individual DVI article/news page (used for #DVIskaidro and news)
// ---------------------------------------------------------------------------

interface ParsedArticle {
  title: string;
  date: string | null;
  bodyText: string;
  summaryText: string | null;
}

function parseArticlePage(html: string, sourceUrl: string): ParsedArticle | null {
  const $ = cheerio.load(html);

  // -- Title --
  let title =
    $("h1").first().text().trim() ||
    $("title")
      .text()
      .replace(/\s*\|\s*Datu valsts inspekcija.*$/i, "")
      .trim();

  if (!title) {
    console.warn(`  No title found on ${sourceUrl}`);
    return null;
  }

  // -- Date --
  let date: string | null = null;

  // Look for "Publicēts: DD.MM.YYYY" pattern
  const publishedMatch = $.text().match(/Publicēts:\s*(\d{1,2}\.\d{1,2}\.\d{4})/);
  if (publishedMatch && publishedMatch[1]) {
    date = extractDate(publishedMatch[1]);
  }

  // Try <time datetime="...">
  if (!date) {
    const timeEl = $("time[datetime]").first();
    if (timeEl.length > 0) {
      date = timeEl.attr("datetime")?.slice(0, 10) ?? null;
    }
  }

  // Meta tags
  if (!date) {
    const metaDate =
      $('meta[property="article:published_time"]').attr("content") ??
      $('meta[name="date"]').attr("content");
    if (metaDate) {
      date = metaDate.slice(0, 10);
    }
  }

  // -- Body text --
  // DVI site uses various content containers; try common patterns
  let bodyHtml =
    $(".field--name-body").html() ??
    $(".node__content").html() ??
    $("article .content").html() ??
    $("article").html() ??
    $("main .region-content").html() ??
    $("main").html() ??
    "";

  // Strip non-content elements
  const body$ = cheerio.load(bodyHtml);
  body$(
    "nav, header, footer, script, style, .breadcrumb, .pager, .sidebar, " +
      ".menu, .tabs, .social-sharing, .field--name-field-tags, " +
      ".feedback-form, .rating-form, form",
  ).remove();

  let bodyText = body$.text().replace(/\s+/g, " ").trim();

  if (!bodyText || bodyText.length < 50) {
    // Fallback: whole page with navigation stripped
    const page$ = cheerio.load(html);
    page$(
      "nav, header, footer, script, style, .breadcrumb, .pager, .sidebar, .menu, .tabs",
    ).remove();
    bodyText = page$("main").text().replace(/\s+/g, " ").trim();
  }

  if (!bodyText || bodyText.length < 30) {
    console.warn(`  Body text too short (${bodyText.length} chars) on ${sourceUrl}`);
    return null;
  }

  // -- Summary (first substantial paragraph) --
  let summaryText: string | null = null;
  const firstP =
    $(".field--name-body p").first().text().trim() ||
    $("article p").first().text().trim() ||
    $("main p").first().text().trim();
  if (firstP && firstP.length > 30 && firstP.length < 1500) {
    summaryText = firstP;
  }

  // Extract date from body text if not found in metadata
  if (!date) {
    date = extractDate(bodyText);
  }

  return { title, date, bodyText, summaryText };
}

// ---------------------------------------------------------------------------
// Parse #DVIskaidro listing page to discover article URLs
// ---------------------------------------------------------------------------

function parseDviskaidroListing(html: string): string[] {
  const $ = cheerio.load(html);
  const urls: string[] = [];

  $("a[href]").each((_i, el) => {
    const href = $(el).attr("href");
    if (!href) return;

    // Article links: /lv/jaunums/<slug>
    if (href.match(/^\/lv\/jaunums\/.+/) && !href.includes("#")) {
      const normalized = href.split("?")[0] ?? href;
      if (!urls.includes(normalized)) {
        urls.push(normalized);
      }
    }
  });

  return urls;
}

// ---------------------------------------------------------------------------
// Parse news/articles listing page to discover article URLs
// ---------------------------------------------------------------------------

function parseNewsListing(html: string): string[] {
  const $ = cheerio.load(html);
  const urls: string[] = [];

  $("a[href]").each((_i, el) => {
    const href = $(el).attr("href");
    if (!href) return;

    // Article and news links
    if (
      (href.match(/^\/lv\/jaunums\/.+/) || href.match(/^\/lv\/article\/.+/)) &&
      !href.includes("#")
    ) {
      const normalized = href.split("?")[0] ?? href;
      if (!urls.includes(normalized)) {
        urls.push(normalized);
      }
    }
  });

  return urls;
}

// ---------------------------------------------------------------------------
// Known guidance URLs — curated index of DVI guidance and explanatory content
//
// dvi.gov.lv publishes guidance as:
//   - #DVIskaidro articles at /lv/jaunums/dviskaidro-*
//   - Structured guidance pages at /lv/datu-aizsardziba/*
//   - FAQ at /lv/BUJ
// We crawl the #DVIskaidro listing page dynamically and supplement
// with curated structured guidance pages below.
// ---------------------------------------------------------------------------

interface GuidelineSource {
  url: string;
  reference?: string;
  type?: string;
}

const KNOWN_GUIDELINES: GuidelineSource[] = [
  // Structured guidance pages
  { url: "/lv/BUJ", type: "faq", reference: "DVI-VADLINIJAS-BUJ" },
  { url: "/lv/datu-aizsardziba", type: "guide", reference: "DVI-VADLINIJAS-DATU-AIZSARDZIBA" },
  { url: "/lv/rights-data-subject", type: "guide", reference: "DVI-VADLINIJAS-DATU-SUBJEKTU-TIESIBAS" },
  { url: "/lv/pdap", type: "guide", reference: "DVI-VADLINIJAS-PARKAPUMA-PAZINOJUMS" },
];

// ---------------------------------------------------------------------------
// News listing pages to crawl for decision announcements
// ---------------------------------------------------------------------------

const NEWS_LISTING_URLS = [
  "/lv/articles",       // General articles listing
  "/lv/category/news",  // News category
];

// ---------------------------------------------------------------------------
// Reference generation
// ---------------------------------------------------------------------------

/**
 * Generate a stable reference from a #DVIskaidro article URL slug.
 * "/lv/jaunums/dviskaidro-20012023" -> "DVI-DVISKAIDRO-20012023"
 */
function guidelineReferenceFromSlug(url: string): string {
  const slug = url.split("/").pop() ?? url;
  return `DVI-${slug.toUpperCase().replace(/[^A-Z0-9-]/g, "")}`;
}

// ---------------------------------------------------------------------------
// Database operations
// ---------------------------------------------------------------------------

function initDb(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
    console.log(`Created data directory: ${dir}`);
  }

  if (FLAG_FORCE && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    console.log(`Deleted existing database (--force)`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);

  return db;
}

function getExistingReferences(db: Database.Database): Set<string> {
  const refs = new Set<string>();
  const rows = db
    .prepare("SELECT reference FROM decisions")
    .all() as Array<{ reference: string }>;
  for (const row of rows) {
    refs.add(row.reference);
  }
  return refs;
}

function getExistingGuidelineRefs(db: Database.Database): Set<string> {
  const refs = new Set<string>();
  const rows = db
    .prepare("SELECT reference FROM guidelines WHERE reference IS NOT NULL")
    .all() as Array<{ reference: string }>;
  for (const row of rows) {
    refs.add(row.reference);
  }
  return refs;
}

function ensureTopics(db: Database.Database): void {
  const insertTopic = db.prepare(
    "INSERT OR IGNORE INTO topics (id, name_local, name_en, description) VALUES (?, ?, ?, ?)",
  );

  const insertAll = db.transaction(() => {
    for (const rule of TOPIC_RULES) {
      insertTopic.run(rule.id, rule.name_local, rule.name_en, rule.description);
    }
  });

  insertAll();
}

// ---------------------------------------------------------------------------
// Ingestion stats
// ---------------------------------------------------------------------------

interface IngestStats {
  decisionsIngested: number;
  decisionsSkipped: number;
  decisionsFailed: number;
  guidelinesIngested: number;
  guidelinesSkipped: number;
  guidelinesFailed: number;
  discoveredGuidelineUrls: number;
  discoveredNewsUrls: number;
}

// ---------------------------------------------------------------------------
// Phase 1: Ingest decisions from the /lv/lemumi table page
//
// The decisions page lists all enforcement actions in HTML tables grouped
// by year. Each row has entity name, corrective measure, PDF download link,
// date, and status. We parse the table for metadata and optionally fetch
// the PDF decision text (future enhancement). For now, we construct
// full_text from the available table metadata + corrective measure details.
// ---------------------------------------------------------------------------

async function ingestDecisionsFromTable(
  db: Database.Database,
  existingRefs: Set<string>,
  stats: IngestStats,
): Promise<void> {
  const url = `${BASE_URL}/lv/lemumi`;
  console.log(`Fetching decisions table: ${url}`);

  const res = await rateLimitedFetch(url);
  if (!res) {
    console.error("Failed to fetch decisions page");
    return;
  }

  const html = await res.text();
  const rows = parseDecisionsPage(html);
  console.log(`Found ${rows.length} decision rows in table`);

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i]!;
    console.log(`[${i + 1}/${rows.length}] Decision: ${row.reference}`);

    if (FLAG_RESUME && existingRefs.has(row.reference)) {
      console.log(`  [skip] ${row.reference} (already in DB)`);
      stats.decisionsSkipped++;
      continue;
    }

    // Build full_text from table metadata — the actual decision content
    // is in the downloadable PDF. We include what the table provides.
    const fullTextParts: string[] = [];
    fullTextParts.push(`Pārzinis/apstrādātājs: ${row.entity_name}`);
    if (row.corrective_measure) {
      fullTextParts.push(`Korektīvais līdzeklis: ${row.corrective_measure}`);
    }
    if (row.date) {
      fullTextParts.push(`Pieņemšanas datums: ${row.date}`);
    }
    if (row.status) {
      fullTextParts.push(`Lēmuma statuss: ${row.status}`);
    }
    if (row.download_url) {
      fullTextParts.push(`Lēmuma dokuments: ${row.download_url}`);
    }
    const fullText = fullTextParts.join("\n");

    // Build a title from entity + type
    const typeLabels: Record<string, string> = {
      sanction: "Naudas sods",
      warning: "Brīdinājums",
      obligation: "Uzlikts pienākums",
      ban: "Aizliegums",
      decision: "Lēmums",
    };
    const typeLabel = typeLabels[row.type] ?? "Lēmums";
    const title = `DVI ${typeLabel.toLowerCase()} — ${row.entity_name}`;

    // Detect topics and GDPR articles from all available text
    const combinedText = `${title} ${row.corrective_measure} ${row.entity_name}`;
    const topics = detectTopics(combinedText);
    const gdprArticles = extractGdprArticles(combinedText);

    // Build summary
    const summary = row.fine_amount
      ? `DVI piemēroja ${row.entity_name} naudas sodu EUR ${row.fine_amount.toLocaleString("lv-LV")}. ${row.corrective_measure}`
      : `DVI piemēroja korektīvo līdzekli ${row.entity_name}: ${row.corrective_measure}`;

    if (FLAG_DRY_RUN) {
      console.log(`  [dry-run] Would insert decision: ${row.reference}`);
      console.log(`    Title:    ${title}`);
      console.log(`    Date:     ${row.date ?? "unknown"}`);
      console.log(`    Entity:   ${row.entity_name}`);
      console.log(`    Fine:     ${row.fine_amount != null ? `EUR ${row.fine_amount.toLocaleString("lv-LV")}` : "N/A"}`);
      console.log(`    Type:     ${row.type}`);
      console.log(`    Status:   ${row.status}`);
      console.log(`    Topics:   ${topics.join(", ") || "none detected"}`);
      console.log(`    GDPR art: ${gdprArticles.join(", ") || "none detected"}`);
      stats.decisionsIngested++;
      continue;
    }

    try {
      db.prepare(`
        INSERT OR REPLACE INTO decisions
          (reference, title, date, type, entity_name, fine_amount, summary, full_text, topics, gdpr_articles, status)
        VALUES
          (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        row.reference,
        title,
        row.date,
        row.type,
        row.entity_name,
        row.fine_amount,
        summary,
        fullText,
        JSON.stringify(topics),
        JSON.stringify(gdprArticles),
        row.status,
      );
      console.log(`  [ok] Inserted decision: ${row.reference}`);
      stats.decisionsIngested++;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`  [error] Failed to insert ${row.reference}: ${msg}`);
      stats.decisionsFailed++;
    }
  }
}

// ---------------------------------------------------------------------------
// Phase 2: Discover and ingest news articles about decisions
//
// News articles at /lv/jaunums/* contain more detailed narratives about
// DVI enforcement actions. We crawl news listing pages to find articles
// that describe sanctions and decisions, then ingest them as decisions.
// ---------------------------------------------------------------------------

async function discoverDecisionNewsUrls(): Promise<string[]> {
  const discovered: string[] = [];

  console.log("Crawling news listing pages for decision-related articles...");

  for (const listingPath of NEWS_LISTING_URLS) {
    const url = `${BASE_URL}${listingPath}`;
    console.log(`  Listing: ${url}`);

    const res = await rateLimitedFetch(url);
    if (!res) continue;

    const html = await res.text();
    const articleUrls = parseNewsListing(html);

    for (const articleUrl of articleUrls) {
      const lower = articleUrl.toLowerCase();
      // Filter for decision/sanction-related articles
      if (
        lower.includes("sod") ||
        lower.includes("sankcij") ||
        lower.includes("parkapum") ||
        lower.includes("lemum") ||
        lower.includes("naudas") ||
        lower.includes("bridinajum") ||
        lower.includes("korektiv")
      ) {
        if (!discovered.includes(articleUrl)) {
          discovered.push(articleUrl);
        }
      }
    }
  }

  return discovered;
}

async function ingestDecisionFromArticle(
  db: Database.Database,
  articlePath: string,
  existingRefs: Set<string>,
  stats: IngestStats,
): Promise<void> {
  const reference = `DVI-JAUNUMS-${(articlePath.split("/").pop() ?? articlePath).toUpperCase().replace(/[^A-Z0-9-]/g, "")}`;

  if (FLAG_RESUME && existingRefs.has(reference)) {
    console.log(`  [skip] ${reference} (already in DB)`);
    stats.decisionsSkipped++;
    return;
  }

  const fullUrl = `${BASE_URL}${articlePath}`;
  console.log(`  Fetching: ${fullUrl}`);

  const res = await rateLimitedFetch(fullUrl);
  if (!res) {
    stats.decisionsFailed++;
    return;
  }

  const html = await res.text();
  const parsed = parseArticlePage(html, articlePath);
  if (!parsed) {
    stats.decisionsFailed++;
    return;
  }

  const { title, date, bodyText, summaryText } = parsed;
  const fineAmount = extractFineAmount(bodyText);
  const combinedText = `${title} ${summaryText ?? ""} ${bodyText}`;
  const topics = detectTopics(combinedText);
  const gdprArticles = extractGdprArticles(combinedText);

  // Infer type from article content
  const lower = combinedText.toLowerCase();
  let type = "decision";
  if (lower.includes("naudas sod") || lower.includes("sodu piemēr")) type = "sanction";
  else if (lower.includes("brīdinājum")) type = "warning";
  else if (lower.includes("pienākum")) type = "obligation";

  // Try to extract entity name from title
  let entityName: string | null = null;
  // Pattern: "... piemēro ... sodu SIA "XYZ" ..." or entity in quotes
  const entityMatch = title.match(/(?:SIA|AS|VAS|VSIA)\s*["""]([^"""]+)["""]/);
  if (entityMatch) {
    entityName = `${entityMatch[0]?.split('"')[0]?.split("\u201C")[0]?.split("\u201E")[0]?.trim() ?? "SIA"} "${entityMatch[1]}"`;
  }
  // Try bare SIA pattern
  if (!entityName) {
    const siaMatch = title.match(/((?:SIA|AS|VAS|VSIA)\s+["""][^"""]+["""])/);
    if (siaMatch) entityName = cleanEntityName(siaMatch[1] ?? "");
  }

  if (FLAG_DRY_RUN) {
    console.log(`  [dry-run] Would insert decision (from article): ${reference}`);
    console.log(`    Title:    ${title}`);
    console.log(`    Date:     ${date ?? "unknown"}`);
    console.log(`    Entity:   ${entityName ?? "unknown"}`);
    console.log(`    Fine:     ${fineAmount != null ? `EUR ${fineAmount.toLocaleString("lv-LV")}` : "N/A"}`);
    console.log(`    Type:     ${type}`);
    console.log(`    Topics:   ${topics.join(", ") || "none detected"}`);
    console.log(`    GDPR art: ${gdprArticles.join(", ") || "none detected"}`);
    console.log(`    Body:     ${bodyText.length} chars`);
    stats.decisionsIngested++;
    return;
  }

  try {
    db.prepare(`
      INSERT OR REPLACE INTO decisions
        (reference, title, date, type, entity_name, fine_amount, summary, full_text, topics, gdpr_articles, status)
      VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      reference,
      title,
      date,
      type,
      entityName,
      fineAmount,
      summaryText,
      bodyText,
      JSON.stringify(topics),
      JSON.stringify(gdprArticles),
      "final",
    );
    console.log(`  [ok] Inserted decision (article): ${reference}`);
    stats.decisionsIngested++;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`  [error] Failed to insert ${reference}: ${msg}`);
    stats.decisionsFailed++;
  }
}

// ---------------------------------------------------------------------------
// Phase 3: Discover and ingest #DVIskaidro guidance articles
// ---------------------------------------------------------------------------

async function discoverDviskaidroUrls(): Promise<string[]> {
  const url = `${BASE_URL}/lv/dviskaidro`;
  console.log(`Fetching #DVIskaidro listing: ${url}`);

  const res = await rateLimitedFetch(url);
  if (!res) {
    console.error("Failed to fetch #DVIskaidro listing page");
    return [];
  }

  const html = await res.text();
  return parseDviskaidroListing(html);
}

async function ingestGuideline(
  db: Database.Database,
  source: GuidelineSource | { url: string; reference?: string; type?: string },
  existingRefs: Set<string>,
  stats: IngestStats,
): Promise<void> {
  const reference = source.reference ?? guidelineReferenceFromSlug(source.url);

  if (FLAG_RESUME && existingRefs.has(reference)) {
    console.log(`  [skip] ${reference} (already in DB)`);
    stats.guidelinesSkipped++;
    return;
  }

  const fullUrl = source.url.startsWith("http") ? source.url : `${BASE_URL}${source.url}`;
  console.log(`  Fetching: ${fullUrl}`);

  const res = await rateLimitedFetch(fullUrl);
  if (!res) {
    stats.guidelinesFailed++;
    return;
  }

  const html = await res.text();
  const parsed = parseArticlePage(html, source.url);
  if (!parsed) {
    stats.guidelinesFailed++;
    return;
  }

  const { title, date, bodyText, summaryText } = parsed;
  const type = source.type ?? "dviskaidro";
  const topics = detectTopics(`${title} ${summaryText ?? ""} ${bodyText}`);

  if (FLAG_DRY_RUN) {
    console.log(`  [dry-run] Would insert guideline: ${reference}`);
    console.log(`    Title:    ${title}`);
    console.log(`    Date:     ${date ?? "unknown"}`);
    console.log(`    Type:     ${type}`);
    console.log(`    Topics:   ${topics.join(", ") || "none detected"}`);
    console.log(`    Body:     ${bodyText.length} chars`);
    stats.guidelinesIngested++;
    return;
  }

  try {
    db.prepare(`
      INSERT OR REPLACE INTO guidelines
        (reference, title, date, type, summary, full_text, topics, language)
      VALUES
        (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      reference,
      title,
      date,
      type,
      summaryText,
      bodyText,
      JSON.stringify(topics),
      "lv",
    );
    console.log(`  [ok] Inserted guideline: ${reference}`);
    stats.guidelinesIngested++;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`  [error] Failed to insert guideline ${reference}: ${msg}`);
    stats.guidelinesFailed++;
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log("=== DVI (Datu valsts inspekcija) Ingestion Crawler ===");
  console.log();
  console.log(`Database:    ${DB_PATH}`);
  console.log(`Rate limit:  ${RATE_LIMIT_MS}ms between requests`);
  console.log(`Max retries: ${MAX_RETRIES}`);
  console.log(`Flags:       ${[
    FLAG_RESUME && "--resume",
    FLAG_DRY_RUN && "--dry-run",
    FLAG_FORCE && "--force",
  ].filter(Boolean).join(" ") || "(none)"}`);
  console.log();

  // -- Init database --------------------------------------------------------
  const db = initDb();

  ensureTopics(db);
  console.log(`Ensured ${TOPIC_RULES.length} topics in database`);

  const existingDecisionRefs = getExistingReferences(db);
  const existingGuidelineRefs = getExistingGuidelineRefs(db);

  if (FLAG_RESUME) {
    console.log(`Existing decisions: ${existingDecisionRefs.size}`);
    console.log(`Existing guidelines: ${existingGuidelineRefs.size}`);
  }

  const stats: IngestStats = {
    decisionsIngested: 0,
    decisionsSkipped: 0,
    decisionsFailed: 0,
    guidelinesIngested: 0,
    guidelinesSkipped: 0,
    guidelinesFailed: 0,
    discoveredGuidelineUrls: 0,
    discoveredNewsUrls: 0,
  };

  // -- Phase 1: Ingest decisions from /lv/lemumi table ----------------------
  console.log();
  console.log("--- Phase 1: Decisions from /lv/lemumi table ---");

  await ingestDecisionsFromTable(db, existingDecisionRefs, stats);

  // Refresh existing refs after phase 1 inserts
  const updatedDecisionRefs = getExistingReferences(db);

  // -- Phase 2: Discover and ingest decision-related news articles ----------
  console.log();
  console.log("--- Phase 2: Decision articles from news pages ---");

  const newsUrls = await discoverDecisionNewsUrls();
  stats.discoveredNewsUrls = newsUrls.length;
  console.log(`Discovered ${newsUrls.length} decision-related news articles`);

  for (let i = 0; i < newsUrls.length; i++) {
    const articlePath = newsUrls[i]!;
    console.log(`[${i + 1}/${newsUrls.length}] News article: ${articlePath}`);
    await ingestDecisionFromArticle(db, articlePath, updatedDecisionRefs, stats);
  }

  // -- Phase 3: Discover and ingest #DVIskaidro guidance articles -----------
  console.log();
  console.log("--- Phase 3: #DVIskaidro guidance articles ---");

  const dviskaidroUrls = await discoverDviskaidroUrls();
  stats.discoveredGuidelineUrls = dviskaidroUrls.length;
  console.log(`Discovered ${dviskaidroUrls.length} #DVIskaidro article URLs`);

  // Merge discovered #DVIskaidro URLs with curated guidelines
  const allGuidelineSources: Array<{ url: string; reference?: string; type?: string }> = [
    ...KNOWN_GUIDELINES,
  ];

  const curatedPaths = new Set(KNOWN_GUIDELINES.map((g) => g.url));
  for (const dvUrl of dviskaidroUrls) {
    if (!curatedPaths.has(dvUrl)) {
      allGuidelineSources.push({ url: dvUrl, type: "dviskaidro" });
    }
  }

  console.log(
    `Total guideline sources: ${allGuidelineSources.length} ` +
      `(${KNOWN_GUIDELINES.length} curated + ${allGuidelineSources.length - KNOWN_GUIDELINES.length} discovered)`,
  );

  for (let i = 0; i < allGuidelineSources.length; i++) {
    const source = allGuidelineSources[i]!;
    console.log(`[${i + 1}/${allGuidelineSources.length}] Guideline: ${source.url}`);
    await ingestGuideline(db, source, existingGuidelineRefs, stats);
  }

  // -- Summary --------------------------------------------------------------
  console.log();
  console.log("=== Ingestion Complete ===");
  console.log();
  console.log(`Decisions:`);
  console.log(`  Ingested: ${stats.decisionsIngested}`);
  console.log(`  Skipped:  ${stats.decisionsSkipped}`);
  console.log(`  Failed:   ${stats.decisionsFailed}`);
  console.log();
  console.log(`Guidelines:`);
  console.log(`  Ingested: ${stats.guidelinesIngested}`);
  console.log(`  Skipped:  ${stats.guidelinesSkipped}`);
  console.log(`  Failed:   ${stats.guidelinesFailed}`);
  console.log();
  console.log(`Discovery:`);
  console.log(`  News article URLs: ${stats.discoveredNewsUrls}`);
  console.log(`  #DVIskaidro URLs:  ${stats.discoveredGuidelineUrls}`);

  const decisionCount = (
    db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }
  ).cnt;
  const guidelineCount = (
    db.prepare("SELECT count(*) as cnt FROM guidelines").get() as { cnt: number }
  ).cnt;
  const topicCount = (
    db.prepare("SELECT count(*) as cnt FROM topics").get() as { cnt: number }
  ).cnt;

  console.log();
  console.log(`Database totals:`);
  console.log(`  Topics:     ${topicCount}`);
  console.log(`  Decisions:  ${decisionCount}`);
  console.log(`  Guidelines: ${guidelineCount}`);

  db.close();

  console.log();
  console.log(`Database: ${DB_PATH}`);
}

main().catch((err) => {
  console.error("Fatal error:", err instanceof Error ? err.message : String(err));
  if (err instanceof Error && err.stack) {
    console.error(err.stack);
  }
  process.exit(1);
});
