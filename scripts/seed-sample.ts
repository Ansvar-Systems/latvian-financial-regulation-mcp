/**
 * Seed the FKTK database with sample provisions for testing.
 *
 * Inserts representative provisions from FKTK_Noteikumi, FKTK_Ieteikumi,
 * and FKTK_Vadlinijas sourcebooks so MCP tools can be tested
 * without running a full ingest.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force   # drop and recreate
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["FKTK_DB_PATH"] ?? "data/fktk.db";
const force = process.argv.includes("--force");

const dir = dirname(DB_PATH);
if (!existsSync(dir)) {
  mkdirSync(dir, { recursive: true });
}

if (force && existsSync(DB_PATH)) {
  unlinkSync(DB_PATH);
  console.log(`Deleted existing database at ${DB_PATH}`);
}

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);

console.log(`Database initialised at ${DB_PATH}`);

// --- Sourcebooks ---

interface SourcebookRow {
  id: string;
  name: string;
  description: string;
}

const sourcebooks: SourcebookRow[] = [
  {
    id: "FKTK_NOTEIKUMI",
    name: "FKTK Noteikumi",
    description:
      "Finansu un kapitala tirgus komisijas noteikumi — riska parvaldiba, IT drosiba, kapitala pietiekamiba, un korporativa parvaldiba.",
  },
  {
    id: "FKTK_IETEIKUMI",
    name: "FKTK Ieteikumi",
    description:
      "Finansu un kapitala tirgus komisijas ieteikumi — labas prakses standarti un brivpraligakas atbilstibas vadlinijas.",
  },
  {
    id: "FKTK_VADLINIJAS",
    name: "FKTK Vadlinijas",
    description:
      "Finansu un kapitala tirgus komisijas vadlinijas — uzraudzibas sagaidas, piemeribu nosacini, un skaidrojosi materiali.",
  },
];

const insertSourcebook = db.prepare(
  "INSERT OR IGNORE INTO sourcebooks (id, name, description) VALUES (?, ?, ?)",
);

for (const sb of sourcebooks) {
  insertSourcebook.run(sb.id, sb.name, sb.description);
}

console.log(`Inserted ${sourcebooks.length} sourcebooks`);

// --- Sample provisions ---

interface ProvisionRow {
  sourcebook_id: string;
  reference: string;
  title: string;
  text: string;
  type: string;
  status: string;
  effective_date: string;
  chapter: string;
  section: string;
}

const provisions: ProvisionRow[] = [
  // FKTK Noteikumi — regulations
  {
    sourcebook_id: "FKTK_NOTEIKUMI",
    reference: "FKTK noteikumi 100",
    title: "Noteikumi par riska parvaldibu un ieksejas kontroles sistema",
    text: "Kreditorstacijas un apdrosanatjas uznemumos jaizveidot visaptverosha riska parvaldbas sistema. Sistema ietver riska identifikaciju, novertejumu, monitoringu un zinojoanu, ka ari stresatestu veikshanu. Padome un valde ir atbildiga par riska kulturali un tonas vadibai.",
    type: "noteikumi",
    status: "in_force",
    effective_date: "2016-01-01",
    chapter: "III",
    section: "3.1",
  },
  {
    sourcebook_id: "FKTK_NOTEIKUMI",
    reference: "FKTK noteikumi 125",
    title: "Noteikumi par IT drosibas prasibam finansu institucijam",
    text: "Finansu institucijam jabut apstiprinatai IT drosibas politikai un standartiem. Obligatas ir kiberdrosibu incidentu atklasanas un reaaksijas procedurusas, regulari penetraciju testesanas, un darbinieku apmaciba. Buti incidenti 4 stundu laika jazinosanas FKTK.",
    type: "noteikumi",
    status: "in_force",
    effective_date: "2020-07-01",
    chapter: "IV",
    section: "4.2",
  },
  {
    sourcebook_id: "FKTK_NOTEIKUMI",
    reference: "FKTK noteikumi 87",
    title: "Noteikumi par korporativo parvaldibu un padomes prasibam",
    text: "Finansu instituciju pades locekliem jabut atbilstoshamiem gan zinu un pieredzes, gan nelabpratibas zinapas. Valdes darba organizacijai jabut skaidrai noteiktai ar reglamenta. Atalgojuma politikai jaatbalsta ilgtermineja riska parvaldibu un jaizvairas no stimuliem uz parmerigiem riskiem.",
    type: "noteikumi",
    status: "in_force",
    effective_date: "2013-01-01",
    chapter: "II",
    section: "2.4",
  },
  {
    sourcebook_id: "FKTK_NOTEIKUMI",
    reference: "FKTK noteikumi 142",
    title: "Noteikumi par naudas atmazgasanas novershanu",
    text: "Visiem FKTK uzraudzitajiem subjektiem jabut ieviestai riskos balstitai AML/CFT sistesmai. Klienta uzticamibu parbaude (KYC/CDD) javeic pirms darbibu uzsaksanas un periodiski ta laika. Aizdomigam darijumiem jatiek nekavejoties zinotiem Finansintelligences dienestam.",
    type: "noteikumi",
    status: "in_force",
    effective_date: "2018-11-01",
    chapter: "I",
    section: "1.2",
  },

  // FKTK Ieteikumi — recommendations
  {
    sourcebook_id: "FKTK_IETEIKUMI",
    reference: "FKTK ieteikumi 2022/1",
    title: "Ieteikumi par ESG risku integrasanu uzraudzibas procesaos",
    text: "FKTK iesaka finansu institicijam integreet ESG riskus sava riska parvaldibu un stratejiskajas planotsanas. Klimata riski jaikleuj kreditvides veerteejumos, stresa testos un ICAAP procesaos. Institucijam jatiek atklatas ESG riska apjomu un riska parvaldibas metodes.",
    type: "ieteikumi",
    status: "in_force",
    effective_date: "2022-06-01",
    chapter: "II",
    section: "2.1",
  },
  {
    sourcebook_id: "FKTK_IETEIKUMI",
    reference: "FKTK ieteikumi 2020/3",
    title: "Ieteikumi par klientu aizsardzibu finansu pakalpojumu snigsana",
    text: "FKTK iesaka finansu pakalpojumu sniedzejiem nodroshinet skaidru un godigu sazibu ar klientiem. Produktu apraksti jaraksta vienkarshu valoda, maxsas un komisijas jasataiek atklatas pirms liuguma. Sudzibu izskatroshanas proceduram jabut iekshejam un parramigas.",
    type: "ieteikumi",
    status: "in_force",
    effective_date: "2020-10-01",
    chapter: "I",
    section: "1.3",
  },

  // FKTK Vadlinijas — guidelines
  {
    sourcebook_id: "FKTK_VADLINIJAS",
    reference: "FKTK vadlinijas 2023/2",
    title: "Vadlinijas par maaksligo intelektu finansu sektora",
    text: "FKTK sagaida, ka finansu institucijas, kas izmanto MI sistemas (piem. kreditpunktu skaitsana, krapsanas nosaksana), nodrosina sist emu caurskatamiba un izskaidrjamiba. Institucijam jabut spejigam izskaidrot automatizetu lemumu pienemshanu klientiem. MI modeliem regularaja veiksmana validacija un kontrole ir obligata.",
    type: "vadlinijas",
    status: "in_force",
    effective_date: "2023-03-01",
    chapter: "II",
    section: "2.3",
  },
  {
    sourcebook_id: "FKTK_VADLINIJAS",
    reference: "FKTK vadlinijas 2021/1",
    title: "Vadlinijas par maka pakalpojumu un fintech sadarbibas regulejumu",
    text: "FKTK skaidro, ka bankas un citi uzraudzitie subjekti, nodibinot partnerattiecibas ar fintech uznemumiem, saglaba pilnu atbildibu par riska parvaldibu. Tresha puse sniegtie pakalpojumi janoverte gan pirms liguma slegshanas, gan ik gadu. Kritisku pakalpojumu traucejumam jab planam reaaksijas plani.",
    type: "vadlinijas",
    status: "in_force",
    effective_date: "2021-04-01",
    chapter: "III",
    section: "3.4",
  },
  {
    sourcebook_id: "FKTK_NOTEIKUMI",
    reference: "FKTK noteikumi 160",
    title: "Noteikumi par kapitala pietiekamibu un SREP procesu",
    text: "Visi kreditiestades un apdrosanataju uznemumam ar FKTK atbalstu jabut paklauti ikgadejam Uzraudzibas parbaudes un novertejuma procesam (SREP). SREP novertee kapitala pietiekamibas iekshejo novertejumu (ICAAP), likviditates iekshejo novertejumu (ILAAP) un biznesa modela ilgtspejibas.",
    type: "noteikumi",
    status: "in_force",
    effective_date: "2019-01-01",
    chapter: "V",
    section: "5.1",
  },
  {
    sourcebook_id: "FKTK_VADLINIJAS",
    reference: "FKTK vadlinijas 2024/1",
    title: "Vadlinijas par DORA (Digitala operacionala noturibas akts) ievieshanu",
    text: "Sinas vadlinijas skaidro FKTK sagaidas attieciba uz ES Digitalas operacionalas noturibas akta (DORA) prakstisku ievieshanu Latvijas finansu institicijam. Tiek apskatita IKT riska parvaldibas struktukra, IKT incidentu zinosana, digitala operacionalas noturibas testesana un IKT tresho pushu riska parvaldibu.",
    type: "vadlinijas",
    status: "in_force",
    effective_date: "2025-01-17",
    chapter: "I",
    section: "1.1",
  },
];

const insertProvision = db.prepare(`
  INSERT INTO provisions (sourcebook_id, reference, title, text, type, status, effective_date, chapter, section)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertAll = db.transaction(() => {
  for (const p of provisions) {
    insertProvision.run(
      p.sourcebook_id,
      p.reference,
      p.title,
      p.text,
      p.type,
      p.status,
      p.effective_date,
      p.chapter,
      p.section,
    );
  }
});

insertAll();
console.log(`Inserted ${provisions.length} sample provisions`);

// --- Sample enforcement actions ---

interface EnforcementRow {
  firm_name: string;
  reference_number: string;
  action_type: string;
  amount: number;
  date: string;
  summary: string;
  sourcebook_references: string;
}

const enforcements: EnforcementRow[] = [
  {
    firm_name: "AS Citadele banka",
    reference_number: "FKTK-ENF-2021-0089",
    action_type: "fine",
    amount: 1_820_000,
    date: "2021-07-15",
    summary:
      "FKTK uzlika AS Citadele banka 1 820 000 EUR sodu par but rnauciskam AML/CFT proceduru nepilnibam. Banka laika periodaa 2018–2020 neizveica pietiekamu uzticamibu parbaudi augsta riska klientiem un sistematiski neuzraudzija klientu darbibu, kas rada bativus naudas atmazgashanas riskus.",
    sourcebook_references: "FKTK noteikumi 142, FKTK noteikumi 100",
  },
  {
    firm_name: "SIA Monify (maksajumu institicija)",
    reference_number: "FKTK-ENF-2023-0034",
    action_type: "fine",
    amount: 95_000,
    date: "2023-11-08",
    summary:
      "FKTK uzlika SIA Monify 95 000 EUR sodu par IT incidentu zinosanas prasibas neieveeroshanu. Uznemums neinformeja FKTK 4 stundu laika par kritisku maksajumu apstrasades sistemu pararukumu, kas skara vairak neka 15 000 klientus, un nebija izstradajis atbilstoshu verslas testinumo planu.",
    sourcebook_references: "FKTK noteikumi 125, FKTK vadlinijas 2021/1",
  },
];

const insertEnforcement = db.prepare(`
  INSERT INTO enforcement_actions (firm_name, reference_number, action_type, amount, date, summary, sourcebook_references)
  VALUES (?, ?, ?, ?, ?, ?, ?)
`);

const insertEnforcementsAll = db.transaction(() => {
  for (const e of enforcements) {
    insertEnforcement.run(
      e.firm_name,
      e.reference_number,
      e.action_type,
      e.amount,
      e.date,
      e.summary,
      e.sourcebook_references,
    );
  }
});

insertEnforcementsAll();
console.log(`Inserted ${enforcements.length} sample enforcement actions`);

// --- Summary ---

const provisionCount = (
  db.prepare("SELECT count(*) as cnt FROM provisions").get() as { cnt: number }
).cnt;
const sourcebookCount = (
  db.prepare("SELECT count(*) as cnt FROM sourcebooks").get() as { cnt: number }
).cnt;
const enforcementCount = (
  db.prepare("SELECT count(*) as cnt FROM enforcement_actions").get() as { cnt: number }
).cnt;
const ftsCount = (
  db.prepare("SELECT count(*) as cnt FROM provisions_fts").get() as { cnt: number }
).cnt;

console.log(`\nDatabase summary:`);
console.log(`  Sourcebooks:          ${sourcebookCount}`);
console.log(`  Provisions:           ${provisionCount}`);
console.log(`  Enforcement actions:  ${enforcementCount}`);
console.log(`  FTS entries:          ${ftsCount}`);
console.log(`\nDone. Database ready at ${DB_PATH}`);

db.close();
