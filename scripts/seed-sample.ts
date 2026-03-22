/**
 * Seed the DVI database with sample decisions and guidelines for testing.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force   # drop and recreate
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["DVI_DB_PATH"] ?? "data/dvi.db";
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

interface TopicRow { id: string; name_local: string; name_en: string; description: string; }

const topics: TopicRow[] = [
  { id: "cookies", name_local: "Sīkdatnes un izsekotāji", name_en: "Cookies and trackers", description: "Sīkdatņu un citu izsekotāju izmantošana lietotāju ierīcēs (VDAR 6. pants)." },
  { id: "employee_monitoring", name_local: "Darbinieku uzraudzība", name_en: "Employee monitoring", description: "Darbinieku datu apstrāde un uzraudzība darba vietā." },
  { id: "video_surveillance", name_local: "Videonovērošana", name_en: "Video surveillance", description: "Videonovērošanas sistēmu izmantošana un personas datu aizsardzība (VDAR 6. pants)." },
  { id: "data_breach", name_local: "Datu drošības pārkāpumi", name_en: "Data breach notification", description: "Paziņošana par personas datu drošības pārkāpumiem DVI un datu subjektiem (VDAR 33.–34. pants)." },
  { id: "consent", name_local: "Piekrišana", name_en: "Consent", description: "Piekrišanas personas datu apstrādei iegūšana, spēkā esamība un atsaukšana (VDAR 7. pants)." },
  { id: "dpia", name_local: "Ietekmes novērtējums", name_en: "Data Protection Impact Assessment (DPIA)", description: "Ietekmes uz datu aizsardzību novērtējums augsta riska apstrādei (VDAR 35. pants)." },
  { id: "transfers", name_local: "Starptautiskie datu nosūtījumi", name_en: "International data transfers", description: "Personas datu nosūtīšana uz trešajām valstīm vai starptautiskajām organizācijām (VDAR 44.–49. pants)." },
  { id: "data_subject_rights", name_local: "Datu subjektu tiesības", name_en: "Data subject rights", description: "Piekļuves, labošanas, dzēšanas un citu tiesību īstenošana (VDAR 15.–22. pants)." },
];

const insertTopic = db.prepare("INSERT OR IGNORE INTO topics (id, name_local, name_en, description) VALUES (?, ?, ?, ?)");
for (const t of topics) { insertTopic.run(t.id, t.name_local, t.name_en, t.description); }
console.log(`Inserted ${topics.length} topics`);

interface DecisionRow {
  reference: string; title: string; date: string; type: string;
  entity_name: string; fine_amount: number | null; summary: string;
  full_text: string; topics: string; gdpr_articles: string; status: string;
}

const decisions: DecisionRow[] = [
  {
    reference: "DVI-2022-007",
    title: "DVI lēmums par sīkdatņu pārkāpumiem tīmekļvietnē",
    date: "2022-05-20",
    type: "sanction",
    entity_name: "E-komercijas uzņēmums",
    fine_amount: 12000,
    summary: "DVI uzlika 12 000 EUR sodu e-komercijas uzņēmumam par analītisko un reklāmas sīkdatņu izmantošanu bez iepriekšējas lietotāju piekrišanas un nepiedāvājot vienkāršu atteikšanās iespēju.",
    full_text: "Datu valsts inspekcija veica pārbaudi pēc vairāku lietotāju sūdzībām. Tika konstatēts, ka uzņēmums aktivizēja reklāmas un analītiskās sīkdatnes uzreiz pēc tīmekļvietnes atvēršanas, pirms lietotājs varēja izvēlēties piekrišanas opciju. Piekrišanas baneri uzrādīja akcentu uz piekrišanas pogu, bet atteikšanās opcija bija mazāk pamanāma un prasīja papildu klikšķus. DVI konstatēja: 1) sīkdatnes tika aktivizētas pirms piekrišanas saņemšanas; 2) atteikšanās process bija sarežģītāks nekā piekrišanas; 3) informācija par sīkdatņu mērķiem bija nepilnīga. Uzņēmumam tika uzlikts 12 000 EUR sods un noteikts pienākums novērst pārkāpumus 60 dienu laikā.",
    topics: JSON.stringify(["cookies", "consent"]),
    gdpr_articles: JSON.stringify(["6", "7"]),
    status: "final",
  },
  {
    reference: "DVI-2022-015",
    title: "DVI lēmums par darbinieku GPS izsekošanu",
    date: "2022-09-12",
    type: "sanction",
    entity_name: "Transporta uzņēmums",
    fine_amount: 20000,
    summary: "DVI uzlika 20 000 EUR sodu transporta uzņēmumam par nepārtrauktu GPS izsekošanu darbiniekiem gan darba, gan brīvajā laikā, pārkāpjot proporcionalitātes principu.",
    full_text: "DVI saņēma sūdzības no uzņēmuma darbiniekiem par nepārtrauktu GPS izsekošanu, izmantojot transportlīdzekļu pārvaldības sistēmu. Izmeklēšana atklāja: 1) GPS dati tika vākti 24 stundas diennaktī 7 dienas nedēļā, tostarp ārpus darba laika un brīvdienās; 2) darbinieki nebija pienācīgi informēti par apstrādes apjomu pirms sistēmas ieviešanas; 3) dati tika glabāti 3 gadus bez pamatota iemesla. DVI uzsvēra, ka GPS izsekošana ir pieļaujama tikai darba laikā un konkrētiem likumīgiem mērķiem. Uzņēmumam tika uzlikts 20 000 EUR sods un noteikts pienākums ierobežot izsekošanu darba laikam.",
    topics: JSON.stringify(["employee_monitoring"]),
    gdpr_articles: JSON.stringify(["5", "6", "13"]),
    status: "final",
  },
  {
    reference: "DVI-2023-003",
    title: "DVI lēmums par datu pārkāpuma paziņošanas kavēšanos",
    date: "2023-01-30",
    type: "sanction",
    entity_name: "Veselības aprūpes iestāde",
    fine_amount: 35000,
    summary: "DVI uzlika 35 000 EUR sodu veselības aprūpes iestādei par datu pārkāpuma paziņošanas kavēšanos — paziņojums tika iesniegts 12 dienas pēc incidenta atklāšanas, nevis 72 stundu laikā, kā noteikts VDAR.",
    full_text: "Veselības aprūpes iestāde cieta no kiberuzbrukuma, kura rezultātā tika apdraudēti aptuveni 15 000 pacientu personas dati, tostarp medicīniskā informācija. DVI konstatēja šādus pārkāpumus: 1) paziņojums DVI tika iesniegts 12 dienas pēc incidenta konstatēšanas, pārkāpjot 72 stundu termiņu; 2) paziņojums bija nepilnīgs — tajā nebija norādīts skartās personas datu veids, skaits un riska novērtējums; 3) skartie pacienti netika informēti, kaut gan incidents radīja augstu risku viņu tiesībām. Uzņēmumam tika uzlikts 35 000 EUR sods. DVI uzsvēra: medicīniskās informācijas pārkāpumi ir īpaši smagi, jo tie var izraisīt nopietnas sekas cietušajiem.",
    topics: JSON.stringify(["data_breach"]),
    gdpr_articles: JSON.stringify(["33", "34"]),
    status: "final",
  },
  {
    reference: "DVI-2023-021",
    title: "DVI lēmums par videonovērošanu darba vietā",
    date: "2023-06-15",
    type: "warning",
    entity_name: "Mazumtirdzniecības tīkls",
    fine_amount: null,
    summary: "DVI izteica brīdinājumu mazumtirdzniecības tīklam par videonovērošanas kameru uzstādīšanu darbinieku atpūtas telpās un nepietiekamu informēšanu par videonovērošanu.",
    full_text: "DVI veica pārbaudes mazumtirdzniecības veikalos un atklāja, ka videonovērošanas kameras bija uzstādītas darbinieku atpūtas telpās — ģērbtuves un kafejnīcā. Tas ir acīmredzams proporcionalitātes principa pārkāpums, jo nav tiesiska pamata tik intensīvai uzraudzībai privātās darbinieku zonās. Turklāt darbinieki nebija pienācīgi informēti par kameru atrašanās vietām un apstrādāto datu apjomu. DVI izteica brīdinājumu un uzdeva: 1) nekavējoties noņemt kameras no darbinieku atpūtas telpām; 2) pārskatīt videonovērošanas politiku; 3) sagatavot un publicēt skaidru informāciju darbiniekiem.",
    topics: JSON.stringify(["video_surveillance", "employee_monitoring"]),
    gdpr_articles: JSON.stringify(["5", "6", "13"]),
    status: "final",
  },
  {
    reference: "DVI-2023-044",
    title: "DVI lēmums par tiešā mārketinga vēstuļu sūtīšanu bez piekrišanas",
    date: "2023-11-08",
    type: "sanction",
    entity_name: "Apdrošināšanas sabiedrība",
    fine_amount: 18000,
    summary: "DVI uzlika 18 000 EUR sodu apdrošināšanas sabiedrībai par tiešā mārketinga e-pastu sūtīšanu klientiem bez derīgas piekrišanas un nenodrošinot vieglu atteikšanās iespēju.",
    full_text: "DVI izmeklēja sūdzības no vairākiem patērētājiem, kuri saņēma nevēlamus mārketinga e-pastus no apdrošināšanas sabiedrības. Izmeklēšana atklāja: 1) sabiedrība sūtīja mārketinga ziņojumus personām, kuras nebija skaidri piekritušas saņemt šādus ziņojumus — piekrišana tika iegūta, izmantojot iepriekš atzīmētas izvēles rūtiņas; 2) atteikšanās no abonēšanas saite bija paslēpta e-pasta apakšdaļā ar mazu fontu; 3) daži patērētāji ziņoja, ka pēc atteikšanās vēstules turpināja saņemt vairākas nedēļas. DVI uzsvēra, ka piekrišana mārketinga ziņojumiem jāiegūst aktīvas darbības veidā. Sabiedrībai tika uzlikts 18 000 EUR sods.",
    topics: JSON.stringify(["consent"]),
    gdpr_articles: JSON.stringify(["6", "7"]),
    status: "final",
  },
];

const insertDecision = db.prepare(`INSERT OR IGNORE INTO decisions (reference, title, date, type, entity_name, fine_amount, summary, full_text, topics, gdpr_articles, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
const insertDecisionsAll = db.transaction(() => { for (const d of decisions) { insertDecision.run(d.reference, d.title, d.date, d.type, d.entity_name, d.fine_amount, d.summary, d.full_text, d.topics, d.gdpr_articles, d.status); } });
insertDecisionsAll();
console.log(`Inserted ${decisions.length} decisions`);

interface GuidelineRow { reference: string | null; title: string; date: string; type: string; summary: string; full_text: string; topics: string; language: string; }

const guidelines: GuidelineRow[] = [
  {
    reference: "DVI-VADLĪNIJAS-SĪKDATNES-2022",
    title: "Vadlīnijas sīkdatņu izmantošanai",
    date: "2022-03-15",
    type: "guide",
    summary: "DVI vadlīnijas par sīkdatņu un citu izsekotāju izmantošanu. Ietver piekrišanas prasības, informēšanu un atteikšanās mehānismus.",
    full_text: "Šīs vadlīnijas skaidro prasības sīkdatņu izmantošanai Latvijā saskaņā ar VDAR un Elektronisko sakaru likumu. Galvenās prasības: 1) Piekrišana pirms sīkdatnēm — nebūtiskām sīkdatnēm (reklāmas, analītikas) nepieciešama iepriekšēja, skaidra un aktīva lietotāja piekrišana; 2) Vienlīdzīga piekļuve — jānodrošina vienlīdz vienkārša iespēja gan piekrist, gan atteikties no sīkdatnēm; 3) Informācija — skaidra informācija par sīkdatņu mērķiem, ilgumu un trešajām pusēm; 4) Atsaukšana — lietotājiem jābūt iespējai jebkurā laikā atsaukt savu piekrišanu.",
    topics: JSON.stringify(["cookies", "consent"]),
    language: "lv",
  },
  {
    reference: "DVI-VADLĪNIJAS-DPIA-2021",
    title: "Ietekmes uz datu aizsardzību novērtējuma veikšanas vadlīnijas",
    date: "2021-09-20",
    type: "guide",
    summary: "DVI metodiskās vadlīnijas par ietekmes uz datu aizsardzību novērtējumu (IDAP/DPIA). Ietver, kad DPIA ir obligāts, kā to veikt un dokumentēt.",
    full_text: "VDAR 35. pants pieprasa veikt ietekmes uz datu aizsardzību novērtējumu, ja apstrāde var radīt augstu risku fizisko personu tiesībām un brīvībām. DPIA ir obligāts: apstrādājot biometriskus vai veselības datus lielā apjomā; sistemātiski uzraugot publiskas vietas; apstrādājot datus automatizētu lēmumu pieņemšanai ar juridiskām sekām. DPIA posmi: 1) Apstrādes apraksts — datu kategorijas, mērķi, saņēmēji, nosūtīšana, glabāšanas termiņš; 2) Nepieciešamības un proporcionalitātes novērtējums; 3) Risku pārvaldība — draudu identificēšana, iespējamības un smaguma novērtēšana, papildu pasākumu noteikšana.",
    topics: JSON.stringify(["dpia"]),
    language: "lv",
  },
  {
    reference: "DVI-VADLĪNIJAS-DATU-SUBJEKTI-2022",
    title: "Datu subjektu tiesību īstenošanas vadlīnijas",
    date: "2022-07-01",
    type: "guide",
    summary: "DVI vadlīnijas par datu subjektu tiesību — piekļuves, labošanas, dzēšanas, ierobežošanas, pārnesamības un iebildumu — īstenošanu.",
    full_text: "VDAR piešķir datu subjektiem plašas tiesības attiecībā uz viņu personas datu apstrādi. Galvenās tiesības: 1) Tiesības piekļūt datiem (15. pants) — persona ir tiesīga saņemt apstiprinājumu par datu apstrādi un to kopiju; atbilde sniedzama 1 mēneša laikā; 2) Tiesības labot (16. pants) — neprecīzi dati jālabo bez nepamatotas kavēšanās; 3) Tiesības dzēst (17. pants) — 'tiesības tikt aizmirstam' noteiktos apstākļos; 4) Tiesības ierobežot apstrādi (18. pants); 5) Tiesības uz datu pārnesamību (20. pants); 6) Tiesības iebilst (21. pants). Organizācijām jābūt skaidrām procedūrām šo tiesību īstenošanai un jāatbild noteiktajos termiņos.",
    topics: JSON.stringify(["data_subject_rights"]),
    language: "lv",
  },
];

const insertGuideline = db.prepare(`INSERT INTO guidelines (reference, title, date, type, summary, full_text, topics, language) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
const insertGuidelinesAll = db.transaction(() => { for (const g of guidelines) { insertGuideline.run(g.reference, g.title, g.date, g.type, g.summary, g.full_text, g.topics, g.language); } });
insertGuidelinesAll();
console.log(`Inserted ${guidelines.length} guidelines`);

const decisionCount = (db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }).cnt;
const guidelineCount = (db.prepare("SELECT count(*) as cnt FROM guidelines").get() as { cnt: number }).cnt;
const topicCount = (db.prepare("SELECT count(*) as cnt FROM topics").get() as { cnt: number }).cnt;
console.log(`\nDatabase summary:\n  Topics: ${topicCount}\n  Decisions: ${decisionCount}\n  Guidelines: ${guidelineCount}\n\nDone. Database ready at ${DB_PATH}`);
db.close();
