process.env.TZ = 'Asia/Dhaka';

// =============================================
//   FANFLIX BOT v6.1 - FINAL CLEAN
// =============================================

const express     = require('express');
const TelegramBot = require('node-telegram-bot-api');
const Database    = require('better-sqlite3');
const cron        = require('node-cron');
const axios       = require('axios');
const crypto      = require('crypto');
const fs          = require('fs');
const config      = require('./config');

// =============================================================
//  DATABASE
// =============================================================

const DB_DIR        = '/app/data';
const DB_PATH       = '/app/data/fanflix.db';
const CONTACTS_FILE = '/app/data/contacts.txt';

if (!fs.existsSync(DB_DIR)) fs.mkdirSync(DB_DIR, { recursive: true });

const db = new Database(DB_PATH);

// Migrations
try { db.exec(`ALTER TABLE pending_orders ADD COLUMN cancelled INTEGER DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE pending_orders ADD COLUMN products TEXT DEFAULT '[]'`); } catch(e) {}
try { db.exec(`ALTER TABLE pending_orders ADD COLUMN discount_sent INTEGER DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE pending_orders ADD COLUMN cancelled_at TEXT`); } catch(e) {}
try { db.exec(`ALTER TABLE customers ADD COLUMN store_amount REAL DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE payments ADD COLUMN store_amount REAL DEFAULT 0`); } catch(e) {}
try { db.exec(`UPDATE customers SET store_amount = amount * 0.977 WHERE store_amount = 0 AND amount > 0`); } catch(e) {}
try { db.exec(`UPDATE pending_orders SET products = '[]' WHERE products IS NULL`); } catch(e) {}

db.exec(`
  CREATE TABLE IF NOT EXISTS customers (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    name              TEXT,
    phone             TEXT,
    email             TEXT,
    product           TEXT,
    product_type      TEXT,
    variant           TEXT,
    order_id          TEXT,
    order_name        TEXT,
    amount            REAL,
    store_amount      REAL DEFAULT 0,
    duration_days     INTEGER,
    start_date        TEXT,
    expiry_date       TEXT,
    renewal_count     INTEGER DEFAULT 1,
    is_vip            INTEGER DEFAULT 0,
    reminder_1_sent   INTEGER DEFAULT 0,
    lost_alert_sent   INTEGER DEFAULT 0,
    created_at        TEXT DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS payments (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    eps_txn_id   TEXT UNIQUE,
    phone        TEXT,
    amount       REAL,
    store_amount REAL DEFAULT 0,
    status       TEXT,
    created_at   TEXT DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS pending_orders (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    shopify_order_id TEXT UNIQUE,
    order_name       TEXT,
    name             TEXT,
    phone            TEXT,
    email            TEXT,
    products         TEXT DEFAULT '[]',
    amount           REAL,
    followup_sent    INTEGER DEFAULT 0,
    paid             INTEGER DEFAULT 0,
    cancelled        INTEGER DEFAULT 0,
    cancelled_at     TEXT,
    discount_sent    INTEGER DEFAULT 0,
    created_at       TEXT DEFAULT CURRENT_TIMESTAMP
  );
`);

// =============================================================
//  SMS MESSAGES
// =============================================================

const SMS_MSG1 = (product) =>
  `প্রিয় গ্রাহক,\n\nআপনার ${product} সাবস্ক্রিপশনটি আগামীকাল মেয়াদ শেষ হয়ে যাবে।\n\nসার্ভিস বন্ধ হওয়ার আগেই রিনিউ করুন এবং বিরতিহীন বিনোদন উপভোগ করতে থাকুন।\n\nরিনিউ করতে যোগাযোগ করুন:\n📲 WhatsApp: wa.me/+8801928382918\n\nঅথবা সরাসরি অর্ডার করুন:\n🌐 fanflixbd.com\n\n— FanFlix BD`;

const SMS_FOLLOWUP =
  `প্রিয় গ্রাহক,\n\nআপনি সম্প্রতি FanFlix-এ একটি অর্ডার করেছেন, কিন্তু পেমেন্টটি এখনো সম্পন্ন হয়নি। আপনার অর্ডারটি পেন্ডিং অবস্থায় রয়েছে।\n\nএখনই পেমেন্ট সম্পন্ন করুন:\n💳 https://pg.eps.com.bd/DefaultPaymentLink?id=805A9AEE\n\nযেকোনো সহায়তার জন্য WhatsApp করুন:\n📲 wa.me/+8801928382918\n\n— FanFlix BD`;

const SMS_DISCOUNT =
  `প্রিয় গ্রাহক,\n\nআপনি আগে FanFlix থেকে একটি অর্ডার করেছিলেন কিন্তু সম্পন্ন করেননি। আমরা আপনাকে আবার স্বাগত জানাতে চাই!\n\n🎁 শুধুমাত্র আপনার জন্য বিশেষ ১০% ছাড়!\n\nঅর্ডার করার সময় এই কোডটি ব্যবহার করুন:\n✅ কোড: WELCOMEBACK10\n\nএখনই অর্ডার করুন:\n🌐 fanflixbd.com\n\nযেকোনো সহায়তায়:\n📲 wa.me/+8801928382918\n\n— FanFlix BD`;

// SMS time restriction: only send between 11 AM - 12:10 AM
function canSendSMS() {
  const hour = new Date().getHours();
  const min  = new Date().getMinutes();
  // Allow: 11:00 AM (11) to 12:10 AM (0:10)
  if (hour >= 11) return true;
  if (hour === 0 && min <= 10) return true;
  return false;
}

// =============================================================
//  TELEGRAM
// =============================================================

const bot = new TelegramBot(config.TELEGRAM_BOT_TOKEN, { polling: true });

async function safeSend(text) {
  try {
    return await bot.sendMessage(config.TELEGRAM_CHAT_ID, text, { parse_mode: 'Markdown' });
  } catch(e) {
    return await bot.sendMessage(config.TELEGRAM_CHAT_ID, text.replace(/[*_`\[\]]/g, ''));
  }
}

async function sendAutoDelete(chatId, text, opts = {}) {
  try {
    const sent = await bot.sendMessage(chatId, text, opts);
    setTimeout(() => bot.deleteMessage(chatId, sent.message_id).catch(() => {}), 60000);
    return sent;
  } catch(e) { console.error('sendAutoDelete:', e.message); }
}

function isOwner(msg) {
  return String(msg.chat.id) === String(config.TELEGRAM_CHAT_ID);
}

// =============================================================
//  UTILS
// =============================================================

function decryptEPS(data) {
  const [ivBase64, cipherBase64] = data.split(':');
  const iv  = Buffer.from(ivBase64, 'base64');
  const ct  = Buffer.from(cipherBase64, 'base64');
  const key = Buffer.alloc(32);
  Buffer.from(config.EPS_SECRET_KEY, 'utf8').copy(key);
  const dec = crypto.createDecipheriv('aes-256-cbc', key, iv);
  return JSON.parse(Buffer.concat([dec.update(ct), dec.final()]).toString('utf8'));
}

function normalizePhone(raw = '') {
  let p = String(raw).replace(/\D/g, '');
  if (p.startsWith('880')) p = p.slice(3);
  if (p.startsWith('0'))   p = p.slice(1);
  return p;
}

function isBDPhone(phone) {
  const p = normalizePhone(phone);
  return p.length === 10 && ['13','14','15','16','17','18','19'].some(pfx => p.startsWith(pfx));
}

function saveContact(phone, name = '') {
  try {
    const p = normalizePhone(phone);
    if (!isBDPhone(p)) return;
    const full     = '0' + p;
    const existing = fs.existsSync(CONTACTS_FILE) ? fs.readFileSync(CONTACTS_FILE, 'utf8') : '';
    if (!existing.includes(full)) {
      fs.appendFileSync(CONTACTS_FILE, `${full}${name ? ' | ' + name : ''}\n`);
    }
  } catch(e) {}
}

function cleanText(text) {
  return String(text || '').replace(/[_*[\]()~`>#+=|{}.!\-]/g, ' ').trim();
}

function detectProductType(name = '') {
  const n = name.toLowerCase();
  if (n.includes('gift card') || n.includes('itunes') || n.includes('psn') ||
      n.includes('steam') || n.includes('valorant') || n.includes('razer') ||
      n.includes('roblox') || n.includes('pubg') || n.includes('voucher') ||
      n.includes('top up') || n.includes('uc')) return 'giftcard';
  if (n.includes('windows') || n.includes('idm') || n.includes('adobe') ||
      n.includes('office') || n.includes('icloud') || n.includes('google one') ||
      n.includes('lifetime') || n.includes('key')) return 'software';
  if (n.includes('chatgpt') || n.includes('claude') || n.includes('gemini') ||
      n.includes('grok') || n.includes('perplexity') || n.includes('ideogram') ||
      n.includes('quillbot') || n.includes('duolingo')) return 'ai';
  return 'subscription';
}

function productTypeEmoji(type) {
  if (type === 'giftcard') return '🎁';
  if (type === 'software') return '🔑';
  if (type === 'ai')       return '🤖';
  return '📺';
}

function isOneTime(type) { return type === 'giftcard' || type === 'software'; }

function parseDuration(text = '') {
  const t = text.toLowerCase();
  if (t.includes('1 year') || t.includes('12 month')) return 365;
  if (t.includes('6 month')) return 180;
  if (t.includes('3 month')) return 90;
  if (t.includes('2 month')) return 60;
  if (t.includes('1 month')) return 30;
  if (t.includes('7 day') || t.includes('1 week')) return 7;
  return 30;
}

function addDays(n) {
  const d = new Date();
  d.setDate(d.getDate() + n);
  return d.toLocaleDateString('en-CA');
}

function addDaysStr(dateStr, n) {
  const d = new Date(dateStr);
  d.setDate(d.getDate() + n);
  return d.toLocaleDateString('en-CA');
}

function formatDate(s) {
  if (!s) return 'N/A';
  return new Date(s).toLocaleDateString('en-BD', { day: '2-digit', month: 'short', year: 'numeric' });
}

function formatEPSTime(s) {
  try {
    const d = new Date(s);
    return d.toLocaleDateString('en-US', { day: '2-digit', month: 'long', year: 'numeric' }) + ', ' +
           d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: true });
  } catch { return s; }
}

function today() { return new Date().toLocaleDateString('en-CA'); }

function daysUntil(s) {
  if (!s) return null;
  const t = new Date(); t.setHours(0,0,0,0);
  const e = new Date(s); e.setHours(0,0,0,0);
  return Math.ceil((e - t) / 86400000);
}

function timeAgo(dateStr) {
  const mins = Math.floor((new Date() - new Date(dateStr)) / 60000);
  if (mins < 60)   return `${mins}m ago`;
  if (mins < 1440) return `${Math.floor(mins/60)}h ago`;
  return `${Math.floor(mins/1440)}d ago`;
}

function nameSimilar(a = '', b = '') {
  a = a.toLowerCase().trim();
  b = b.toLowerCase().trim();
  if (a === b) return true;
  if (a.includes(b) || b.includes(a)) return true;
  const wordsA = a.split(' ');
  const wordsB = b.split(' ');
  return wordsA.some(w => w.length > 2 && wordsB.includes(w));
}

function getProductNames(productsJson) {
  try {
    const products = JSON.parse(productsJson || '[]');
    return products.map(p => p.name).join(', ') || 'Unknown';
  } catch(e) { return 'Unknown'; }
}

// =============================================================
//  SMS SENDER
// =============================================================

async function sendSMS(phone, message) {
  if (!canSendSMS()) {
    console.log(`SMS blocked (outside hours) to ${phone}`);
    return;
  }
  const number = '880' + normalizePhone(phone);
  await axios.post('https://bulksmsbd.net/api/smsapi', null, {
    params: { api_key: config.SMS_API_KEY, senderid: config.SMS_SENDER_ID, number, message }
  });
}

// =============================================================
//  SMART MATCHING
// =============================================================

function findMatchingOrder(phone, email, name, amount) {
  const normalPhone = normalizePhone(phone);

  // 1. Phone
  let order = db.prepare(`SELECT * FROM pending_orders WHERE phone = ? AND paid = 0 AND cancelled = 0 ORDER BY created_at DESC LIMIT 1`).get(normalPhone);
  if (order) return { order, matchMethod: 'Phone' };

  // 2. Email
  if (email) {
    order = db.prepare(`SELECT * FROM pending_orders WHERE LOWER(email) = LOWER(?) AND paid = 0 AND cancelled = 0 ORDER BY created_at DESC LIMIT 1`).get(email);
    if (order) return { order, matchMethod: 'Email' };
  }

  // 3. Name (fuzzy, last 6 hours)
  const recent = db.prepare(`SELECT * FROM pending_orders WHERE paid = 0 AND cancelled = 0 AND created_at >= datetime('now', '-6 hours') ORDER BY created_at DESC`).all();
  const nameMatch = recent.find(o => nameSimilar(o.name, name));
  if (nameMatch) return { order: nameMatch, matchMethod: 'Name' };

  // 4. Amount (last 2 hours)
  order = db.prepare(`SELECT * FROM pending_orders WHERE amount = ? AND paid = 0 AND cancelled = 0 AND created_at >= datetime('now', '-2 hours') ORDER BY created_at DESC LIMIT 1`).get(amount);
  if (order) return { order, matchMethod: 'Amount' };

  return null;
}

function findPossibleMatches(phone, email, name, amount) {
  const recent = db.prepare(`SELECT * FROM pending_orders WHERE paid = 0 AND cancelled = 0 AND created_at >= datetime('now', '-24 hours') ORDER BY created_at DESC LIMIT 10`).all();
  return recent.map(o => {
    let score = 0;
    if (nameSimilar(o.name, name)) score += 3;
    if (email && o.email && o.email.toLowerCase() === email.toLowerCase()) score += 3;
    if (Math.abs(o.amount - amount) < 1) score += 2;
    return { ...o, score };
  }).filter(o => o.score > 0).sort((a, b) => b.score - a.score).slice(0, 2);
}

// =============================================================
//  EXPRESS
// =============================================================

const app = express();
app.use(express.json());

// =============================================================
//  SHOPIFY WEBHOOK - New Order
// =============================================================

app.post('/shopify-order', async (req, res) => {
  res.sendStatus(200);
  try {
    const o      = req.body;
    const phone  = normalizePhone(o.phone || o.billing_address?.phone || '');
    const name   = o.billing_address?.name || o.customer?.first_name || 'Customer';
    const email  = o.email || '';
    const amount = parseFloat(o.total_price || 0);
    const products = (o.line_items || []).map(li => ({ name: li.name || 'Unknown', variant: li.variant_title || '' }));

    if (!phone && !email) return;

    db.prepare(`INSERT OR IGNORE INTO pending_orders (shopify_order_id, order_name, name, phone, email, products, amount) VALUES (?, ?, ?, ?, ?, ?, ?)`)
      .run(String(o.id), o.name, name, phone || '', email, JSON.stringify(products), amount);

    saveContact(phone, name);

    // 1 hour follow-up SMS
    setTimeout(async () => {
      const pending = db.prepare('SELECT * FROM pending_orders WHERE shopify_order_id = ?').get(String(o.id));
      if (!pending || pending.paid === 1 || pending.cancelled === 1) return;
      try {
        await sendSMS(phone, SMS_FOLLOWUP);
        db.prepare('UPDATE pending_orders SET followup_sent = followup_sent + 1 WHERE shopify_order_id = ?').run(String(o.id));
        await safeSend(
          `⏰ *Follow-up SMS Sent!*\n` +
          `👤 ${cleanText(name)} | 📱 0${phone}\n` +
          `🛒 ${o.name}\n` +
          `💰 ৳${amount}`
        );
      } catch(e) {
        console.error('1hr followup:', e.message);
        await safeSend(`❌ *Follow-up SMS Failed!*\n👤 ${cleanText(name)} | 📱 0${phone}\nError: ${e.message}`);
      }
    }, config.FOLLOW_UP_DELAY_MS);

  } catch(e) { console.error('Shopify order webhook:', e.message); }
});

// =============================================================
//  SHOPIFY WEBHOOK - Order Cancelled
// =============================================================

app.post('/shopify-cancel', async (req, res) => {
  res.sendStatus(200);
  try {
    const o       = req.body;
    const oid     = String(o.id);
    const pending = db.prepare('SELECT * FROM pending_orders WHERE shopify_order_id = ?').get(oid);
    if (!pending) return;

    saveContact(pending.phone, pending.name);
    db.prepare('UPDATE pending_orders SET cancelled = 1, cancelled_at = datetime("now") WHERE shopify_order_id = ?').run(oid);

    const cancelCount = db.prepare(`SELECT COUNT(*) AS cnt FROM pending_orders WHERE phone = ? AND cancelled = 1`).get(pending.phone);
    let msg =
      `🚫 *Order Cancelled*\n` +
      `👤 ${cleanText(pending.name)} | 📱 0${pending.phone}\n` +
      `🛒 ${pending.order_name}\n` +
      `📦 ${getProductNames(pending.products)}\n` +
      `💰 ৳${pending.amount}\n` +
      `📱 Phone saved ✅`;

    if (cancelCount.cnt >= 2) {
      const lost = db.prepare(`SELECT COALESCE(SUM(amount),0) AS t FROM pending_orders WHERE phone = ? AND cancelled = 1`).get(pending.phone);
      msg += `\n⚠️ Repeat Canceller (${cancelCount.cnt}x) | Lost: ৳${lost.t.toFixed(0)}`;
    }
    await safeSend(msg);
  } catch(e) { console.error('Shopify cancel webhook:', e.message); }
});

// =============================================================
//  EPS IPN
// =============================================================

app.post('/eps-ipn', async (req, res) => {
  res.json({ status: 'OK' });
  try {
    const { Data } = req.body;
    if (!Data) return;
    const p = decryptEPS(Data);

    saveContact(p.customerPhone || '', p.customerName || '');

    // Failed payment
    if (p.status !== 'Success') {
      await safeSend(
        `❌ *Failed Payment — FanFlix*\n` +
        `━━━━━━━━━━━━━━━━━━\n` +
        `👤 Name: ${cleanText(p.customerName)}\n` +
        `📱 Phone: ${p.customerPhone || 'N/A'}\n` +
        `📧 Email: ${p.customerEmail || 'N/A'}\n` +
        `━━━━━━━━━━━━━━━━━━\n` +
        `💰 Amount: ৳${p.totalAmount}\n` +
        `💳 Method: ${p.financialEntity || 'N/A'}\n` +
        `📋 Status: ${p.status}\n` +
        `🔖 Reference: ${p.merchantTransactionId || 'N/A'}\n` +
        `🕐 Time: ${formatEPSTime(p.transactionDate)}\n` +
        `━━━━━━━━━━━━━━━━━━`
      );
      db.prepare('INSERT OR IGNORE INTO payments (eps_txn_id, phone, amount, store_amount, status) VALUES (?, ?, ?, 0, ?)').run(p.epsTransactionId || '', normalizePhone(p.customerPhone || ''), parseFloat(p.totalAmount || 0), p.status);
      return;
    }

    const phone      = p.customerPhone         || '';
    const name       = p.customerName          || 'Customer';
    const email      = p.customerEmail         || '';
    const totalAmt   = parseFloat(p.totalAmount  || 0);
    const storeAmt   = parseFloat(p.storeAmount  || 0);
    const gatewayFee = (totalAmt - storeAmt).toFixed(2);
    const epsTxnId   = p.epsTransactionId      || '';
    const reference  = p.merchantTransactionId || 'N/A';
    const method     = p.financialEntity       || 'N/A';
    const time       = formatEPSTime(p.transactionDate);

    const seen = db.prepare('SELECT id FROM payments WHERE eps_txn_id = ?').get(epsTxnId);
    if (seen) return;

    db.prepare('INSERT OR IGNORE INTO payments (eps_txn_id, phone, amount, store_amount, status) VALUES (?, ?, ?, ?, ?)').run(epsTxnId, normalizePhone(phone), totalAmt, storeAmt, p.status);

    // Smart matching
    const matchResult = findMatchingOrder(phone, email, name, totalAmt);

    if (!matchResult) {
      const suggestions = findPossibleMatches(phone, email, name, totalAmt);
      let msg =
        `💰 *New Payment — FanFlix*\n` +
        `━━━━━━━━━━━━━━━━━━\n` +
        `👤 Name: ${cleanText(name)}\n` +
        `📱 Phone: ${phone}\n` +
        `📧 Email: ${email || 'N/A'}\n` +
        `━━━━━━━━━━━━━━━━━━\n` +
        `💰 Customer Paid: ৳${totalAmt}\n` +
        `🏪 You Receive: ৳${storeAmt}\n` +
        `📊 Gateway Fee: ৳${gatewayFee}\n` +
        `💳 Method: ${method}\n` +
        `🔖 Reference: ${reference}\n` +
        `🕐 Time: ${time}\n` +
        `━━━━━━━━━━━━━━━━━━\n` +
        `⚠️ No Shopify Order Found!`;
      if (suggestions.length) {
        msg += `\n\n💡 Possible matches:\n`;
        suggestions.forEach(s => { msg += `${s.order_name} — ${cleanText(s.name)} | ৳${s.amount}\n`; });
      }
      await safeSend(msg);
      return;
    }

    const { order: pendingOrder, matchMethod } = matchResult;
    db.prepare('UPDATE pending_orders SET paid = 1 WHERE id = ?').run(pendingOrder.id);

    if (pendingOrder.followup_sent >= 1) {
      await safeSend(
        `✅ *Paid After Follow-up!*\n` +
        `👤 ${cleanText(name)} | 📱 ${phone}\n` +
        `🛒 ${pendingOrder.order_name}\n` +
        `💰 ৳${totalAmt}\n` +
        `✅ Removed from unpaid list`
      );
    }

    let products = [];
    try { products = JSON.parse(pendingOrder.products || '[]'); } catch(e) {}
    if (!products.length) products = [{ name: 'Unknown Product', variant: '' }];

    const existing     = db.prepare('SELECT * FROM customers WHERE phone = ? ORDER BY created_at DESC LIMIT 1').get(normalizePhone(phone));
    const renewalCount = existing ? existing.renewal_count + 1 : 1;
    const isVip        = renewalCount >= config.VIP_RENEWAL_COUNT ? 1 : 0;

    // Early renewal detection
    const earlyRenewal = products.map(li => {
      const ex = db.prepare(`SELECT * FROM customers WHERE phone = ? AND product = ? AND expiry_date >= ? ORDER BY expiry_date ASC LIMIT 1`).get(normalizePhone(phone), li.name, today());
      return ex ? daysUntil(ex.expiry_date) : null;
    }).filter(d => d !== null && d <= 5);

    const productLines = [];
    for (const li of products) {
      const productType  = detectProductType(li.name);
      const oneTime      = isOneTime(productType);
      const durationDays = oneTime ? null : parseDuration(li.variant || li.name);
      const expiryDate   = oneTime ? null : addDays(durationDays);

      db.prepare(`INSERT INTO customers (name, phone, email, product, product_type, variant, order_id, order_name, amount, store_amount, duration_days, start_date, expiry_date, renewal_count, is_vip) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
        .run(name, normalizePhone(phone), email, li.name, productType, li.variant || '', pendingOrder.shopify_order_id, pendingOrder.order_name, totalAmt, storeAmt, durationDays, today(), expiryDate, renewalCount, isVip);

      productLines.push(oneTime
        ? `${productTypeEmoji(productType)} ${cleanText(li.name)} | One-time`
        : `${productTypeEmoji(productType)} ${cleanText(li.name)} | ${formatDate(expiryDate)} | ${daysUntil(expiryDate)}d`
      );
    }

    let alert =
      `✅ *New Payment — FanFlix*\n` +
      `━━━━━━━━━━━━━━━━━━\n` +
      `👤 Name: ${cleanText(name)}\n` +
      `📱 Phone: ${phone}\n` +
      `📧 Email: ${email || 'N/A'}\n` +
      `━━━━━━━━━━━━━━━━━━\n` +
      `💰 Customer Paid: ৳${totalAmt}\n` +
      `🏪 You Receive: ৳${storeAmt}\n` +
      `📊 Gateway Fee: ৳${gatewayFee}\n` +
      `💳 Method: ${method}\n` +
      `🔖 Reference: ${reference}\n` +
      `🕐 Time: ${time}\n` +
      `🔗 Matched by: ${matchMethod}\n` +
      `━━━━━━━━━━━━━━━━━━\n` +
      `🛒 Order: ${pendingOrder.order_name}\n` +
      productLines.join('\n') + '\n' +
      (renewalCount > 1 ? `🔄 Renewal #${renewalCount}\n` : '') +
      (isVip ? `⭐ VIP Customer\n` : '') +
      (earlyRenewal.length ? `⚠️ Early Renewal! ${earlyRenewal[0]}d still left\n` : '') +
      `━━━━━━━━━━━━━━━━━━`;

    await safeSend(alert);

  } catch(err) {
    console.error('IPN Error:', err.message);
    safeSend(`❌ Bot Error: ${err.message}`).catch(() => {});
  }
});

app.get('/', (req, res) => res.send('FanFlix Bot v6.1'));

// =============================================================
//  PAGINATION
// =============================================================

const PAGE_SIZE = 5;

function showCustomerPage(chatId, page = 0) {
  const todayStr = today();
  const allRows  = db.prepare(`SELECT * FROM customers WHERE expiry_date >= ? ORDER BY product, expiry_date ASC`).all(todayStr);
  if (!allRows.length) return sendAutoDelete(chatId, 'No active customers.');
  const total      = allRows.length;
  const totalPages = Math.ceil(total / PAGE_SIZE);
  const rows       = allRows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  const grouped = {};
  rows.forEach(c => { if (!grouped[c.product]) grouped[c.product] = []; grouped[c.product].push(c); });
  const productCounts = {};
  allRows.forEach(c => { productCounts[c.product] = (productCounts[c.product] || 0) + 1; });

  let text = `Active Customers (${total}) — Page ${page + 1}/${totalPages}\n━━━━━━━━━━━━━━━━━━\n`;
  Object.entries(grouped).forEach(([product, customers]) => {
    text += `\n📦 ${product} (${productCounts[product]})\n`;
    customers.forEach(c => {
      const d = daysUntil(c.expiry_date);
      text += `${c.is_vip ? '⭐' : ''}${cleanText(c.name)} | 0${c.phone} | ${formatDate(c.expiry_date)} | ${d}d\n`;
    });
  });

  const buttons = [];
  if (page > 0) buttons.push({ text: '◀️ Prev', callback_data: `cust_${page - 1}` });
  if (page < totalPages - 1) buttons.push({ text: 'Next ▶️', callback_data: `cust_${page + 1}` });
  return sendAutoDelete(chatId, text, buttons.length ? { reply_markup: { inline_keyboard: [buttons] } } : {});
}

function showTodayPage(chatId, page = 0) {
  const todayStr = today();
  const allRows  = db.prepare(`SELECT * FROM customers WHERE start_date = ? ORDER BY product, created_at DESC`).all(todayStr);
  if (!allRows.length) return sendAutoDelete(chatId, 'No orders today.');
  const totalRev   = allRows.reduce((s, c) => s + (c.store_amount || 0), 0);
  const totalPages = Math.ceil(allRows.length / PAGE_SIZE);
  const rows       = allRows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
  const grouped    = {};
  rows.forEach(c => { if (!grouped[c.product]) grouped[c.product] = []; grouped[c.product].push(c); });

  let text = `Today: ${allRows.length} orders | ৳${totalRev.toFixed(0)} — Page ${page + 1}/${totalPages}\n━━━━━━━━━━━━━━━━━━\n`;
  Object.entries(grouped).forEach(([product, customers]) => {
    text += `\n📦 ${product}\n`;
    customers.forEach(c => { text += `${cleanText(c.name)} | ৳${c.store_amount || 0}\n`; });
  });

  const buttons = [];
  if (page > 0) buttons.push({ text: '◀️ Prev', callback_data: `today_${page - 1}` });
  if (page < totalPages - 1) buttons.push({ text: 'Next ▶️', callback_data: `today_${page + 1}` });
  return sendAutoDelete(chatId, text, buttons.length ? { reply_markup: { inline_keyboard: [buttons] } } : {});
}

function showExpiringPage(chatId, page = 0) {
  const todayStr = today();
  const in7days  = addDaysStr(todayStr, 7);
  const allRows  = db.prepare(`SELECT * FROM customers WHERE expiry_date >= ? AND expiry_date <= ? ORDER BY expiry_date ASC`).all(todayStr, in7days);
  if (!allRows.length) return sendAutoDelete(chatId, 'No one expiring this week!');
  const totalPages = Math.ceil(allRows.length / PAGE_SIZE);
  const rows       = allRows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  let text = `Expiring This Week (${allRows.length}) — Page ${page + 1}/${totalPages}\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach(c => { text += `${cleanText(c.name)} | 0${c.phone}\n${c.product}\n${formatDate(c.expiry_date)} | ${daysUntil(c.expiry_date)}d left\n\n`; });

  const buttons = [];
  if (page > 0) buttons.push({ text: '◀️ Prev', callback_data: `exp_${page - 1}` });
  if (page < totalPages - 1) buttons.push({ text: 'Next ▶️', callback_data: `exp_${page + 1}` });
  return sendAutoDelete(chatId, text, buttons.length ? { reply_markup: { inline_keyboard: [buttons] } } : {});
}

// =============================================================
//  CALLBACK HANDLER
// =============================================================

bot.on('callback_query', async (query) => {
  const data   = query.data;
  const chatId = query.message.chat.id;
  if (String(chatId) !== String(config.TELEGRAM_CHAT_ID)) return;
  await bot.deleteMessage(chatId, query.message.message_id).catch(() => {});
  await bot.answerCallbackQuery(query.id);
  if (data.startsWith('cust_'))  showCustomerPage(chatId, parseInt(data.split('_')[1]));
  if (data.startsWith('today_')) showTodayPage(chatId, parseInt(data.split('_')[1]));
  if (data.startsWith('exp_'))   showExpiringPage(chatId, parseInt(data.split('_')[1]));
});

// =============================================================
//  BOT COMMANDS
// =============================================================

bot.onText(/\/start/, (msg) => {
  if (!isOwner(msg)) return;
  sendAutoDelete(msg.chat.id,
    `FanFlix Bot v6.1\n\n` +
    `Commands:\n` +
    `/customers - Active customers\n` +
    `/expiring - Expiring this week\n` +
    `/today - Today orders\n` +
    `/revenue - Revenue + lost\n` +
    `/stats - Business overview\n` +
    `/product - Sales by product\n` +
    `/retention - Retention rate\n` +
    `/top - Top customers by spend\n` +
    `/unpaid - Today unpaid orders\n` +
    `/cancelled - Cancelled history\n` +
    `/search - Find customer\n` +
    `/history 01874... - Full history\n` +
    `/cancel fanflix35684 - Cancel order\n` +
    `/edit - Edit expiry date\n` +
    `/export - Download CSV\n` +
    `/exportcontacts - Download contacts`
  );
});

bot.onText(/\/customers/, (msg) => { if (!isOwner(msg)) return; showCustomerPage(msg.chat.id, 0); });
bot.onText(/\/expiring/,  (msg) => { if (!isOwner(msg)) return; showExpiringPage(msg.chat.id, 0); });
bot.onText(/\/today/,     (msg) => { if (!isOwner(msg)) return; showTodayPage(msg.chat.id, 0); });

bot.onText(/\/revenue/, (msg) => {
  if (!isOwner(msg)) return;
  const todayStr  = today();
  const t = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS total, COUNT(*) AS cnt FROM customers WHERE start_date = ?`).get(todayStr);
  const w = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS total, COUNT(*) AS cnt FROM customers WHERE start_date >= date(?, '-7 days')`).get(todayStr);
  const m = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS total, COUNT(*) AS cnt FROM customers WHERE start_date >= date(?, '-30 days')`).get(todayStr);
  const lostToday = db.prepare(`SELECT COUNT(*) AS cnt, COALESCE(SUM(amount),0) AS total FROM pending_orders WHERE cancelled = 1 AND date(COALESCE(cancelled_at, created_at), '+6 hours') = ?`).get(todayStr);
  const lostMonth = db.prepare(`SELECT COUNT(*) AS cnt, COALESCE(SUM(amount),0) AS total FROM pending_orders WHERE cancelled = 1 AND COALESCE(cancelled_at, created_at) >= datetime('now', '-30 days')`).get();
  sendAutoDelete(msg.chat.id,
    `Revenue Report\n━━━━━━━━━━━━━━━━━━\n` +
    `Today: ৳${t.total.toFixed(0)} (${t.cnt})\n` +
    `Week:  ৳${w.total.toFixed(0)} (${w.cnt})\n` +
    `Month: ৳${m.total.toFixed(0)} (${m.cnt})\n` +
    `━━━━━━━━━━━━━━━━━━\n` +
    `❌ Lost Today: ৳${lostToday.total.toFixed(0)} (${lostToday.cnt})\n` +
    `❌ Lost Month: ৳${lostMonth.total.toFixed(0)} (${lostMonth.cnt})`
  );
});

bot.onText(/\/stats/, (msg) => {
  if (!isOwner(msg)) return;
  const todayStr = today();
  const active  = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date >= ?`).get(todayStr);
  const expired = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date < ?`).get(todayStr);
  const onetime = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date IS NULL`).get();
  const total   = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS total FROM customers`).get();
  const vip     = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE is_vip = 1 AND expiry_date >= ?`).get(todayStr);
  const best    = db.prepare(`SELECT product, COUNT(*) AS cnt FROM customers GROUP BY product ORDER BY cnt DESC LIMIT 1`).get();
  sendAutoDelete(msg.chat.id,
    `Business Overview\n━━━━━━━━━━━━━━━━━━\n` +
    `Total: ${active.cnt + expired.cnt + onetime.cnt}\n` +
    `Active: ${active.cnt} | One-time: ${onetime.cnt}\n` +
    `Expired: ${expired.cnt} | VIP: ${vip.cnt}\n` +
    `Revenue: ৳${total.total.toFixed(0)}\n` +
    `Best: ${best?.product || 'N/A'}`
  );
});

bot.onText(/\/product/, (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT product, COUNT(*) AS cnt, SUM(store_amount) AS rev FROM customers GROUP BY product ORDER BY cnt DESC`).all();
  if (!rows.length) return sendAutoDelete(msg.chat.id, 'No data.');
  let text = `Sales by Product\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach(r => { text += `${r.product}\n${r.cnt} orders | ৳${(r.rev||0).toFixed(0)}\n\n`; });
  sendAutoDelete(msg.chat.id, text);
});

bot.onText(/\/retention/, (msg) => {
  if (!isOwner(msg)) return;
  const total   = db.prepare(`SELECT COUNT(DISTINCT phone) AS cnt FROM customers`).get();
  const renewed = db.prepare(`SELECT COUNT(DISTINCT phone) AS cnt FROM customers WHERE renewal_count > 1`).get();
  const rate    = total.cnt > 0 ? Math.round((renewed.cnt / total.cnt) * 100) : 0;
  const top     = db.prepare(`SELECT name, phone, MAX(renewal_count) AS r FROM customers GROUP BY phone ORDER BY r DESC LIMIT 5`).all();
  let text = `Retention\n━━━━━━━━━━━━━━━━━━\nTotal: ${total.cnt} | Renewed: ${renewed.cnt} | Rate: ${rate}%\n\nTop Loyal:\n`;
  top.forEach((c, i) => { text += `${i+1}. ${cleanText(c.name)} — ${c.r}x\n`; });
  sendAutoDelete(msg.chat.id, text);
});

// /top - sorted by highest spend
bot.onText(/\/top/, (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT name, phone, MAX(renewal_count) AS r, SUM(store_amount) AS spent FROM customers GROUP BY phone ORDER BY spent DESC LIMIT 10`).all();
  if (!rows.length) return sendAutoDelete(msg.chat.id, 'No data.');
  let text = `Top Customers by Spend\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach((c, i) => { text += `${i+1}. ${cleanText(c.name)} | 0${c.phone}\n💰 ৳${(c.spent||0).toFixed(0)} | 🔄 ${c.r}x\n\n`; });
  sendAutoDelete(msg.chat.id, text);
});

// /unpaid - today's unpaid orders
bot.onText(/\/unpaid/, (msg) => {
  if (!isOwner(msg)) return;
  const todayStr = today();
  const rows = db.prepare(`SELECT * FROM pending_orders WHERE paid = 0 AND cancelled = 0 AND datetime(created_at, '+6 hours') >= ? AND datetime(created_at, '+6 hours') < ? ORDER BY created_at ASC`).all(todayStr + ' 00:00:00', todayStr + ' 23:59:59');
  if (!rows.length) return sendAutoDelete(msg.chat.id, 'No unpaid orders today.');
  let text = `Unpaid Orders Today (${rows.length})\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach((o, i) => {
    text += `${i+1}. ${o.order_name} — ${cleanText(o.name)}\n`;
    text += `📦 ${getProductNames(o.products)}\n`;
    text += `💰 ৳${o.amount} | ⏰ ${timeAgo(o.created_at)}\n\n`;
  });
  sendAutoDelete(msg.chat.id, text);
});

bot.onText(/\/cancelled/, (msg) => {
  if (!isOwner(msg)) return;
  const allTime  = db.prepare(`SELECT COUNT(*) AS cnt, COALESCE(SUM(amount),0) AS total FROM pending_orders WHERE cancelled = 1`).get();
  const last30   = db.prepare(`SELECT COUNT(*) AS cnt, COALESCE(SUM(amount),0) AS total FROM pending_orders WHERE cancelled = 1 AND COALESCE(cancelled_at, created_at) >= datetime('now', '-30 days')`).get();
  const last7    = db.prepare(`SELECT * FROM pending_orders WHERE cancelled = 1 ORDER BY COALESCE(cancelled_at, created_at) DESC LIMIT 20`).all();
  const peakHour = db.prepare(`SELECT strftime('%H', cancelled_at) AS hr, COUNT(*) AS cnt FROM pending_orders WHERE cancelled = 1 AND cancelled_at IS NOT NULL GROUP BY hr ORDER BY cnt DESC LIMIT 1`).get();
  const peakTime = peakHour ? `${parseInt(peakHour.hr)}:00 - ${parseInt(peakHour.hr)+1}:00` : 'N/A';
  const cancelledPhones = db.prepare(`SELECT DISTINCT phone FROM pending_orders WHERE cancelled = 1`).all().map(r => r.phone);
  let reorderCount = 0;
  cancelledPhones.forEach(phone => { if (db.prepare(`SELECT id FROM customers WHERE phone = ?`).get(phone)) reorderCount++; });
  const reorderRate = cancelledPhones.length > 0 ? Math.round((reorderCount / cancelledPhones.length) * 100) : 0;

  let text = `Cancelled Orders\n━━━━━━━━━━━━━━━━━━\n`;
  text += `All Time: ${allTime.cnt} | ৳${allTime.total.toFixed(0)}\n`;
  text += `Last 30 days: ${last30.cnt} | ৳${last30.total.toFixed(0)}\n`;
  text += `Peak Cancel Time: ${peakTime}\n`;
  text += `Re-order Rate: ${reorderRate}%\n━━━━━━━━━━━━━━━━━━\n`;

  if (last7.length) {
    text += `Recent ${last7.length}:\n\n`;
    last7.forEach((o, i) => {
      const cancelTime = formatEPSTime(o.cancelled_at || o.created_at);
      text += `${i+1}. ${o.order_name} — ${cleanText(o.name)}\n`;
      text += `📦 ${getProductNames(o.products)} | ৳${o.amount}\n`;
      text += `⏰ ${cancelTime}\n\n`;
    });
  }
  sendAutoDelete(msg.chat.id, text);
});

// /search
bot.onText(/\/search (.+)/, (msg, match) => {
  if (!isOwner(msg)) return;
  const query = match[1].trim();
  const phone = normalizePhone(query);
  const rows  = db.prepare(`SELECT * FROM customers WHERE phone LIKE ? OR LOWER(name) LIKE LOWER(?) OR LOWER(email) LIKE LOWER(?) ORDER BY created_at DESC LIMIT 10`).all(`%${phone}%`, `%${query}%`, `%${query}%`);
  if (!rows.length) return sendAutoDelete(msg.chat.id, 'No customer found.');

  // Get lifetime value per phone
  let text = `Search: "${query}"\n━━━━━━━━━━━━━━━━━━\n`;
  const phones = [...new Set(rows.map(r => r.phone))];
  phones.forEach(ph => {
    const customerRows = rows.filter(r => r.phone === ph);
    const ltv = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS total FROM customers WHERE phone = ?`).get(ph);
    const c   = customerRows[0];
    const d   = c.expiry_date ? daysUntil(c.expiry_date) : null;
    const status = c.expiry_date ? (d > 0 ? `✅ Active (${d}d)` : '❌ Expired') : '🎁 One-time';
    text += `${c.is_vip ? '⭐' : '👤'} ${cleanText(c.name)} | 0${c.phone}\n`;
    text += `📦 ${c.product}\n`;
    text += `${status} | 🔄 ${c.renewal_count}x | 💰 ৳${ltv.total.toFixed(0)} lifetime\n\n`;
  });
  sendAutoDelete(msg.chat.id, text);
});

// /history
bot.onText(/\/history (.+)/, (msg, match) => {
  if (!isOwner(msg)) return;
  const phone     = normalizePhone(match[1].trim());
  const orders    = db.prepare(`SELECT * FROM customers WHERE phone = ? ORDER BY created_at DESC`).all(phone);
  const cancelled = db.prepare(`SELECT * FROM pending_orders WHERE phone = ? AND cancelled = 1 ORDER BY COALESCE(cancelled_at, created_at) DESC`).all(phone);
  const unpaid    = db.prepare(`SELECT * FROM pending_orders WHERE phone = ? AND paid = 0 AND cancelled = 0`).all(phone);

  if (!orders.length && !cancelled.length) return sendAutoDelete(msg.chat.id, 'No history found.');

  const totalSpent = orders.reduce((s, o) => s + (o.store_amount || 0), 0);
  const firstOrder = orders.length ? orders[orders.length - 1] : null;

  let text = `Customer History | 0${phone}\n━━━━━━━━━━━━━━━━━━\n`;
  if (orders.length) {
    text += `💰 Lifetime Value: ৳${totalSpent.toFixed(0)}\n`;
    text += `🔄 Total Orders: ${orders.length}\n`;
    text += `📅 Since: ${firstOrder ? formatDate(firstOrder.start_date) : 'N/A'}\n`;
    if (orders[0]?.is_vip) text += `⭐ VIP Customer\n`;
    text += `\n✅ Orders:\n`;
    orders.slice(0, 5).forEach(o => {
      text += `${o.order_name} | ${cleanText(o.product)}\n৳${o.store_amount || 0} | ${formatDate(o.start_date)}\n`;
      if (o.expiry_date) text += `Expires: ${formatDate(o.expiry_date)}\n`;
      text += `\n`;
    });
  }
  if (cancelled.length) {
    text += `❌ Cancelled (${cancelled.length}):\n`;
    cancelled.slice(0, 3).forEach(o => { text += `${o.order_name} | ৳${o.amount}\n`; });
    text += `\n`;
  }
  if (unpaid.length) {
    text += `⏳ Unpaid (${unpaid.length}):\n`;
    unpaid.forEach(o => { text += `${o.order_name} | ৳${o.amount}\n`; });
  }
  sendAutoDelete(msg.chat.id, text);
});

// /cancel
bot.onText(/\/cancel (.+)/, async (msg, match) => {
  if (!isOwner(msg)) return;
  const orderName = match[1].trim().toUpperCase().replace('#', '');
  const pending   = db.prepare(`SELECT * FROM pending_orders WHERE UPPER(REPLACE(order_name, '#', '')) = ? AND cancelled = 0`).get(orderName);
  if (!pending) return sendAutoDelete(msg.chat.id, `Order not found or already cancelled.`);

  db.prepare('UPDATE pending_orders SET cancelled = 1, cancelled_at = datetime("now") WHERE id = ?').run(pending.id);
  saveContact(pending.phone, pending.name);

  const cancelCount = db.prepare(`SELECT COUNT(*) AS cnt FROM pending_orders WHERE phone = ? AND cancelled = 1`).get(pending.phone);
  let text =
    `🚫 *Order Cancelled*\n` +
    `👤 ${cleanText(pending.name)} | 📱 0${pending.phone}\n` +
    `🛒 ${pending.order_name}\n` +
    `📦 ${getProductNames(pending.products)}\n` +
    `💰 ৳${pending.amount}\n` +
    `📱 Phone saved ✅`;

  if (cancelCount.cnt >= 2) {
    const lost = db.prepare(`SELECT COALESCE(SUM(amount),0) AS t FROM pending_orders WHERE phone = ? AND cancelled = 1`).get(pending.phone);
    text += `\n⚠️ Repeat Canceller (${cancelCount.cnt}x) | Lost: ৳${lost.t.toFixed(0)}`;
  }
  sendAutoDelete(msg.chat.id, text);
});

// /edit
const editState = {};
bot.onText(/\/edit/, (msg) => {
  if (!isOwner(msg)) return;
  editState[msg.chat.id] = { step: 'phone' };
  sendAutoDelete(msg.chat.id, 'Enter phone number to edit:');
});

bot.onText(/\/export/, async (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT * FROM customers ORDER BY created_at DESC`).all();
  if (!rows.length) return sendAutoDelete(msg.chat.id, 'No data.');
  let csv = 'Name,Phone,Email,Product,Type,Amount,Start,Expiry,Renewals,VIP\n';
  rows.forEach(c => { csv += `"${c.name}","0${c.phone}","${c.email}","${c.product}","${c.product_type}",${c.store_amount||0},${c.start_date},${c.expiry_date||'N/A'},${c.renewal_count},${c.is_vip?'Yes':'No'}\n`; });
  const sent = await bot.sendDocument(msg.chat.id, Buffer.from(csv,'utf8'), {}, { filename: `fanflix_${today()}.csv`, contentType: 'text/csv' });
  setTimeout(() => bot.deleteMessage(msg.chat.id, sent.message_id).catch(() => {}), 60000);
});

bot.onText(/\/exportcontacts/, async (msg) => {
  if (!isOwner(msg)) return;
  if (!fs.existsSync(CONTACTS_FILE)) return sendAutoDelete(msg.chat.id, 'No contacts yet.');
  const sent = await bot.sendDocument(msg.chat.id, CONTACTS_FILE, {}, { filename: `contacts_${today()}.txt`, contentType: 'text/plain' });
  setTimeout(() => bot.deleteMessage(msg.chat.id, sent.message_id).catch(() => {}), 60000);
});

bot.on('message', (msg) => {
  if (!isOwner(msg)) return;
  const cid  = msg.chat.id;
  const text = msg.text || '';
  if (text.startsWith('/')) return;

  if (editState[cid]) {
    const s = editState[cid];
    if (s.step === 'phone') {
      const c = db.prepare(`SELECT * FROM customers WHERE phone = ? ORDER BY created_at DESC LIMIT 1`).get(normalizePhone(text));
      if (!c) { delete editState[cid]; return sendAutoDelete(cid, 'Customer not found.'); }
      s.customer = c; s.step = 'date';
      return sendAutoDelete(cid, `${cleanText(c.name)} | ${c.product}\nExpiry: ${c.expiry_date || 'One-time'}\n\nNew date (YYYY-MM-DD):`);
    }
    if (s.step === 'date') {
      db.prepare(`UPDATE customers SET expiry_date=?, reminder_1_sent=0 WHERE id=?`).run(text, s.customer.id);
      delete editState[cid];
      return sendAutoDelete(cid, `Updated to ${formatDate(text)}`);
    }
  }
});

// =============================================================
//  SCHEDULED TASKS
// =============================================================

// 8 AM - SMS balance check
cron.schedule('0 8 * * *', async () => {
  try {
    const res     = await axios.get('https://bulksmsbd.net/api/getBalanceApi', { params: { api_key: config.SMS_API_KEY } });
    const balance = parseFloat(res.data?.balance || res.data?.data?.balance || 0);
    if (balance < 100) {
      await safeSend(`⚠️ *Low SMS Balance!*\nRemaining: ${balance}\nTop up now to avoid missed reminders!`);
    }
  } catch(e) { console.error('SMS balance:', e.message); }
});

// 7 PM - renewal + lost alerts + expiry preview + 12h unpaid list
cron.schedule('0 19 * * *', async () => {
  const todayStr = today();
  const in1day   = addDaysStr(todayStr, 1);
  const lost3ago = addDaysStr(todayStr, -config.LOST_ALERT_DAYS_AFTER_EXPIRY);

  // Renewal SMS 1 day before
  const in1 = db.prepare(`SELECT * FROM customers WHERE expiry_date = ? AND reminder_1_sent = 0`).all(in1day);
  for (const c of in1) {
    try {
      await sendSMS(c.phone, SMS_MSG1(c.product));
      db.prepare('UPDATE customers SET reminder_1_sent=1 WHERE id=?').run(c.id);
      await safeSend(
        `🚨 *Renewal SMS Sent (1 day)*\n` +
        `👤 ${cleanText(c.name)} | 📱 0${c.phone}\n` +
        `📦 ${c.product}\n` +
        `📅 Expires: TOMORROW`
      );
    } catch(e) { console.error('SMS 1d:', e.message); }
  }

  // Lost customer alerts
  const lost = db.prepare(`SELECT * FROM customers WHERE expiry_date = ? AND lost_alert_sent = 0`).all(lost3ago);
  for (const c of lost) {
    try {
      await safeSend(`⚠️ *Lost Customer!*\n👤 ${cleanText(c.name)} | 0${c.phone}\n📦 ${c.product}\n💀 Expired ${config.LOST_ALERT_DAYS_AFTER_EXPIRY} days ago`);
      db.prepare('UPDATE customers SET lost_alert_sent=1 WHERE id=?').run(c.id);
    } catch(e) { console.error('Lost:', e.message); }
  }

  // Tomorrow expiry preview
  const tomorrow = db.prepare(`SELECT * FROM customers WHERE expiry_date = ?`).all(in1day);
  if (tomorrow.length) {
    let text = `📅 *Expiring Tomorrow (${tomorrow.length})*\n━━━━━━━━━━━━━━━━━━\n`;
    tomorrow.forEach(c => { text += `👤 ${cleanText(c.name)} | 0${c.phone}\n📦 ${c.product}\n\n`; });
    await safeSend(text);
  }

  // 12h unpaid list with conversion tracking
  const unpaid12h = db.prepare(`
    SELECT * FROM pending_orders
    WHERE paid = 0 AND cancelled = 0
    AND created_at <= datetime('now', '-12 hours')
    ORDER BY created_at ASC
  `).all();

  if (unpaid12h.length) {
    // Conversion: how many paid after 1st follow-up
    const paidAfterSMS = db.prepare(`SELECT COUNT(*) AS cnt FROM pending_orders WHERE paid = 1 AND followup_sent >= 1 AND date(created_at, '+6 hours') = ?`).get(todayStr);

    let text =
      `📋 *Unpaid Orders (12h+)*\n━━━━━━━━━━━━━━━━━━\n` +
      `✅ Paid after SMS today: ${paidAfterSMS.cnt}\n` +
      `❌ Still unpaid: ${unpaid12h.length}\n` +
      `━━━━━━━━━━━━━━━━━━\n`;

    unpaid12h.forEach((o, i) => {
      text += `${i+1}. ${o.order_name} — ${cleanText(o.name)}\n`;
      text += `📦 ${getProductNames(o.products)} | ৳${o.amount}\n`;
      text += `⏰ ${timeAgo(o.created_at)}\n\n`;
    });
    text += `Use /cancel to cancel each order.`;
    await safeSend(text);
  }

  // Clean pending orders older than 24h
  db.prepare(`DELETE FROM pending_orders WHERE paid = 0 AND cancelled = 0 AND created_at < datetime('now', '-24 hours')`).run();
});

// 9 PM - discount SMS (7 days after cancel, once only)
cron.schedule('0 21 * * *', async () => {
  try {
    const eligible = db.prepare(`
      SELECT DISTINCT p.phone, p.name FROM pending_orders p
      WHERE p.cancelled = 1 AND p.discount_sent = 0
      AND COALESCE(p.cancelled_at, p.created_at) <= datetime('now', '-7 days')
      AND p.phone NOT IN (SELECT DISTINCT phone FROM customers)
    `).all();

    for (const c of eligible) {
      try {
        await sendSMS(c.phone, SMS_DISCOUNT);
        db.prepare(`UPDATE pending_orders SET discount_sent = 1 WHERE phone = ? AND cancelled = 1`).run(c.phone);
        await safeSend(`🎁 *Discount SMS Sent!*\n👤 ${cleanText(c.name)} | 📱 0${c.phone}\nCode: WELCOMEBACK10`);
      } catch(e) { console.error('Discount SMS:', e.message); }
    }
  } catch(e) { console.error('Discount cron:', e.message); }
});

// 10:30 PM - daily summary
cron.schedule('30 22 * * *', async () => {
  try {
    const todayStr = today();
    const t        = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS revenue, COUNT(*) AS orders FROM customers WHERE start_date = ?`).get(todayStr);
    const expiring = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date >= ? AND expiry_date <= ?`).get(todayStr, addDaysStr(todayStr, 7));
    const active   = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date >= ?`).get(todayStr);
    await safeSend(
      `📊 *Daily Summary*\n━━━━━━━━━━━━━━━━━━\n` +
      `✅ Orders: ${t.orders}\n` +
      `💰 Revenue: ৳${t.revenue.toFixed(0)}\n` +
      `👥 Active: ${active.cnt}\n` +
      `⚠️ Expiring This Week: ${expiring.cnt}`
    );
  } catch(e) { console.error('Summary:', e.message); }
});

// 11:50 PM - payment reconciliation
cron.schedule('50 23 * * *', async () => {
  try {
    const todayStr     = today();
    const totalSuccess = db.prepare(`SELECT COUNT(*) AS cnt, COALESCE(SUM(amount),0) AS total, COALESCE(SUM(store_amount),0) AS net FROM payments WHERE date(created_at, '+6 hours') = ? AND status = 'Success'`).get(todayStr);
    const totalFailed  = db.prepare(`SELECT COUNT(*) AS cnt, COALESCE(SUM(amount),0) AS total FROM payments WHERE date(created_at, '+6 hours') = ? AND status != 'Success'`).get(todayStr);
    const matched      = db.prepare(`SELECT COUNT(*) AS cnt, COALESCE(SUM(store_amount),0) AS total FROM customers WHERE start_date = ?`).get(todayStr);
    const reportDate   = new Date().toLocaleDateString('en-US', { day: '2-digit', month: 'long', year: 'numeric' });

    await safeSend(
      `📊 *Payment Report — ${reportDate}*\n` +
      `━━━━━━━━━━━━━━━━━━\n` +
      `💰 Total Received: ৳${totalSuccess.total.toFixed(0)} (${totalSuccess.cnt})\n` +
      `✅ Matched: ${matched.cnt} | ৳${matched.total.toFixed(0)}\n` +
      `⚠️ Unmatched: ${Math.max(0, totalSuccess.cnt - matched.cnt)}\n` +
      `❌ Failed: ${totalFailed.cnt} | ৳${totalFailed.total.toFixed(0)}\n` +
      `━━━━━━━━━━━━━━━━━━\n` +
      `🏪 Net Revenue: ৳${matched.total.toFixed(0)}\n` +
      `📊 Gateway Fees: ৳${(totalSuccess.total - matched.total > 0 ? totalSuccess.total - matched.total : 0).toFixed(0)}`
    );
  } catch(e) { console.error('Payment report:', e.message); }
});

// 1st of month - growth
cron.schedule('0 10 1 * *', async () => {
  try {
    const tm = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE start_date >= date('now','start of month')`).get();
    const lm = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE start_date >= date('now','start of month','-1 month') AND start_date < date('now','start of month')`).get();
    const g  = lm.cnt > 0 ? Math.round(((tm.cnt - lm.cnt) / lm.cnt) * 100) : 0;
    await safeSend(`${g >= 0 ? '📈' : '📉'} *Monthly Growth*\nLast: ${lm.cnt} | This: ${tm.cnt} | ${g >= 0 ? '+' : ''}${g}%`);
  } catch(e) { console.error('Growth:', e.message); }
});

// =============================================================
//  START
// =============================================================

app.listen(config.PORT, () => {
  console.log(`FanFlix Bot v6.1 on port ${config.PORT}`);
  safeSend('🚀 *FanFlix Bot v6.1 Started!*').catch(() => {});
});
