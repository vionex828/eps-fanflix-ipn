process.env.TZ = 'Asia/Dhaka';

// =============================================
//   FANFLIX BOT v5.2 - ALL FIXES
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

// Migrate existing DB
try { db.exec(`ALTER TABLE pending_orders ADD COLUMN cancelled INTEGER DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE customers ADD COLUMN store_amount REAL DEFAULT 0`); } catch(e) {}

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
    reminder_3_sent   INTEGER DEFAULT 0,
    reminder_1_sent   INTEGER DEFAULT 0,
    lost_alert_sent   INTEGER DEFAULT 0,
    created_at        TEXT DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS payments (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    eps_txn_id  TEXT UNIQUE,
    phone       TEXT,
    amount      REAL,
    status      TEXT,
    created_at  TEXT DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS pending_orders (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    shopify_order_id TEXT UNIQUE,
    order_name       TEXT,
    name             TEXT,
    phone            TEXT,
    email            TEXT,
    product          TEXT,
    variant          TEXT,
    amount           REAL,
    followup_sent    INTEGER DEFAULT 0,
    paid             INTEGER DEFAULT 0,
    cancelled        INTEGER DEFAULT 0,
    created_at       TEXT DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS unmatched_payments (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    eps_txn_id  TEXT UNIQUE,
    name        TEXT,
    phone       TEXT,
    email       TEXT,
    total_amt   REAL,
    store_amt   REAL,
    method      TEXT,
    reference   TEXT,
    txn_time    TEXT,
    created_at  TEXT DEFAULT CURRENT_TIMESTAMP
  );
`);

// =============================================================
//  SMS MESSAGES
// =============================================================

const SMS_MSG3 = (product) =>
  `প্রিয় গ্রাহক,\n\nআপনার ${product} সাবস্ক্রিপশনটি আগামী ৩ দিনের মধ্যে মেয়াদ শেষ হতে চলেছে।\n\nবিরতিহীন সেবা উপভোগ করতে এখনই রিনিউ করুন।\n\nWhatsApp: wa.me/+8801928382918\n\n— FanFlix BD`;

const SMS_MSG1 = (product) =>
  `প্রিয় গ্রাহক,\n\nআপনার ${product} সাবস্ক্রিপশনটি আগামীকাল মেয়াদ শেষ হবে।\n\nসার্ভিস বন্ধ হওয়ার আগেই রিনিউ করুন।\n\nWhatsApp: wa.me/+8801928382918\n\n— FanFlix BD`;

const SMS_FOLLOWUP =
  `প্রিয় গ্রাহক,\n\nআপনার অর্ডারটি এখনো সম্পন্ন হয়নি। পেমেন্ট না হওয়ায় অর্ডারটি পেন্ডিং অবস্থায় রয়েছে।\n\nপেমেন্ট করুন:\nhttps://pg.eps.com.bd/DefaultPaymentLink?id=805A9AEE\n\nWhatsApp: wa.me/+8801928382918\n\n— FanFlix BD`;

// =============================================================
//  TELEGRAM
// =============================================================

const bot = new TelegramBot(config.TELEGRAM_BOT_TOKEN, { polling: true });

function sendTelegram(msg) {
  return bot.sendMessage(config.TELEGRAM_CHAT_ID, msg, { parse_mode: 'Markdown' });
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
  } catch(e) { console.error('saveContact:', e.message); }
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
  if (type === 'giftcard') return '🎁 Gift Card';
  if (type === 'software') return '🔑 Software';
  if (type === 'ai')       return '🤖 AI Tool';
  return '📺 Subscription';
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
  return d.toISOString().split('T')[0];
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

// BD-aware date functions - fixes SQLite UTC issue
function today() {
  return new Date().toLocaleDateString('en-CA'); // YYYY-MM-DD in local timezone
}

function addDaysStr(dateStr, n) {
  const d = new Date(dateStr);
  d.setDate(d.getDate() + n);
  return d.toLocaleDateString('en-CA');
}

function daysUntil(s) {
  if (!s) return null;
  const t = new Date(); t.setHours(0,0,0,0);
  const e = new Date(s); e.setHours(0,0,0,0);
  return Math.ceil((e - t) / 86400000);
}

function timeAgo(dateStr) {
  const now  = new Date();
  const then = new Date(dateStr);
  const mins = Math.floor((now - then) / 60000);
  if (mins < 60)  return `${mins}m ago`;
  if (mins < 1440) return `${Math.floor(mins/60)}h ago`;
  return `${Math.floor(mins/1440)}d ago`;
}

// =============================================================
//  SMS
// =============================================================

async function sendSMS(phone, message) {
  const number = '880' + normalizePhone(phone);
  await axios.post('https://bulksmsbd.net/api/smsapi', null, {
    params: { api_key: config.SMS_API_KEY, senderid: config.SMS_SENDER_ID, number, message }
  });
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
    const o     = req.body;
    const phone = normalizePhone(o.phone || o.billing_address?.phone || '');
    if (!phone) return;
    const name    = o.billing_address?.name || o.customer?.first_name || 'Customer';
    const email   = o.email || '';
    const amount  = parseFloat(o.total_price || 0);
    const li      = o.line_items?.[0] || {};
    const product = li.name || 'Unknown';
    const variant = li.variant_title || '';

    db.prepare(`INSERT OR IGNORE INTO pending_orders (shopify_order_id, order_name, name, phone, email, product, variant, amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(String(o.id), o.name, name, phone, email, product, variant, amount);

    saveContact(phone, name);

    // 1 hour follow-up SMS if not paid
    setTimeout(async () => {
      const pending = db.prepare('SELECT * FROM pending_orders WHERE shopify_order_id = ?').get(String(o.id));
      if (!pending || pending.paid === 1 || pending.cancelled === 1) return;
      try {
        await sendSMS(phone, SMS_FOLLOWUP);
        db.prepare('UPDATE pending_orders SET followup_sent = followup_sent + 1 WHERE shopify_order_id = ?').run(String(o.id));
        await sendTelegram(
          `⏰ *Follow-up SMS Sent!*\n` +
          `👤 ${name} | 📱 0${phone}\n` +
          `🛒 ${o.name}\n` +
          `📦 ${product}\n` +
          `💰 ৳${amount}`
        );
      } catch(e) {
        console.error('1hr followup:', e.message);
        await sendTelegram(
          `❌ *Follow-up SMS Failed!*\n` +
          `👤 ${name} | 📱 0${phone}\n` +
          `🛒 ${o.name}\n` +
          `Error: ${e.message}`
        );
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
    const o   = req.body;
    const oid = String(o.id);
    const pending = db.prepare('SELECT * FROM pending_orders WHERE shopify_order_id = ?').get(oid);
    if (!pending) return;
    saveContact(pending.phone, pending.name);
    db.prepare('UPDATE pending_orders SET cancelled = 1 WHERE shopify_order_id = ?').run(oid);
    await sendTelegram(
      `🚫 *Order Cancelled*\n` +
      `👤 ${pending.name} | 📱 0${pending.phone}\n` +
      `🛒 ${pending.order_name}\n` +
      `📦 ${pending.product}\n` +
      `💰 ৳${pending.amount}\n` +
      `📱 Phone saved to contacts ✅`
    );
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
      await sendTelegram(
        `❌ *Failed Payment — FanFlix*\n` +
        `━━━━━━━━━━━━━━━━━━\n` +
        `👤 Name: ${p.customerName || 'N/A'}\n` +
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
    const waLink     = `wa.me/880${normalizePhone(phone)}`;

    const seen = db.prepare('SELECT id FROM payments WHERE eps_txn_id = ?').get(epsTxnId);
    if (seen) return;

    const recentDup = db.prepare(`SELECT id FROM payments WHERE phone = ? AND created_at > datetime('now', '-${config.DUPLICATE_WINDOW_MINUTES} minutes')`).get(normalizePhone(phone));
    if (recentDup) {
      await sendTelegram(
        `⚠️ *Duplicate Payment Alert!*\n` +
        `👤 ${name} | 📱 ${phone}\n` +
        `💰 ৳${totalAmt} | 🔖 ${reference}\n` +
        `🕐 ${time}`
      );
    }

    db.prepare('INSERT OR IGNORE INTO payments (eps_txn_id, phone, amount, status) VALUES (?, ?, ?, ?)')
      .run(epsTxnId, normalizePhone(phone), totalAmt, p.status);

    const pendingOrder = db.prepare(`SELECT * FROM pending_orders WHERE phone = ? AND paid = 0 AND cancelled = 0 ORDER BY created_at DESC LIMIT 1`).get(normalizePhone(phone));

    if (!pendingOrder) {
      db.prepare(`INSERT OR IGNORE INTO unmatched_payments (eps_txn_id, name, phone, email, total_amt, store_amt, method, reference, txn_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
        .run(epsTxnId, name, normalizePhone(phone), email, totalAmt, storeAmt, method, reference, p.transactionDate);
      await sendTelegram(
        `💰 *New Payment — FanFlix*\n` +
        `━━━━━━━━━━━━━━━━━━\n` +
        `👤 Name: ${name}\n` +
        `📱 Phone: ${phone}\n` +
        `📧 Email: ${email || 'N/A'}\n` +
        `📲 ${waLink}\n` +
        `━━━━━━━━━━━━━━━━━━\n` +
        `💰 Customer Paid: ৳${totalAmt}\n` +
        `🏪 You Receive: ৳${storeAmt}\n` +
        `📊 Gateway Fee: ৳${gatewayFee}\n` +
        `💳 Method: ${method}\n` +
        `🔖 Reference: ${reference}\n` +
        `🕐 Time: ${time}\n` +
        `━━━━━━━━━━━━━━━━━━\n` +
        `⚠️ No Shopify Order Found!`
      );
      return;
    }

    db.prepare('UPDATE pending_orders SET paid = 1 WHERE id = ?').run(pendingOrder.id);

    if (pendingOrder.followup_sent >= 1) {
      await sendTelegram(
        `✅ *Paid After Follow-up!*\n` +
        `👤 ${name} | 📱 ${phone}\n` +
        `🛒 ${pendingOrder.order_name}\n` +
        `📦 ${pendingOrder.product}\n` +
        `💰 ৳${totalAmt}\n` +
        `✅ Removed from unpaid list`
      );
    }

    const product      = pendingOrder.product;
    const variant      = pendingOrder.variant;
    const productType  = detectProductType(product);
    const oneTime      = isOneTime(productType);
    const durationDays = oneTime ? null : parseDuration(variant || product);
    const expiryDate   = oneTime ? null : addDays(durationDays);

    const existing     = db.prepare('SELECT * FROM customers WHERE phone = ? ORDER BY created_at DESC LIMIT 1').get(normalizePhone(phone));
    const renewalCount = existing ? existing.renewal_count + 1 : 1;
    const isVip        = renewalCount >= config.VIP_RENEWAL_COUNT ? 1 : 0;
    const dupOrder     = db.prepare(`SELECT * FROM customers WHERE phone = ? AND product = ? AND created_at > datetime('now', '-24 hours') LIMIT 1`).get(normalizePhone(phone), product);

    db.prepare(`INSERT INTO customers (name, phone, email, product, product_type, variant, order_id, order_name, amount, store_amount, duration_days, start_date, expiry_date, renewal_count, is_vip) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(name, normalizePhone(phone), email, product, productType, variant, pendingOrder.shopify_order_id, pendingOrder.order_name, totalAmt, storeAmt, durationDays, today(), expiryDate, renewalCount, isVip);

    let alert =
      `✅ *New Payment — FanFlix*\n` +
      `━━━━━━━━━━━━━━━━━━\n` +
      `👤 Name: ${name}\n` +
      `📱 Phone: ${phone}\n` +
      `📧 Email: ${email || 'N/A'}\n` +
      `📲 ${waLink}\n` +
      `━━━━━━━━━━━━━━━━━━\n` +
      `💰 Customer Paid: ৳${totalAmt}\n` +
      `🏪 You Receive: ৳${storeAmt}\n` +
      `📊 Gateway Fee: ৳${gatewayFee}\n` +
      `💳 Method: ${method}\n` +
      `🔖 Reference: ${reference}\n` +
      `🕐 Time: ${time}\n` +
      `━━━━━━━━━━━━━━━━━━\n` +
      `🛒 Order: ${pendingOrder.order_name}\n` +
      `${productTypeEmoji(productType)} | ${product}\n` +
      (variant ? `📦 Variant: ${variant}\n` : '') +
      (oneTime ? `🎁 One-time delivery\n` : `📅 Expires: ${formatDate(expiryDate)}\n`) +
      (renewalCount > 1 ? `🔄 Renewal #${renewalCount}\n` : '') +
      (isVip ? `⭐ VIP Customer\n` : '') +
      (dupOrder ? `⚠️ Possible Duplicate Order!\n` : '') +
      `━━━━━━━━━━━━━━━━━━`;

    await sendTelegram(alert);

  } catch(err) {
    console.error('IPN Error:', err.message);
    sendTelegram(`❌ *Bot Error:* ${err.message}`).catch(() => {});
  }
});

app.get('/', (req, res) => res.send('FanFlix Bot v5.2'));

// =============================================================
//  PAGINATION HELPERS
// =============================================================

const PAGE_SIZE = 5;

function showCustomerPage(chatId, page = 0) {
  const todayStr = today();
  const allRows  = db.prepare(`SELECT * FROM customers WHERE expiry_date >= ? ORDER BY product, expiry_date ASC`).all(todayStr);
  if (!allRows.length) return sendAutoDelete(chatId, 'No active customers.');

  const total      = allRows.length;
  const totalPages = Math.ceil(total / PAGE_SIZE);
  const rows       = allRows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  // Group by product
  const grouped = {};
  rows.forEach(c => {
    if (!grouped[c.product]) grouped[c.product] = [];
    grouped[c.product].push(c);
  });

  // Count per product in full list
  const productCounts = {};
  allRows.forEach(c => { productCounts[c.product] = (productCounts[c.product] || 0) + 1; });

  let text = `Active Customers (${total}) — Page ${page + 1}/${totalPages}\n━━━━━━━━━━━━━━━━━━\n`;
  Object.entries(grouped).forEach(([product, customers]) => {
    text += `\n📦 ${product} (${productCounts[product]})\n`;
    customers.forEach(c => {
      const d = daysUntil(c.expiry_date);
      text += `${c.is_vip ? '⭐' : ''}${c.name} | 0${c.phone} | ${formatDate(c.expiry_date)} | ${d}d\n`;
    });
  });

  const buttons = [];
  if (page > 0) buttons.push({ text: '◀️ Prev', callback_data: `cust_${page - 1}` });
  if (page < totalPages - 1) buttons.push({ text: 'Next ▶️', callback_data: `cust_${page + 1}` });
  const opts = buttons.length ? { reply_markup: { inline_keyboard: [buttons] } } : {};
  return sendAutoDelete(chatId, text, opts);
}

function showTodayPage(chatId, page = 0) {
  const todayStr = today();
  const allRows  = db.prepare(`SELECT * FROM customers WHERE start_date = ? ORDER BY product, created_at DESC`).all(todayStr);
  if (!allRows.length) return sendAutoDelete(chatId, 'No orders today.');

  const totalRev   = allRows.reduce((s, c) => s + (c.store_amount || 0), 0);
  const totalPages = Math.ceil(allRows.length / PAGE_SIZE);
  const rows       = allRows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  const grouped = {};
  rows.forEach(c => {
    if (!grouped[c.product]) grouped[c.product] = [];
    grouped[c.product].push(c);
  });

  let text = `Today: ${allRows.length} orders | ৳${totalRev.toFixed(0)} — Page ${page + 1}/${totalPages}\n━━━━━━━━━━━━━━━━━━\n`;
  Object.entries(grouped).forEach(([product, customers]) => {
    text += `\n📦 ${product}\n`;
    customers.forEach(c => { text += `${c.name} | ৳${c.store_amount || 0}\n`; });
  });

  const buttons = [];
  if (page > 0) buttons.push({ text: '◀️ Prev', callback_data: `today_${page - 1}` });
  if (page < totalPages - 1) buttons.push({ text: 'Next ▶️', callback_data: `today_${page + 1}` });
  const opts = buttons.length ? { reply_markup: { inline_keyboard: [buttons] } } : {};
  return sendAutoDelete(chatId, text, opts);
}

function showExpiringPage(chatId, page = 0) {
  const todayStr   = today();
  const in7days    = addDaysStr(todayStr, 7);
  const allRows    = db.prepare(`SELECT * FROM customers WHERE expiry_date >= ? AND expiry_date <= ? ORDER BY expiry_date ASC`).all(todayStr, in7days);
  if (!allRows.length) return sendAutoDelete(chatId, 'No one expiring this week!');

  const totalPages = Math.ceil(allRows.length / PAGE_SIZE);
  const rows       = allRows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  let text = `Expiring This Week (${allRows.length}) — Page ${page + 1}/${totalPages}\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach(c => {
    const d = daysUntil(c.expiry_date);
    text += `${c.name} | 0${c.phone}\n${c.product}\n${formatDate(c.expiry_date)} | ${d}d left\n\n`;
  });

  const buttons = [];
  if (page > 0) buttons.push({ text: '◀️ Prev', callback_data: `exp_${page - 1}` });
  if (page < totalPages - 1) buttons.push({ text: 'Next ▶️', callback_data: `exp_${page + 1}` });
  const opts = buttons.length ? { reply_markup: { inline_keyboard: [buttons] } } : {};
  return sendAutoDelete(chatId, text, opts);
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
    `FanFlix Bot v5.2\n\n` +
    `Commands:\n` +
    `/customers - Active customers\n` +
    `/expiring - Expiring this week\n` +
    `/today - Today orders\n` +
    `/revenue - Revenue report\n` +
    `/stats - Business overview\n` +
    `/product - Sales by product\n` +
    `/retention - Retention rate\n` +
    `/top - Top customers\n` +
    `/pending - Unmatched payments\n` +
    `/unpaid - Unpaid orders today\n` +
    `/edit - Edit expiry date\n` +
    `/export - Export customers CSV\n` +
    `/exportcontacts - Export contacts`
  );
});

bot.onText(/\/customers/, (msg) => { if (!isOwner(msg)) return; showCustomerPage(msg.chat.id, 0); });
bot.onText(/\/expiring/,  (msg) => { if (!isOwner(msg)) return; showExpiringPage(msg.chat.id, 0); });
bot.onText(/\/today/,     (msg) => { if (!isOwner(msg)) return; showTodayPage(msg.chat.id, 0); });

bot.onText(/\/revenue/, (msg) => {
  if (!isOwner(msg)) return;
  const todayStr = today();
  const t = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS total, COUNT(*) AS cnt FROM customers WHERE start_date = ?`).get(todayStr);
  const w = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS total, COUNT(*) AS cnt FROM customers WHERE start_date >= date(?, '-7 days')`).get(todayStr);
  const m = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS total, COUNT(*) AS cnt FROM customers WHERE start_date >= date(?, '-30 days')`).get(todayStr);
  sendAutoDelete(msg.chat.id,
    `Revenue Report\n━━━━━━━━━━━━━━━━━━\n` +
    `Today: ৳${t.total.toFixed(0)} (${t.cnt} orders)\n` +
    `Week:  ৳${w.total.toFixed(0)} (${w.cnt} orders)\n` +
    `Month: ৳${m.total.toFixed(0)} (${m.cnt} orders)`
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
  top.forEach((c, i) => { text += `${i+1}. ${c.name} — ${c.r}x\n`; });
  sendAutoDelete(msg.chat.id, text);
});

bot.onText(/\/top/, (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT name, phone, MAX(renewal_count) AS r, SUM(store_amount) AS spent FROM customers GROUP BY phone ORDER BY r DESC LIMIT 10`).all();
  if (!rows.length) return sendAutoDelete(msg.chat.id, 'No data.');
  let text = `Top Customers\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach((c, i) => { text += `${i+1}. ${c.name} | 0${c.phone}\n${c.r}x renewals | ৳${(c.spent||0).toFixed(0)}\n\n`; });
  sendAutoDelete(msg.chat.id, text);
});

bot.onText(/\/pending/, (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT * FROM unmatched_payments WHERE created_at >= datetime('now', '-2 days') ORDER BY created_at DESC`).all();
  if (!rows.length) return sendAutoDelete(msg.chat.id, 'No unmatched payments in last 2 days.');
  let text = `Unmatched Payments\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach(p => {
    text += `👤 ${p.name} | 📱 0${p.phone}\n`;
    text += `📧 ${p.email || 'N/A'}\n`;
    text += `💰 ৳${p.total_amt} | 🏪 ৳${p.store_amt}\n`;
    text += `💳 ${p.method} | 🔖 ${p.reference}\n`;
    text += `🕐 ${formatEPSTime(p.txn_time)}\n\n`;
  });
  sendAutoDelete(msg.chat.id, text);
});

bot.onText(/\/unpaid/, (msg) => {
  if (!isOwner(msg)) return;
  const todayStr = today();
  const rows = db.prepare(`SELECT * FROM pending_orders WHERE paid = 0 AND cancelled = 0 AND date(created_at, '+6 hours') = ? ORDER BY created_at ASC`).all(todayStr);
  if (!rows.length) return sendAutoDelete(msg.chat.id, 'No unpaid orders today.');
  let text = `Unpaid Orders Today (${rows.length})\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach((o, i) => {
    text += `${i+1}. ${o.order_name} — ${o.name}\n`;
    text += `📦 ${o.product}\n`;
    text += `💰 ৳${o.amount} | ⏰ ${timeAgo(o.created_at)}\n\n`;
  });
  sendAutoDelete(msg.chat.id, text);
});

bot.onText(/\/export/, async (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT * FROM customers ORDER BY created_at DESC`).all();
  if (!rows.length) return sendAutoDelete(msg.chat.id, 'No data.');
  let csv = 'Name,Phone,Email,Product,Type,Amount,Start,Expiry,Renewals,VIP\n';
  rows.forEach(c => {
    csv += `"${c.name}","0${c.phone}","${c.email}","${c.product}","${c.product_type}",${c.store_amount||0},${c.start_date},${c.expiry_date||'N/A'},${c.renewal_count},${c.is_vip?'Yes':'No'}\n`;
  });
  const sent = await bot.sendDocument(msg.chat.id, Buffer.from(csv,'utf8'), {}, { filename: `fanflix_customers_${today()}.csv`, contentType: 'text/csv' });
  setTimeout(() => bot.deleteMessage(msg.chat.id, sent.message_id).catch(() => {}), 60000);
});

bot.onText(/\/exportcontacts/, async (msg) => {
  if (!isOwner(msg)) return;
  if (!fs.existsSync(CONTACTS_FILE)) return sendAutoDelete(msg.chat.id, 'No contacts yet.');
  const sent = await bot.sendDocument(msg.chat.id, CONTACTS_FILE, {}, { filename: `fanflix_contacts_${today()}.txt`, contentType: 'text/plain' });
  setTimeout(() => bot.deleteMessage(msg.chat.id, sent.message_id).catch(() => {}), 60000);
});

// /edit
const editState = {};
bot.onText(/\/edit/, (msg) => {
  if (!isOwner(msg)) return;
  editState[msg.chat.id] = { step: 'phone' };
  sendAutoDelete(msg.chat.id, 'Enter phone number to edit:');
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
      return sendAutoDelete(cid, `${c.name} | ${c.product}\nExpiry: ${c.expiry_date || 'One-time'}\n\nNew date (YYYY-MM-DD):`);
    }
    if (s.step === 'date') {
      db.prepare(`UPDATE customers SET expiry_date=?, reminder_3_sent=0, reminder_1_sent=0 WHERE id=?`).run(text, s.customer.id);
      delete editState[cid];
      return sendAutoDelete(cid, `Updated to ${formatDate(text)}`);
    }
  }
});

// =============================================================
//  SCHEDULED TASKS
// =============================================================

// 7 PM - renewals + follow-up + lost alerts
cron.schedule('0 19 * * *', async () => {
  const todayStr = today();
  const in3days  = addDaysStr(todayStr, 3);
  const in1day   = addDaysStr(todayStr, 1);
  const lost3ago = addDaysStr(todayStr, -config.LOST_ALERT_DAYS_AFTER_EXPIRY);

  // Renewal SMS 3 days
  const in3 = db.prepare(`SELECT * FROM customers WHERE expiry_date = ? AND reminder_3_sent = 0`).all(in3days);
  for (const c of in3) {
    try {
      await sendSMS(c.phone, SMS_MSG3(c.product));
      db.prepare('UPDATE customers SET reminder_3_sent=1 WHERE id=?').run(c.id);
      await sendTelegram(
        `📩 *Renewal SMS Sent (3 days)*\n` +
        `👤 ${c.name} | 📱 0${c.phone}\n` +
        `🛒 ${c.order_name}\n` +
        `📦 ${c.product}\n` +
        `📅 Expires: ${formatDate(c.expiry_date)}`
      );
    } catch(e) { console.error('SMS 3d:', e.message); }
  }

  // Renewal SMS 1 day
  const in1 = db.prepare(`SELECT * FROM customers WHERE expiry_date = ? AND reminder_1_sent = 0`).all(in1day);
  for (const c of in1) {
    try {
      await sendSMS(c.phone, SMS_MSG1(c.product));
      db.prepare('UPDATE customers SET reminder_1_sent=1 WHERE id=?').run(c.id);
      await sendTelegram(
        `🚨 *Renewal SMS Sent (1 day)*\n` +
        `👤 ${c.name} | 📱 0${c.phone}\n` +
        `🛒 ${c.order_name}\n` +
        `📦 ${c.product}\n` +
        `📅 Expires: TOMORROW`
      );
    } catch(e) { console.error('SMS 1d:', e.message); }
  }

  // Lost customer alerts
  const lost = db.prepare(`SELECT * FROM customers WHERE expiry_date = ? AND lost_alert_sent = 0`).all(lost3ago);
  for (const c of lost) {
    try {
      await sendTelegram(`⚠️ *Lost Customer!*\n👤 ${c.name} | 0${c.phone}\n📦 ${c.product}\n💀 Expired ${config.LOST_ALERT_DAYS_AFTER_EXPIRY} days ago`);
      db.prepare('UPDATE customers SET lost_alert_sent=1 WHERE id=?').run(c.id);
    } catch(e) { console.error('Lost:', e.message); }
  }

  // Tomorrow expiry preview
  const tomorrow = db.prepare(`SELECT * FROM customers WHERE expiry_date = ?`).all(in1day);
  if (tomorrow.length) {
    let text = `📅 *Expiring Tomorrow (${tomorrow.length})*\n━━━━━━━━━━━━━━━━━━\n`;
    tomorrow.forEach(c => { text += `👤 ${c.name} | 0${c.phone}\n📦 ${c.product}\n\n`; });
    await sendTelegram(text);
  }

  // Unpaid orders - 2nd follow-up
  const unpaid = db.prepare(`SELECT * FROM pending_orders WHERE paid = 0 AND cancelled = 0 AND followup_sent >= 1 AND date(created_at, '+6 hours') >= date(?, '-2 days') ORDER BY created_at ASC`).all(todayStr);
  if (unpaid.length) {
    let text = `📋 *Unpaid Orders (${unpaid.length})*\n━━━━━━━━━━━━━━━━━━\n`;
    unpaid.forEach((o, i) => { text += `${i+1}. ${o.order_name} — ${o.name}\n📦 ${o.product} | ৳${o.amount} | ⏰ ${timeAgo(o.created_at)}\n\n`; });
    await sendTelegram(text);

    for (const o of unpaid) {
      try {
        await sendSMS(o.phone, SMS_FOLLOWUP);
        db.prepare('UPDATE pending_orders SET followup_sent = followup_sent + 1 WHERE id=?').run(o.id);
      } catch(e) { console.error('Followup SMS:', e.message); }
    }

    let cancelText = `❌ *Cancel These Orders:*\n━━━━━━━━━━━━━━━━━━\n`;
    unpaid.forEach((o, i) => { cancelText += `${i+1}. ${o.order_name}\n`; });
    cancelText += `\nCancel manually on Shopify.`;
    await sendTelegram(cancelText);
  }

  // Clean unmatched older than 2 days
  db.prepare(`DELETE FROM unmatched_payments WHERE created_at < datetime('now', '-2 days')`).run();
});

// 10:30 PM - daily summary + best day ever
cron.schedule('30 22 * * *', async () => {
  try {
    const todayStr = today();
    const t        = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS revenue, COUNT(*) AS orders FROM customers WHERE start_date = ?`).get(todayStr);
    const expiring = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date >= ? AND expiry_date <= ?`).get(todayStr, addDaysStr(todayStr, 7));
    const active   = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date >= ?`).get(todayStr);

    await sendTelegram(
      `📊 *Daily Summary*\n━━━━━━━━━━━━━━━━━━\n` +
      `✅ Orders: ${t.orders}\n` +
      `💰 Revenue: ৳${t.revenue.toFixed(0)}\n` +
      `👥 Active: ${active.cnt}\n` +
      `⚠️ Expiring This Week: ${expiring.cnt}`
    );

    const byProduct = db.prepare(`SELECT product, COUNT(*) AS cnt FROM customers WHERE expiry_date >= ? AND expiry_date <= ? GROUP BY product ORDER BY cnt DESC`).all(todayStr, addDaysStr(todayStr, 30));
    if (byProduct.length) {
      let text = `📅 *Expiring This Month*\n━━━━━━━━━━━━━━━━━━\n`;
      byProduct.forEach(r => { text += `${r.product} → ${r.cnt}\n`; });
      await sendTelegram(text);
    }

    const allDays = db.prepare(`SELECT start_date, SUM(store_amount) AS rev FROM customers GROUP BY start_date ORDER BY rev DESC LIMIT 1`).get();
    if (allDays && t.revenue > 0 && t.revenue >= allDays.rev) {
      await sendTelegram(
        `🏆 *Best Day Ever!*\n` +
        `💰 ৳${t.revenue.toFixed(0)}\n` +
        `📈 Previous: ৳${allDays.rev.toFixed(0)}\n` +
        `Congratulations! 🎉`
      );
    }
  } catch(e) { console.error('Summary:', e.message); }
});

// 1st of month - growth
cron.schedule('0 10 1 * *', async () => {
  try {
    const tm = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE start_date >= date('now','start of month')`).get();
    const lm = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE start_date >= date('now','start of month','-1 month') AND start_date < date('now','start of month')`).get();
    const g  = lm.cnt > 0 ? Math.round(((tm.cnt - lm.cnt) / lm.cnt) * 100) : 0;
    await sendTelegram(
      `${g >= 0 ? '📈' : '📉'} *Monthly Growth*\n` +
      `Last Month: ${lm.cnt}\nThis Month: ${tm.cnt}\nGrowth: ${g >= 0 ? '+' : ''}${g}%`
    );
  } catch(e) { console.error('Growth:', e.message); }
});

// =============================================================
//  START
// =============================================================

app.listen(config.PORT, () => {
  console.log(`FanFlix Bot v5.2 on port ${config.PORT}`);
  sendTelegram('🚀 *FanFlix Bot v5.2 Started!*').catch(() => {});
});
