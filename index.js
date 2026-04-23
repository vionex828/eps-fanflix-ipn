process.env.TZ = 'Asia/Dhaka';

// =============================================
//   FANFLIX BOT v5.0 - COMPLETE FINAL
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
//  DATABASE - Persistent storage
// =============================================================

const DB_DIR  = '/app/data';
const DB_PATH = '/app/data/fanflix.db';
if (!fs.existsSync(DB_DIR)) fs.mkdirSync(DB_DIR, { recursive: true });

const db = new Database(DB_PATH);

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
    store_amount      REAL,
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
//  HARDCODED SMS MESSAGES
// =============================================================

const SMS_MSG3 = (product) =>
  `প্রিয় গ্রাহক,\n\nআপনার ${product} সাবস্ক্রিপশনটি আগামী ৩ দিনের মধ্যে মেয়াদ শেষ হতে চলেছে।\n\nবিরতিহীন সেবা উপভোগ করতে এখনই রিনিউ করুন।\n\n📲 WhatsApp: wa.me/+8801928382918\n\n— FanFlix BD`;

const SMS_MSG1 = (product) =>
  `প্রিয় গ্রাহক,\n\nআপনার ${product} সাবস্ক্রিপশনটি আগামীকাল মেয়াদ শেষ হবে।\n\nসার্ভিস বন্ধ হওয়ার আগেই রিনিউ করুন এবং নিরবচ্ছিন্ন বিনোদন উপভোগ করুন।\n\n📲 WhatsApp: wa.me/+8801928382918\n\n— FanFlix BD`;

const SMS_FOLLOWUP =
  `প্রিয় গ্রাহক,\n\nআপনার অর্ডারটি এখনো সম্পন্ন হয়নি। পেমেন্ট না হওয়ায় অর্ডারটি পেন্ডিং অবস্থায় রয়েছে।\n\n✅ পেমেন্ট সম্পন্ন করুন:\nhttps://pg.eps.com.bd/DefaultPaymentLink?id=805A9AEE\n\n📲 সহায়তায় WhatsApp করুন:\nwa.me/+8801928382918\n\n— FanFlix BD`;

// =============================================================
//  TELEGRAM
// =============================================================

const bot = new TelegramBot(config.TELEGRAM_BOT_TOKEN, { polling: true });

function sendTelegram(msg) {
  return bot.sendMessage(config.TELEGRAM_CHAT_ID, msg, { parse_mode: 'Markdown' });
}

// Send command response that auto-deletes after 1 minute
async function sendAutoDelete(chatId, text, opts = {}) {
  const sent = await bot.sendMessage(chatId, text, opts);
  setTimeout(() => {
    bot.deleteMessage(chatId, sent.message_id).catch(() => {});
  }, 60 * 1000);
  return sent;
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
    const full = '0' + p;
    const contactsFile = '/app/data/contacts.txt';
    const existing = fs.existsSync(contactsFile) ? fs.readFileSync(contactsFile, 'utf8') : '';
    if (!existing.includes(full)) {
      fs.appendFileSync(contactsFile, `${full}${name ? ' | ' + name : ''}\n`);
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

function daysUntil(s) {
  const t = new Date(); t.setHours(0,0,0,0);
  const e = new Date(s); e.setHours(0,0,0,0);
  return Math.ceil((e - t) / 86400000);
}

function today() { return new Date().toISOString().split('T')[0]; }

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
//  SHOPIFY WEBHOOK
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

    // Save contact
    saveContact(phone, name);

  } catch(e) { console.error('Shopify webhook:', e.message); }
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

    // Save contact from every payment attempt
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

    // Skip already processed
    const seen = db.prepare('SELECT id FROM payments WHERE eps_txn_id = ?').get(epsTxnId);
    if (seen) return;

    // Duplicate phone check
    const recentDup = db.prepare(`SELECT id FROM payments WHERE phone = ? AND created_at > datetime('now', '-${config.DUPLICATE_WINDOW_MINUTES} minutes')`).get(normalizePhone(phone));
    if (recentDup) {
      await sendTelegram(
        `⚠️ *Duplicate Payment Alert!*\n` +
        `👤 ${name} | 📱 ${phone}\n` +
        `💰 ৳${totalAmt} | 🔖 ${reference}`
      );
    }

    // Save payment
    db.prepare('INSERT OR IGNORE INTO payments (eps_txn_id, phone, amount, status) VALUES (?, ?, ?, ?)')
      .run(epsTxnId, normalizePhone(phone), totalAmt, p.status);

    // Find matching pending order
    const pendingOrder = db.prepare(`SELECT * FROM pending_orders WHERE phone = ? AND paid = 0 ORDER BY created_at DESC LIMIT 1`).get(normalizePhone(phone));

    if (!pendingOrder) {
      // Save to unmatched table
      db.prepare(`INSERT OR IGNORE INTO unmatched_payments (eps_txn_id, name, phone, email, total_amt, store_amt, method, reference, txn_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
        .run(epsTxnId, name, normalizePhone(phone), email, totalAmt, storeAmt, method, reference, p.transactionDate);

      await sendTelegram(
        `💰 *New Payment — FanFlix*\n` +
        `━━━━━━━━━━━━━━━━━━\n` +
        `👤 Name: ${name}\n` +
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
        `⚠️ No Shopify Order Found!`
      );
      return;
    }

    // Mark order paid
    db.prepare('UPDATE pending_orders SET paid = 1 WHERE id = ?').run(pendingOrder.id);

    // Notify if paid after follow-up
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

app.get('/', (req, res) => res.send('FanFlix Bot v5.0'));

// =============================================================
//  PAGINATION HELPERS
// =============================================================

const PAGE_SIZE = 5;

function showCustomerPage(chatId, page = 0) {
  const allRows = db.prepare(`SELECT * FROM customers WHERE expiry_date >= date('now') ORDER BY product, expiry_date ASC`).all();
  if (!allRows.length) return sendAutoDelete(chatId, 'No active customers.');
  const total      = allRows.length;
  const totalPages = Math.ceil(total / PAGE_SIZE);
  const rows       = allRows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  let text = `Active Customers (${total}) — Page ${page + 1}/${totalPages}\n━━━━━━━━━━━━━━━━━━\n`;
  let lastProduct = '';
  rows.forEach(c => {
    if (c.product !== lastProduct) { text += `\n📦 ${c.product}\n`; lastProduct = c.product; }
    text += `${c.is_vip ? '⭐' : ''}${c.name} | 0${c.phone} | ${formatDate(c.expiry_date)}\n`;
  });

  const buttons = [];
  if (page > 0) buttons.push({ text: '◀️ Prev', callback_data: `cust_${page - 1}` });
  if (page < totalPages - 1) buttons.push({ text: 'Next ▶️', callback_data: `cust_${page + 1}` });
  const opts = buttons.length ? { reply_markup: { inline_keyboard: [buttons] } } : {};
  return sendAutoDelete(chatId, text, opts);
}

function showTodayPage(chatId, page = 0) {
  const allRows = db.prepare(`SELECT * FROM customers WHERE start_date = date('now') ORDER BY product, created_at DESC`).all();
  if (!allRows.length) return sendAutoDelete(chatId, 'No orders today.');
  const totalRev   = allRows.reduce((s, c) => s + c.store_amount, 0);
  const totalPages = Math.ceil(allRows.length / PAGE_SIZE);
  const rows       = allRows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  let text = `Today: ${allRows.length} orders | ৳${totalRev.toFixed(0)} — Page ${page + 1}/${totalPages}\n━━━━━━━━━━━━━━━━━━\n`;
  let lastProduct = '';
  rows.forEach(c => {
    if (c.product !== lastProduct) { text += `\n📦 ${c.product}\n`; lastProduct = c.product; }
    text += `${c.name} | ৳${c.store_amount}\n`;
  });

  const buttons = [];
  if (page > 0) buttons.push({ text: '◀️ Prev', callback_data: `today_${page - 1}` });
  if (page < totalPages - 1) buttons.push({ text: 'Next ▶️', callback_data: `today_${page + 1}` });
  const opts = buttons.length ? { reply_markup: { inline_keyboard: [buttons] } } : {};
  return sendAutoDelete(chatId, text, opts);
}

function showExpiringPage(chatId, page = 0) {
  const allRows = db.prepare(`SELECT * FROM customers WHERE expiry_date >= date('now') AND expiry_date <= date('now','+7 days') ORDER BY expiry_date ASC`).all();
  if (!allRows.length) return sendAutoDelete(chatId, 'No one expiring this week!');
  const totalPages = Math.ceil(allRows.length / PAGE_SIZE);
  const rows       = allRows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  let text = `Expiring This Week (${allRows.length}) — Page ${page + 1}/${totalPages}\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach(c => {
    text += `${c.name} | 0${c.phone}\n${c.product}\n${daysUntil(c.expiry_date)}d left\n\n`;
  });

  const buttons = [];
  if (page > 0) buttons.push({ text: '◀️ Prev', callback_data: `exp_${page - 1}` });
  if (page < totalPages - 1) buttons.push({ text: 'Next ▶️', callback_data: `exp_${page + 1}` });
  const opts = buttons.length ? { reply_markup: { inline_keyboard: [buttons] } } : {};
  return sendAutoDelete(chatId, text, opts);
}

// =============================================================
//  CALLBACK HANDLER (Pagination)
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
//  BOT COMMANDS (all auto-delete after 1 min)
// =============================================================

bot.onText(/\/start/, (msg) => {
  if (!isOwner(msg)) return;
  sendAutoDelete(msg.chat.id,
    `FanFlix Bot v5.0\n\n` +
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
    `/export - Export CSV`
  );
});

bot.onText(/\/customers/, (msg) => { if (!isOwner(msg)) return; showCustomerPage(msg.chat.id, 0); });
bot.onText(/\/expiring/,  (msg) => { if (!isOwner(msg)) return; showExpiringPage(msg.chat.id, 0); });
bot.onText(/\/today/,     (msg) => { if (!isOwner(msg)) return; showTodayPage(msg.chat.id, 0); });

bot.onText(/\/revenue/, (msg) => {
  if (!isOwner(msg)) return;
  const t = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS total, COUNT(*) AS cnt FROM customers WHERE start_date = date('now')`).get();
  const w = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS total, COUNT(*) AS cnt FROM customers WHERE start_date >= date('now','-7 days')`).get();
  const m = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS total, COUNT(*) AS cnt FROM customers WHERE start_date >= date('now','-30 days')`).get();
  sendAutoDelete(msg.chat.id,
    `Revenue Report\n━━━━━━━━━━━━━━━━━━\n` +
    `Today: ৳${t.total.toFixed(0)} (${t.cnt} orders)\n` +
    `Week:  ৳${w.total.toFixed(0)} (${w.cnt} orders)\n` +
    `Month: ৳${m.total.toFixed(0)} (${m.cnt} orders)`
  );
});

bot.onText(/\/stats/, (msg) => {
  if (!isOwner(msg)) return;
  const active  = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date >= date('now')`).get();
  const expired = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date < date('now')`).get();
  const onetime = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date IS NULL`).get();
  const total   = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS total FROM customers`).get();
  const vip     = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE is_vip = 1 AND expiry_date >= date('now')`).get();
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
  rows.forEach(r => { text += `${r.product}\n${r.cnt} orders | ৳${r.rev.toFixed(0)}\n\n`; });
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
  rows.forEach((c, i) => { text += `${i+1}. ${c.name} | 0${c.phone}\n${c.r}x renewals | ৳${c.spent.toFixed(0)}\n\n`; });
  sendAutoDelete(msg.chat.id, text);
});

bot.onText(/\/pending/, (msg) => {
  if (!isOwner(msg)) return;
  // Only show last 2 days
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
  const rows = db.prepare(`SELECT * FROM pending_orders WHERE paid = 0 AND date(created_at) = date('now') ORDER BY created_at ASC`).all();
  if (!rows.length) return sendAutoDelete(msg.chat.id, 'No unpaid orders today.');
  let text = `Unpaid Orders Today (${rows.length})\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach((o, i) => {
    text += `${i+1}. ${o.order_name} — ${o.name}\n`;
    text += `📦 ${o.product}\n`;
    text += `💰 ৳${o.amount}\n\n`;
  });
  sendAutoDelete(msg.chat.id, text);
});

bot.onText(/\/export/, async (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT * FROM customers ORDER BY created_at DESC`).all();
  if (!rows.length) return sendAutoDelete(msg.chat.id, 'No data.');
  let csv = 'Name,Phone,Email,Product,Type,Amount,Start,Expiry,Renewals,VIP\n';
  rows.forEach(c => {
    csv += `"${c.name}","0${c.phone}","${c.email}","${c.product}","${c.product_type}",${c.store_amount},${c.start_date},${c.expiry_date||'N/A'},${c.renewal_count},${c.is_vip?'Yes':'No'}\n`;
  });
  const sent = await bot.sendDocument(msg.chat.id, Buffer.from(csv,'utf8'), {}, { filename: `fanflix_${today()}.csv`, contentType: 'text/csv' });
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

// 9 AM - renewal reminders + lost alerts
cron.schedule('0 9 * * *', async () => {
  const in3 = db.prepare(`SELECT * FROM customers WHERE expiry_date = date('now','+3 days') AND reminder_3_sent = 0`).all();
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

  const in1 = db.prepare(`SELECT * FROM customers WHERE expiry_date = date('now','+1 day') AND reminder_1_sent = 0`).all();
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

  const lost = db.prepare(`SELECT * FROM customers WHERE expiry_date = date('now','-${config.LOST_ALERT_DAYS_AFTER_EXPIRY} days') AND lost_alert_sent = 0`).all();
  for (const c of lost) {
    try {
      await sendTelegram(`⚠️ *Lost Customer!*\n👤 ${c.name} | 0${c.phone}\n📦 ${c.product}\n💀 Expired ${config.LOST_ALERT_DAYS_AFTER_EXPIRY} days ago`);
      db.prepare('UPDATE customers SET lost_alert_sent=1 WHERE id=?').run(c.id);
    } catch(e) { console.error('Lost:', e.message); }
  }
});

// 7 PM - tomorrow expiry + unpaid orders + bulk follow-up SMS
cron.schedule('0 19 * * *', async () => {
  try {
    // Tomorrow expiry preview
    const tomorrow = db.prepare(`SELECT * FROM customers WHERE expiry_date = date('now','+1 day')`).all();
    if (tomorrow.length) {
      let text = `📅 *Expiring Tomorrow (${tomorrow.length})*\n━━━━━━━━━━━━━━━━━━\n`;
      tomorrow.forEach(c => { text += `👤 ${c.name} | 0${c.phone}\n📦 ${c.product}\n\n`; });
      await sendTelegram(text);
    }

    // Unpaid orders - send 2nd follow-up to those who already got 1st
    const unpaid = db.prepare(`SELECT * FROM pending_orders WHERE paid = 0 AND followup_sent >= 1 AND date(created_at) >= date('now', '-2 days') ORDER BY created_at ASC`).all();

    if (unpaid.length) {
      let text = `📋 *Unpaid Orders (${unpaid.length})*\n━━━━━━━━━━━━━━━━━━\n`;
      unpaid.forEach((o, i) => {
        text += `${i+1}. ${o.order_name} — ${o.name}\n📦 ${o.product} | ৳${o.amount}\n\n`;
      });
      await sendTelegram(text);

      // Send bulk follow-up SMS
      for (const o of unpaid) {
        try {
          await sendSMS(o.phone, SMS_FOLLOWUP);
          db.prepare('UPDATE pending_orders SET followup_sent = followup_sent + 1 WHERE id=?').run(o.id);
        } catch(e) { console.error('Followup SMS:', e.message); }
      }

      // Show order IDs to cancel
      let cancelText = `❌ *Still Unpaid — Cancel These:*\n━━━━━━━━━━━━━━━━━━\n`;
      unpaid.forEach((o, i) => { cancelText += `${i+1}. ${o.order_name}\n`; });
      cancelText += `\nCancel manually on Shopify.`;
      await sendTelegram(cancelText);
    }

    // Clean up unmatched payments older than 2 days
    db.prepare(`DELETE FROM unmatched_payments WHERE created_at < datetime('now', '-2 days')`).run();

  } catch(e) { console.error('7PM task:', e.message); }
});

// 11 PM - daily summary + best day ever
cron.schedule('0 23 * * *', async () => {
  try {
    const t        = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS revenue, COUNT(*) AS orders FROM customers WHERE start_date = date('now')`).get();
    const expiring = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date >= date('now') AND expiry_date <= date('now','+7 days')`).get();
    const active   = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date >= date('now')`).get();

    await sendTelegram(
      `📊 *Daily Summary*\n━━━━━━━━━━━━━━━━━━\n` +
      `✅ Orders: ${t.orders}\n` +
      `💰 Revenue: ৳${t.revenue.toFixed(0)}\n` +
      `👥 Active: ${active.cnt}\n` +
      `⚠️ Expiring This Week: ${expiring.cnt}`
    );

    const byProduct = db.prepare(`SELECT product, COUNT(*) AS cnt FROM customers WHERE expiry_date >= date('now') AND expiry_date <= date('now','+30 days') GROUP BY product ORDER BY cnt DESC`).all();
    if (byProduct.length) {
      let text = `📅 *Expiring This Month*\n━━━━━━━━━━━━━━━━━━\n`;
      byProduct.forEach(r => { text += `${r.product} → ${r.cnt}\n`; });
      await sendTelegram(text);
    }

    // Best day ever check
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

// 1st of month - growth report
cron.schedule('0 10 1 * *', async () => {
  try {
    const tm = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE start_date >= date('now','start of month')`).get();
    const lm = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE start_date >= date('now','start of month','-1 month') AND start_date < date('now','start of month')`).get();
    const g  = lm.cnt > 0 ? Math.round(((tm.cnt - lm.cnt) / lm.cnt) * 100) : 0;
    await sendTelegram(
      `${g >= 0 ? '📈' : '📉'} *Monthly Growth*\n` +
      `Last Month: ${lm.cnt}\n` +
      `This Month: ${tm.cnt}\n` +
      `Growth: ${g >= 0 ? '+' : ''}${g}%`
    );
  } catch(e) { console.error('Growth:', e.message); }
});

// =============================================================
//  START
// =============================================================

app.listen(config.PORT, () => {
  console.log(`FanFlix Bot v5.0 on port ${config.PORT}`);
  sendTelegram('🚀 *FanFlix Bot v5.0 Started!*').catch(() => {});
});
