process.env.TZ = 'Asia/Dhaka';
// =============================================
//   FANFLIX BOT v4.0 - FINAL CLEAN VERSION
// =============================================

const express     = require('express');
const TelegramBot = require('node-telegram-bot-api');
const Database    = require('better-sqlite3');
const cron        = require('node-cron');
const axios       = require('axios');
const crypto      = require('crypto');
const config      = require('./config');

const fs = require('fs');
const DB_DIR = '/app/data';
const DB_PATH = '/app/data/fanflix.db';
if (!fs.existsSync(DB_DIR)) { fs.mkdirSync(DB_DIR, { recursive: true }); }
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
  CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT
  );
`);

// Hardcoded SMS messages
const SMS_MSG3 = `প্রিয় গ্রাহক,

আপনার {product} সাবস্ক্রিপশনটি আগামী ৩ দিনের মধ্যে মেয়াদ শেষ হতে চলেছে।

বিরতিহীন সেবা উপভোগ করতে এখনই রিনিউ করুন।

📲 WhatsApp: wa.me/+8801928382918

— FanFlix BD`;

const SMS_MSG1 = `প্রিয় গ্রাহক,

আপনার {product} সাবস্ক্রিপশনটি আগামীকাল মেয়াদ শেষ হবে।

সার্ভিস বন্ধ হওয়ার আগেই রিনিউ করুন এবং নিরবচ্ছিন্ন বিনোদন উপভোগ করুন।

📲 WhatsApp: wa.me/+8801928382918

— FanFlix BD`;

const SMS_FOLLOWUP = `প্রিয় গ্রাহক,

আপনার অর্ডারটি এখনো সম্পন্ন হয়নি। পেমেন্ট না হওয়ায় অর্ডারটি পেন্ডিং অবস্থায় রয়েছে।

✅ পেমেন্ট সম্পন্ন করুন:
https://pg.eps.com.bd/DefaultPaymentLink?id=805A9AEE

📲 সহায়তায় WhatsApp করুন:
wa.me/+8801928382918

— FanFlix BD`;

const bot = new TelegramBot(config.TELEGRAM_BOT_TOKEN, { polling: true });
function sendTelegram(msg) {
  return bot.sendMessage(config.TELEGRAM_CHAT_ID, msg, { parse_mode: 'Markdown' });
}
function isOwner(msg) {
  return String(msg.chat.id) === String(config.TELEGRAM_CHAT_ID);
}


function esc(text) {
  return String(text || '').replace(/[_*[\]()~`>#+=|{}.!-]/g, '\\$&');
}

function decryptEPS(data) {
  const [ivBase64, cipherBase64] = data.split(':');
  const iv = Buffer.from(ivBase64, 'base64');
  const ct = Buffer.from(cipherBase64, 'base64');
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

function formatSMS(template, vars = {}) {
  return template
    .replace('{product}', vars.product || '')
    .replace('{link}', vars.link || config.EPS_PAYMENT_LINK);
}

async function sendSMS(phone, message) {
  const number = '880' + normalizePhone(phone);
  await axios.post('https://bulksmsbd.net/api/smsapi', null, {
    params: { api_key: config.SMS_API_KEY, senderid: config.SMS_SENDER_ID, number, message }
  });
}

const app = express();
app.use(express.json());

app.post('/shopify-order', async (req, res) => {
  res.sendStatus(200);
  try {
    const o = req.body;
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
  } catch(e) { console.error('Shopify webhook:', e.message); }
});

app.post('/eps-ipn', async (req, res) => {
  res.json({ status: 'OK' });
  try {
    const { Data } = req.body;
    if (!Data) return;
    const p = decryptEPS(Data);

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
        `🆔 EPS TXN: ${p.epsTransactionId}\n` +
        `🕐 Time: ${formatEPSTime(p.transactionDate)}\n` +
        `━━━━━━━━━━━━━━━━━━`
      );
      return;
    }

    const phone      = p.customerPhone    || '';
    const name       = p.customerName     || 'Customer';
    const email      = p.customerEmail    || '';
    const totalAmt   = parseFloat(p.totalAmount || 0);
    const storeAmt   = parseFloat(p.storeAmount || 0);
    const gatewayFee = (totalAmt - storeAmt).toFixed(2);
    const epsTxnId   = p.epsTransactionId || '';
    const method     = p.financialEntity  || 'N/A';
    const time       = formatEPSTime(p.transactionDate);

    const seen = db.prepare('SELECT id FROM payments WHERE eps_txn_id = ?').get(epsTxnId);
    if (seen) return;

    const recentDup = db.prepare(`SELECT id FROM payments WHERE phone = ? AND created_at > datetime('now', '-${config.DUPLICATE_WINDOW_MINUTES} minutes')`).get(normalizePhone(phone));
    if (recentDup) {
      await sendTelegram(`⚠️ *Duplicate Payment Alert!*\n👤 ${name}\n📱 ${phone}\n💰 ৳${totalAmt}\n🆔 ${epsTxnId}`);
    }

    db.prepare('INSERT OR IGNORE INTO payments (eps_txn_id, phone, amount, status) VALUES (?, ?, ?, ?)')
      .run(epsTxnId, normalizePhone(phone), totalAmt, p.status);

    const pendingOrder = db.prepare(`SELECT * FROM pending_orders WHERE phone = ? AND paid = 0 ORDER BY created_at DESC LIMIT 1`).get(normalizePhone(phone));

    if (!pendingOrder) {
      await sendTelegram(
        `💰 *New Payment — FanFlix*\n` +
        `━━━━━━━━━━━━━━━━━━\n` +
        `👤 Name: ${name}\n📱 Phone: ${phone}\n📧 Email: ${email || 'N/A'}\n` +
        `━━━━━━━━━━━━━━━━━━\n` +
        `💰 Customer Paid: ৳${totalAmt}\n🏪 You Receive: ৳${storeAmt}\n📊 Gateway Fee: ৳${gatewayFee}\n` +
        `💳 Method: ${method}\n🆔 EPS TXN: ${epsTxnId}\n🕐 Time: ${time}\n` +
        `━━━━━━━━━━━━━━━━━━\n⚠️ No Shopify Order Found!`
      );
      return;
    }

    db.prepare('UPDATE pending_orders SET paid = 1 WHERE id = ?').run(pendingOrder.id);

    if (pendingOrder.followup_sent === 1) {
      await sendTelegram(
        `✅ *Paid After Follow\-up\!*\n` +
        `━━━━━━━━━━━━━━━━━━\n` +
        `👤 ${esc(name)} \| 📱 ${phone}\n` +
        `🛒 Order: ${esc(pendingOrder.order_name)}\n` +
        `📦 ${esc(pendingOrder.product)}\n` +
        `💰 ৳${totalAmt}\n` +
        `✅ Order removed from unpaid list`
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

    const dupOrder = db.prepare(`SELECT * FROM customers WHERE phone = ? AND product = ? AND created_at > datetime('now', '-24 hours') LIMIT 1`).get(normalizePhone(phone), product);

    db.prepare(`INSERT INTO customers (name, phone, email, product, product_type, variant, order_id, order_name, amount, store_amount, duration_days, start_date, expiry_date, renewal_count, is_vip) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(name, normalizePhone(phone), email, product, productType, variant, pendingOrder.shopify_order_id, pendingOrder.order_name, totalAmt, storeAmt, durationDays, today(), expiryDate, renewalCount, isVip);

    let alert =
      `✅ *New Payment — FanFlix*\n━━━━━━━━━━━━━━━━━━\n` +
      `👤 Name: ${name}\n📱 Phone: ${phone}\n📧 Email: ${email || 'N/A'}\n` +
      `━━━━━━━━━━━━━━━━━━\n` +
      `💰 Customer Paid: ৳${totalAmt}\n🏪 You Receive: ৳${storeAmt}\n📊 Gateway Fee: ৳${gatewayFee}\n` +
      `💳 Method: ${method}\n🆔 EPS TXN: ${epsTxnId}\n🕐 Time: ${time}\n` +
      `━━━━━━━━━━━━━━━━━━\n` +
      `🛒 Order: ${pendingOrder.order_name}\n` +
      `${productTypeEmoji(productType)} | ${product}\n` +
      (variant ? `📦 Variant: ${variant}\n` : '') +
      (oneTime ? `🎁 One-time delivery — no expiry\n` : `📅 Expires: ${formatDate(expiryDate)}\n`) +
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

app.get('/', (req, res) => res.send('FanFlix Bot v4.0'));

// Commands
bot.onText(/\/start/, (msg) => {
  if (!isOwner(msg)) return;
  bot.sendMessage(msg.chat.id,
    `👋 FanFlix Bot v4.0\n\n` +
    `📋 Commands:\n` +
    `/customers - Active customers\n` +
    `/expiring - Expiring this week\n` +
    `/today - Today's orders\n` +
    `/revenue - Revenue report\n` +
    `/stats - Business overview\n` +
    `/product - Sales by product\n` +
    `/retention - Retention rate\n` +
    `/top - Top customers\n` +
    `/pending - Unmatched payments\n` +
    `/search 01874 - Find customer\n` +
    `/add - Add customer\n` +
    `/edit - Edit expiry date\n` +
    `/delete - Remove customer\n` +
    `/export - Export CSV`);
});

bot.onText(/\/customers/, (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT * FROM customers WHERE expiry_date >= date('now') ORDER BY expiry_date ASC LIMIT 20`).all();
  if (!rows.length) return bot.sendMessage(msg.chat.id, '📭 No active customers.');
  let text = `👥 *Active Customers (${rows.length})*\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach(c => { text += `${c.is_vip ? '⭐' : '👤'} ${c.name} | 📱 0${c.phone}\n📦 ${c.product}\n📅 ${formatDate(c.expiry_date)} (${daysUntil(c.expiry_date)}d)\n\n`; });
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

bot.onText(/\/expiring/, (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT * FROM customers WHERE expiry_date >= date('now') AND expiry_date <= date('now','+7 days') ORDER BY expiry_date ASC`).all();
  if (!rows.length) return bot.sendMessage(msg.chat.id, '✅ No one expiring this week!');
  let text = `⚠️ *Expiring This Week (${rows.length})*\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach(c => { text += `👤 ${c.name} | 📱 0${c.phone}\n📦 ${c.product} | ⏰ ${daysUntil(c.expiry_date)}d\n\n`; });
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

bot.onText(/\/today/, (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT * FROM customers WHERE start_date = date('now') ORDER BY created_at DESC`).all();
  if (!rows.length) return bot.sendMessage(msg.chat.id, '📭 No orders today.');
  const total = rows.reduce((s, c) => s + c.store_amount, 0);
  let text = `📅 *Today (${rows.length})*\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach((c, i) => { text += `${i+1}. ${c.name} — ${c.product} — ৳${c.store_amount}\n`; });
  text += `━━━━━━━━━━━━━━━━━━\n💰 Total: ৳${total.toFixed(2)}`;
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

bot.onText(/\/revenue/, (msg) => {
  if (!isOwner(msg)) return;
  const t = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS total, COUNT(*) AS cnt FROM customers WHERE start_date = date('now')`).get();
  const w = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS total, COUNT(*) AS cnt FROM customers WHERE start_date >= date('now','-7 days')`).get();
  const m = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS total, COUNT(*) AS cnt FROM customers WHERE start_date >= date('now','-30 days')`).get();
  bot.sendMessage(msg.chat.id,
    `💰 *Revenue*\n━━━━━━━━━━━━━━━━━━\n` +
    `📅 Today: ৳${t.total.toFixed(2)} (${t.cnt})\n` +
    `📅 Week:  ৳${w.total.toFixed(2)} (${w.cnt})\n` +
    `📅 Month: ৳${m.total.toFixed(2)} (${m.cnt})`,
    { parse_mode: 'Markdown' });
});

bot.onText(/\/stats/, (msg) => {
  if (!isOwner(msg)) return;
  const active  = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date >= date('now')`).get();
  const expired = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date < date('now')`).get();
  const onetime = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date IS NULL`).get();
  const total   = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS total FROM customers`).get();
  const vip     = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE is_vip = 1 AND expiry_date >= date('now')`).get();
  const best    = db.prepare(`SELECT product, COUNT(*) AS cnt FROM customers GROUP BY product ORDER BY cnt DESC LIMIT 1`).get();
  bot.sendMessage(msg.chat.id,
    `📊 *Stats*\n━━━━━━━━━━━━━━━━━━\n` +
    `👥 Total: ${active.cnt + expired.cnt + onetime.cnt}\n✅ Active: ${active.cnt}\n🎁 One-time: ${onetime.cnt}\n❌ Expired: ${expired.cnt}\n⭐ VIP: ${vip.cnt}\n💰 Revenue: ৳${total.total.toFixed(2)}\n🔥 Best: ${best?.product || 'N/A'}`,
    { parse_mode: 'Markdown' });
});

bot.onText(/\/product/, (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT product, COUNT(*) AS cnt, SUM(store_amount) AS rev FROM customers GROUP BY product ORDER BY cnt DESC`).all();
  if (!rows.length) return bot.sendMessage(msg.chat.id, '📭 No data.');
  let text = `📦 *By Product*\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach(r => { text += `📦 ${r.product}\n👥 ${r.cnt} | ৳${r.rev.toFixed(2)}\n\n`; });
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

bot.onText(/\/retention/, (msg) => {
  if (!isOwner(msg)) return;
  const total   = db.prepare(`SELECT COUNT(DISTINCT phone) AS cnt FROM customers`).get();
  const renewed = db.prepare(`SELECT COUNT(DISTINCT phone) AS cnt FROM customers WHERE renewal_count > 1`).get();
  const rate    = total.cnt > 0 ? Math.round((renewed.cnt / total.cnt) * 100) : 0;
  const top     = db.prepare(`SELECT name, phone, MAX(renewal_count) AS r FROM customers GROUP BY phone ORDER BY r DESC LIMIT 5`).all();
  let text = `📊 *Retention*\n━━━━━━━━━━━━━━━━━━\n👥 ${total.cnt} | 🔄 ${renewed.cnt} | 📈 ${rate}%\n\n⭐ Loyal:\n`;
  top.forEach((c, i) => { text += `${i+1}. ${c.name} — ${c.r}x\n`; });
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

bot.onText(/\/top/, (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT name, phone, MAX(renewal_count) AS r, SUM(store_amount) AS spent FROM customers GROUP BY phone ORDER BY r DESC LIMIT 10`).all();
  if (!rows.length) return bot.sendMessage(msg.chat.id, '📭 No data.');
  let text = `🏆 *Top Customers*\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach((c, i) => { text += `${i+1}. ${c.name} | 0${c.phone}\n🔄 ${c.r}x | ৳${c.spent.toFixed(2)}\n\n`; });
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

bot.onText(/\/pending/, (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT * FROM payments WHERE created_at >= datetime('now','-24 hours') AND phone NOT IN (SELECT phone FROM customers WHERE start_date = date('now'))`).all();
  if (!rows.length) return bot.sendMessage(msg.chat.id, '✅ No unmatched!');
  let text = `⚠️ *Unmatched*\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach(p => { text += `📱 0${p.phone} | ৳${p.amount}\n🆔 ${p.eps_txn_id}\n\n`; });
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

bot.onText(/\/search (.+)/, (msg, match) => {
  if (!isOwner(msg)) return;
  const q    = match[1].trim();
  const rows = db.prepare(`SELECT * FROM customers WHERE phone LIKE ? OR name LIKE ? ORDER BY created_at DESC LIMIT 10`).all(`%${normalizePhone(q)}%`, `%${q}%`);
  if (!rows.length) return bot.sendMessage(msg.chat.id, '🔍 Not found.');
  let text = `🔍 *${q}*\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach(c => {
    const d = c.expiry_date ? daysUntil(c.expiry_date) : null;
    const s = c.expiry_date ? (d > 0 ? `✅ Active (${d}d)` : '❌ Expired') : '🎁 One-time';
    text += `${c.is_vip ? '⭐' : '👤'} ${c.name} | 0${c.phone}\n📦 ${c.product}\n${s} | 🔄 #${c.renewal_count} | ৳${c.store_amount}\n\n`;
  });
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

bot.onText(/\/export/, async (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT * FROM customers ORDER BY created_at DESC`).all();
  if (!rows.length) return bot.sendMessage(msg.chat.id, '📭 No data.');
  let csv = 'Name,Phone,Email,Product,Type,Amount,Start,Expiry,Renewals,VIP\n';
  rows.forEach(c => { csv += `"${c.name}","0${c.phone}","${c.email}","${c.product}","${c.product_type}",${c.store_amount},${c.start_date},${c.expiry_date||'N/A'},${c.renewal_count},${c.is_vip?'Yes':'No'}\n`; });
  bot.sendDocument(msg.chat.id, Buffer.from(csv,'utf8'), {}, { filename: `fanflix_${today()}.csv`, contentType: 'text/csv' });
});

const addState = {}, editState = {}, deleteState = {};

bot.onText(/\/add/, (msg) => { if (!isOwner(msg)) return; addState[msg.chat.id] = { step: 'name' }; bot.sendMessage(msg.chat.id, '👤 Name:'); });
bot.onText(/\/edit/, (msg) => { if (!isOwner(msg)) return; editState[msg.chat.id] = { step: 'phone' }; bot.sendMessage(msg.chat.id, '📱 Phone to edit:'); });
bot.onText(/\/delete/, (msg) => { if (!isOwner(msg)) return; deleteState[msg.chat.id] = { step: 'phone' }; bot.sendMessage(msg.chat.id, '📱 Phone to delete:'); });

// SMS messages are hardcoded

bot.on('message', (msg) => {
  if (!isOwner(msg)) return;
  const cid = msg.chat.id;
  const text = msg.text || '';
  if (text.startsWith('/')) return;

  if (addState[cid]) {
    const s = addState[cid];
    if (s.step === 'name')     { s.name = text; s.step = 'phone'; return bot.sendMessage(cid, '📱 Phone:'); }
    if (s.step === 'phone')    { s.phone = normalizePhone(text); s.step = 'product'; return bot.sendMessage(cid, '📦 Product:'); }
    if (s.step === 'product')  { s.product = text; s.step = 'duration'; return bot.sendMessage(cid, '⏳ Days (0=one-time):'); }
    if (s.step === 'duration') {
      const days = parseInt(text) || 0;
      const exp  = days > 0 ? addDays(days) : null;
      db.prepare(`INSERT INTO customers (name,phone,product,product_type,amount,store_amount,duration_days,start_date,expiry_date,renewal_count) VALUES (?,?,?,?,0,0,?,?,?,1)`)
        .run(s.name, s.phone, s.product, detectProductType(s.product), days||null, today(), exp);
      delete addState[cid];
      return bot.sendMessage(cid, `✅ Added!\n👤 ${s.name}\n📦 ${s.product}\n📅 ${exp ? formatDate(exp) : 'One-time'}`);
    }
  }

  if (editState[cid]) {
    const s = editState[cid];
    if (s.step === 'phone') {
      const c = db.prepare(`SELECT * FROM customers WHERE phone = ? ORDER BY created_at DESC LIMIT 1`).get(normalizePhone(text));
      if (!c) { delete editState[cid]; return bot.sendMessage(cid, '❌ Not found.'); }
      s.customer = c; s.step = 'date';
      return bot.sendMessage(cid, `*${c.name}* | Expiry: ${c.expiry_date || 'One-time'}\n\nNew date (YYYY-MM-DD):`, { parse_mode: 'Markdown' });
    }
    if (s.step === 'date') {
      db.prepare(`UPDATE customers SET expiry_date=?, reminder_3_sent=0, reminder_1_sent=0 WHERE id=?`).run(text, s.customer.id);
      delete editState[cid];
      return bot.sendMessage(cid, `✅ Updated to ${formatDate(text)}`);
    }
  }

  if (deleteState[cid]) {
    const s = deleteState[cid];
    if (s.step === 'phone') {
      const c = db.prepare(`SELECT * FROM customers WHERE phone = ? ORDER BY created_at DESC LIMIT 1`).get(normalizePhone(text));
      if (!c) { delete deleteState[cid]; return bot.sendMessage(cid, '❌ Not found.'); }
      s.customer = c; s.step = 'confirm';
      return bot.sendMessage(cid, `*${c.name}* | ${c.product}\nType YES to confirm:`, { parse_mode: 'Markdown' });
    }
    if (s.step === 'confirm') {
      if (text === 'YES') { db.prepare(`DELETE FROM customers WHERE id=?`).run(s.customer.id); bot.sendMessage(cid, `✅ Deleted.`); }
      else bot.sendMessage(cid, '❌ Cancelled.');
      delete deleteState[cid];
    }
  }
});

// 9 AM - renewal reminders + lost alerts
cron.schedule('0 9 * * *', async () => {
  const in3 = db.prepare(`SELECT * FROM customers WHERE expiry_date = date('now','+3 days') AND reminder_3_sent = 0`).all();
  for (const c of in3) {
    try {
      await sendSMS(c.phone, formatSMS(SMS_MSG3, { product: c.product }));
      db.prepare('UPDATE customers SET reminder_3_sent=1 WHERE id=?').run(c.id);
      await sendTelegram(
        `📩 *Renewal SMS Sent \(3 days\)*\n` +
        `👤 ${esc(c.name)} \| 📱 0${c.phone}\n` +
        `🛒 Order: ${esc(c.order_name)}\n` +
        `📦 ${esc(c.product)}\n` +
        `📅 Expires: ${formatDate(c.expiry_date)}\n` +
        `💬 3\-day reminder SMS sent`
      );
    } catch(e) { console.error('SMS 3d:', e.message); }
  }
  const in1 = db.prepare(`SELECT * FROM customers WHERE expiry_date = date('now','+1 day') AND reminder_1_sent = 0`).all();
  for (const c of in1) {
    try {
      await sendSMS(c.phone, formatSMS(SMS_MSG1, { product: c.product }));
      db.prepare('UPDATE customers SET reminder_1_sent=1 WHERE id=?').run(c.id);
      await sendTelegram(
        `🚨 *Renewal SMS Sent \(1 day\)*\n` +
        `👤 ${esc(c.name)} \| 📱 0${c.phone}\n` +
        `🛒 Order: ${esc(c.order_name)}\n` +
        `📦 ${esc(c.product)}\n` +
        `📅 Expires: TOMORROW\n` +
        `💬 1\-day reminder SMS sent`
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

// 7 PM - tomorrow expiry + unpaid orders + bulk followup SMS
cron.schedule('0 19 * * *', async () => {
  try {
    const tomorrow = db.prepare(`SELECT * FROM customers WHERE expiry_date = date('now','+1 day')`).all();
    if (tomorrow.length) {
      let text = `📅 *Expiring Tomorrow (${tomorrow.length})*\n━━━━━━━━━━━━━━━━━━\n`;
      tomorrow.forEach(c => { text += `👤 ${c.name} — ${c.product}\n📱 0${c.phone}\n\n`; });
      await sendTelegram(text);
    }

    const unpaid = db.prepare(`SELECT * FROM pending_orders WHERE paid = 0 AND followup_sent >= 1 AND date(created_at) >= date('now', '-2 days') ORDER BY created_at ASC`).all();
    if (unpaid.length) {
      let text = `📋 *Unpaid Orders Today (${unpaid.length})*\n━━━━━━━━━━━━━━━━━━\n`;
      unpaid.forEach((o, i) => { text += `${i+1}. ${o.order_name} — ${o.name}\n📦 ${o.product}\n💰 ৳${o.amount}\n\n`; });
      text += `━━━━━━━━━━━━━━━━━━\n📤 Sending follow-up SMS...`;
      await sendTelegram(text);

      for (const o of unpaid) {
        try {
          await sendSMS(o.phone, formatSMS(SMS_FOLLOWUP, { link: config.EPS_PAYMENT_LINK }));
          db.prepare('UPDATE pending_orders SET followup_sent=1 WHERE id=?').run(o.id);
        } catch(e) { console.error('Followup SMS:', e.message); }
      }
      // Show order IDs to cancel
      let cancelText = `❌ *Still Unpaid — Cancel These Orders:*
━━━━━━━━━━━━━━━━━━
`;
      unpaid.forEach((o, i) => { cancelText += `${i+1}. ${o.order_name}
`; });
      cancelText += `
Cancel manually on Shopify.`;
      await bot.sendMessage(config.TELEGRAM_CHAT_ID, cancelText);
    }
  } catch(e) { console.error('10PM:', e.message); }
});

// 11 PM - daily summary + best day ever
cron.schedule('0 23 * * *', async () => {
  try {
    const t        = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS revenue, COUNT(*) AS orders FROM customers WHERE start_date = date('now')`).get();
    const expiring = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date >= date('now') AND expiry_date <= date('now','+7 days')`).get();
    const active   = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date >= date('now')`).get();

    await sendTelegram(
      `📊 *Daily Summary*\n━━━━━━━━━━━━━━━━━━\n` +
      `✅ Orders: ${t.orders}\n💰 Revenue: ৳${t.revenue.toFixed(2)}\n` +
      `👥 Active: ${active.cnt}\n⚠️ Expiring This Week: ${expiring.cnt}`
    );

    const byProduct = db.prepare(`SELECT product, COUNT(*) AS cnt FROM customers WHERE expiry_date >= date('now') AND expiry_date <= date('now','+30 days') GROUP BY product ORDER BY cnt DESC`).all();
    if (byProduct.length) {
      let text = `📅 *Expiring This Month*\n━━━━━━━━━━━━━━━━━━\n`;
      byProduct.forEach(r => { text += `📦 ${r.product} → ${r.cnt}\n`; });
      await sendTelegram(text);
    }

    const allDays = db.prepare(`SELECT start_date, SUM(store_amount) AS rev FROM customers GROUP BY start_date ORDER BY rev DESC LIMIT 1`).get();
    if (allDays && t.revenue > 0 && t.revenue >= allDays.rev) {
      await sendTelegram(`🏆 *Best Day Ever!*\n💰 ৳${t.revenue.toFixed(2)}\n📈 Previous: ৳${allDays.rev.toFixed(2)}\nCongratulations! 🎉`);
    }
  } catch(e) { console.error('Summary:', e.message); }
});

// 1st of month - growth
cron.schedule('0 10 1 * *', async () => {
  try {
    const tm = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE start_date >= date('now','start of month')`).get();
    const lm = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE start_date >= date('now','start of month','-1 month') AND start_date < date('now','start of month')`).get();
    const g  = lm.cnt > 0 ? Math.round(((tm.cnt - lm.cnt) / lm.cnt) * 100) : 0;
    await sendTelegram(`${g >= 0 ? '📈' : '📉'} *Monthly Growth*\nLast: ${lm.cnt} | This: ${tm.cnt} | ${g >= 0 ? '+' : ''}${g}%`);
  } catch(e) { console.error('Growth:', e.message); }
});

app.listen(config.PORT, () => {
  console.log(`FanFlix Bot v4.0 on port ${config.PORT}`);
  sendTelegram('🚀 *FanFlix Bot v4.0*\nClean & final! 💪').catch(() => {});
});
