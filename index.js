// =============================================
//   FANFLIX BOT v2.0
//   Complete Customer Management System
// =============================================

const express     = require('express');
const TelegramBot = require('node-telegram-bot-api');
const Database    = require('better-sqlite3');
const cron        = require('node-cron');
const axios       = require('axios');
const crypto      = require('crypto');
const config      = require('./config');


// =============================================================
//  DATABASE
// =============================================================

const db = new Database('fanflix.db');

db.exec(`
  CREATE TABLE IF NOT EXISTS customers (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    name              TEXT,
    phone             TEXT,
    email             TEXT,
    product           TEXT,
    variant           TEXT,
    order_id          TEXT,
    order_name        TEXT,
    amount            REAL,
    duration_days     INTEGER,
    start_date        TEXT,
    expiry_date       TEXT,
    renewal_count     INTEGER DEFAULT 1,
    is_vip            INTEGER DEFAULT 0,
    reminder_3_sent   INTEGER DEFAULT 0,
    reminder_1_sent   INTEGER DEFAULT 0,
    winback_sent      INTEGER DEFAULT 0,
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
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    shopify_order_id TEXT UNIQUE,
    order_name      TEXT,
    name            TEXT,
    phone           TEXT,
    email           TEXT,
    product         TEXT,
    amount          REAL,
    followup_sent   INTEGER DEFAULT 0,
    created_at      TEXT DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT
  );
`);

// Default SMS messages
const defaultSettings = {
  msg3:      'FanFlix: আপনার {product} subscription ৩ দিন পর শেষ হবে। Renew করুন: fanflixbd.com',
  msg1:      'FanFlix: আপনার {product} subscription আগামীকাল শেষ হবে! এখনই renew করুন: fanflixbd.com',
  winback:   'FanFlix: আপনাকে miss করছি! আজই ফিরে আসুন: fanflixbd.com',
  followup:  'FanFlix: আপনার order টি pending আছে! Payment করুন: {link}',
};

Object.entries(defaultSettings).forEach(([key, value]) => {
  db.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)').run(key, value);
});

function getSetting(key) {
  const row = db.prepare('SELECT value FROM settings WHERE key = ?').get(key);
  return row ? row.value : '';
}

function setSetting(key, value) {
  db.prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)').run(key, value);
}


// =============================================================
//  TELEGRAM
// =============================================================

const bot = new TelegramBot(config.TELEGRAM_BOT_TOKEN, { polling: true });

function sendTelegram(message) {
  return bot.sendMessage(config.TELEGRAM_CHAT_ID, message, { parse_mode: 'Markdown' });
}

function isOwner(msg) {
  return String(msg.chat.id) === String(config.TELEGRAM_CHAT_ID);
}


// =============================================================
//  UTILS
// =============================================================

function decryptEPS(data) {
  const [ivBase64, cipherBase64] = data.split(':');
  const iv         = Buffer.from(ivBase64, 'base64');
  const cipherText = Buffer.from(cipherBase64, 'base64');
  const key        = Buffer.alloc(32);
  Buffer.from(config.EPS_SECRET_KEY, 'utf8').copy(key);
  const decipher   = crypto.createDecipheriv('aes-256-cbc', key, iv);
  const decrypted  = Buffer.concat([decipher.update(cipherText), decipher.final()]);
  return JSON.parse(decrypted.toString('utf8'));
}

function normalizePhone(raw = '') {
  let p = String(raw).replace(/\D/g, '');
  if (p.startsWith('880')) p = p.slice(3);
  if (p.startsWith('0'))   p = p.slice(1);
  return p;
}

function parseDuration(text = '') {
  const t = text.toLowerCase();
  if (t.includes('1 year') || t.includes('12 month')) return 365;
  if (t.includes('6 month')) return 180;
  if (t.includes('3 month')) return 90;
  if (t.includes('2 month')) return 60;
  if (t.includes('1 month')) return 30;
  if (t.includes('7 day')   || t.includes('1 week'))  return 7;
  return 30;
}

function addDays(n) {
  const d = new Date();
  d.setDate(d.getDate() + n);
  return d.toISOString().split('T')[0];
}

function formatDate(dateStr) {
  return new Date(dateStr).toLocaleDateString('en-BD', {
    day: '2-digit', month: 'short', year: 'numeric'
  });
}

function daysUntil(dateStr) {
  const t = new Date(); t.setHours(0, 0, 0, 0);
  const e = new Date(dateStr); e.setHours(0, 0, 0, 0);
  return Math.ceil((e - t) / 86_400_000);
}

function today() {
  return new Date().toISOString().split('T')[0];
}

function formatSMS(template, vars = {}) {
  return template
    .replace('{product}', vars.product || '')
    .replace('{link}', vars.link || config.EPS_PAYMENT_LINK)
    .replace('{name}', vars.name || '');
}


// =============================================================
//  SHOPIFY
// =============================================================

async function getUnpaidOrders() {
  const { data } = await axios.get(
    `https://${config.SHOPIFY_STORE}/admin/api/2024-01/orders.json?status=open&financial_status=pending&limit=100`,
    { headers: { 'X-Shopify-Access-Token': config.SHOPIFY_TOKEN } }
  );
  return data.orders || [];
}

async function findShopifyOrder(phone) {
  const local  = normalizePhone(phone);
  const orders = await getUnpaidOrders();
  return orders.find(o => {
    const p = normalizePhone(o.phone || o.billing_address?.phone || '');
    return p === local;
  }) || null;
}

async function markShopifyPaid(orderId, amount) {
  await axios.post(
    `https://${config.SHOPIFY_STORE}/admin/api/2024-01/orders/${orderId}/transactions.json`,
    { transaction: { kind: 'capture', status: 'success', amount: String(amount) } },
    { headers: { 'X-Shopify-Access-Token': config.SHOPIFY_TOKEN, 'Content-Type': 'application/json' } }
  );
}


// =============================================================
//  SMS
// =============================================================

async function sendSMS(phone, message) {
  const number = '88' + normalizePhone(phone);
  await axios.post('https://bulksmsbd.net/api/smsapi', null, {
    params: { api_key: config.SMS_API_KEY, senderid: config.SMS_SENDER_ID, number, message }
  });
}


// =============================================================
//  EPS IPN
// =============================================================

const app = express();
app.use(express.json());

app.post('/eps-ipn', async (req, res) => {
  res.json({ status: 'OK', message: 'IPN received' });

  try {
    const { Data } = req.body;
    if (!Data) return;

    const p = decryptEPS(Data);
    if (p.status !== 'Success') return; // ignore failed silently

    const phone    = p.customerPhone    || '';
    const name     = p.customerName     || 'Customer';
    const email    = p.customerEmail    || '';
    const amount   = parseFloat(p.storeAmount || p.totalAmount || 0);
    const epsTxnId = p.epsTransactionId || '';

    // Skip already processed
    const seen = db.prepare('SELECT id FROM payments WHERE eps_txn_id = ?').get(epsTxnId);
    if (seen) return;

    // Duplicate phone check
    const recentDup = db.prepare(`
      SELECT id FROM payments WHERE phone = ?
      AND created_at > datetime('now', '-${config.DUPLICATE_WINDOW_MINUTES} minutes')
    `).get(normalizePhone(phone));

    if (recentDup) {
      await sendTelegram(
        `⚠️ *Duplicate Payment Alert!*\n` +
        `👤 Name: ${name}\n📱 Phone: ${phone}\n💰 Amount: ৳${amount}\n🆔 TXN: ${epsTxnId}`
      );
    }

    // Save payment
    db.prepare('INSERT OR IGNORE INTO payments (eps_txn_id, phone, amount, status) VALUES (?, ?, ?, ?)')
      .run(epsTxnId, normalizePhone(phone), amount, p.status);

    // Find Shopify order
    const order = await findShopifyOrder(phone);
    if (!order) {
      await sendTelegram(
        `⚠️ *No Shopify Order Found!*\n` +
        `👤 Name: ${name}\n📱 Phone: ${phone}\n💰 Amount: ৳${amount}\n🆔 TXN: ${epsTxnId}`
      );
      return;
    }

    const lineItem     = order.line_items?.[0] || {};
    const product      = lineItem.name || 'Unknown Product';
    const variant      = lineItem.variant_title || '';
    const durationDays = parseDuration(variant || product);
    const startDate    = today();
    const expiryDate   = addDays(durationDays);

    // Check if existing customer (renewal)
    const existing = db.prepare('SELECT * FROM customers WHERE phone = ? ORDER BY created_at DESC LIMIT 1')
      .get(normalizePhone(phone));

    const renewalCount = existing ? existing.renewal_count + 1 : 1;
    const isVip        = renewalCount >= config.VIP_RENEWAL_COUNT ? 1 : 0;

    // Mark Shopify paid
    await markShopifyPaid(order.id, amount);

    // Mark pending order as paid (cancel follow-up)
    db.prepare('UPDATE pending_orders SET followup_sent = 2 WHERE shopify_order_id = ?')
      .run(String(order.id));

    // Save customer
    db.prepare(`
      INSERT INTO customers
        (name, phone, email, product, variant, order_id, order_name, amount, duration_days, start_date, expiry_date, renewal_count, is_vip)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(name, normalizePhone(phone), email, product, variant, String(order.id), order.name, amount, durationDays, startDate, expiryDate, renewalCount, isVip);

    // Build Telegram alert
    let alert =
      `✅ *New Order!*\n` +
      `━━━━━━━━━━━━━━━━━━\n` +
      `👤 Name: ${name}\n` +
      `📱 Phone: ${phone}\n` +
      `📧 Email: ${email || 'N/A'}\n` +
      `🛒 Order: ${order.name}\n` +
      `📦 Product: ${product}${variant ? ` — ${variant}` : ''}\n` +
      `💰 Amount: ৳${amount}\n` +
      `📅 Expires: ${formatDate(expiryDate)}\n`;

    if (renewalCount > 1) alert += `🔄 Renewal #${renewalCount}\n`;
    if (isVip)            alert += `⭐ VIP Customer\n`;
    alert += `━━━━━━━━━━━━━━━━━━`;

    await sendTelegram(alert);

  } catch (err) {
    console.error('IPN Error:', err.message);
    sendTelegram(`❌ *Bot Error:* ${err.message}`).catch(() => {});
  }
});


// =============================================================
//  SHOPIFY WEBHOOK - New Order (for follow-up tracking)
// =============================================================

app.post('/shopify-order', async (req, res) => {
  res.sendStatus(200);
  try {
    const order   = req.body;
    const phone   = normalizePhone(order.phone || order.billing_address?.phone || '');
    const name    = order.billing_address?.name || order.email || 'Customer';
    const email   = order.email || '';
    const amount  = parseFloat(order.total_price || 0);
    const product = order.line_items?.[0]?.name || 'Unknown';

    if (!phone) return;

    // Save to pending orders
    db.prepare(`
      INSERT OR IGNORE INTO pending_orders (shopify_order_id, order_name, name, phone, email, product, amount)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(String(order.id), order.name, name, phone, email, product, amount);

    // Schedule follow-up after 1 hour
    setTimeout(async () => {
      const pending = db.prepare('SELECT * FROM pending_orders WHERE shopify_order_id = ?').get(String(order.id));
      if (!pending || pending.followup_sent !== 0) return; // already paid or sent

      const smsText = formatSMS(getSetting('followup'), { link: config.EPS_PAYMENT_LINK });
      await sendSMS(phone, smsText);

      db.prepare('UPDATE pending_orders SET followup_sent = 1 WHERE shopify_order_id = ?').run(String(order.id));

      await sendTelegram(
        `⏰ *Follow-up SMS Sent!*\n` +
        `👤 Name: ${name}\n📱 Phone: 0${phone}\n` +
        `🛒 Order: ${order.name}\n📦 Product: ${product}\n💰 Amount: ৳${amount}`
      );
    }, config.FOLLOW_UP_DELAY_MS);

  } catch (err) {
    console.error('Shopify webhook error:', err.message);
  }
});

app.get('/', (req, res) => res.send('✅ FanFlix Bot v2.0 Running'));


// =============================================================
//  BOT COMMANDS
// =============================================================

// /start
bot.onText(/\/start/, (msg) => {
  if (!isOwner(msg)) return;
  bot.sendMessage(msg.chat.id,
    `👋 *FanFlix Bot v2.0*\n\n` +
    `📋 *Commands:*\n` +
    `/customers — Active customers\n` +
    `/expiring — Expiring this week\n` +
    `/today — Today's orders\n` +
    `/revenue — Revenue report\n` +
    `/stats — Business overview\n` +
    `/product — Sales by product\n` +
    `/retention — Retention rate\n` +
    `/top — Top customers\n` +
    `/pending — Unmatched payments\n` +
    `/search 01874... — Find customer\n` +
    `/add — Add customer manually\n` +
    `/edit — Edit expiry date\n` +
    `/delete — Remove customer\n` +
    `/export — Export CSV\n\n` +
    `⚙️ *SMS Settings:*\n` +
    `/setmsg3 — 3-day reminder\n` +
    `/setmsg1 — 1-day reminder\n` +
    `/setwinback — Win-back SMS\n` +
    `/setfollowup — Follow-up SMS`,
    { parse_mode: 'Markdown' }
  );
});

// /customers
bot.onText(/\/customers/, (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT * FROM customers WHERE expiry_date >= date('now') ORDER BY expiry_date ASC LIMIT 20`).all();
  if (!rows.length) return bot.sendMessage(msg.chat.id, '📭 No active customers.');
  let text = `👥 *Active Customers (${rows.length})*\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach(c => {
    const d = daysUntil(c.expiry_date);
    text += `${c.is_vip ? '⭐' : '👤'} ${c.name} | 📱 0${c.phone}\n📦 ${c.product}\n📅 ${formatDate(c.expiry_date)} (${d}d left)\n\n`;
  });
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

// /expiring
bot.onText(/\/expiring/, (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT * FROM customers WHERE expiry_date >= date('now') AND expiry_date <= date('now','+7 days') ORDER BY expiry_date ASC`).all();
  if (!rows.length) return bot.sendMessage(msg.chat.id, '✅ No one expiring this week!');
  let text = `⚠️ *Expiring This Week (${rows.length})*\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach(c => {
    text += `👤 ${c.name} | 📱 0${c.phone}\n📦 ${c.product} | ⏰ ${daysUntil(c.expiry_date)} day(s)\n\n`;
  });
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

// /today
bot.onText(/\/today/, (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT * FROM customers WHERE start_date = date('now') ORDER BY created_at DESC`).all();
  if (!rows.length) return bot.sendMessage(msg.chat.id, '📭 No orders today.');
  const total = rows.reduce((s, c) => s + c.amount, 0);
  let text = `📅 *Today's Orders (${rows.length})*\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach((c, i) => {
    text += `${i + 1}. ${c.name} — ${c.product} — ৳${c.amount}\n`;
  });
  text += `━━━━━━━━━━━━━━━━━━\n💰 Total: ৳${total}`;
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

// /revenue
bot.onText(/\/revenue/, (msg) => {
  if (!isOwner(msg)) return;
  const t = db.prepare(`SELECT COALESCE(SUM(amount),0) AS total, COUNT(*) AS cnt FROM customers WHERE start_date = date('now')`).get();
  const w = db.prepare(`SELECT COALESCE(SUM(amount),0) AS total, COUNT(*) AS cnt FROM customers WHERE start_date >= date('now','-7 days')`).get();
  const m = db.prepare(`SELECT COALESCE(SUM(amount),0) AS total, COUNT(*) AS cnt FROM customers WHERE start_date >= date('now','-30 days')`).get();
  bot.sendMessage(msg.chat.id,
    `💰 *Revenue Report*\n━━━━━━━━━━━━━━━━━━\n` +
    `📅 Today:      ৳${t.total} (${t.cnt} orders)\n` +
    `📅 This Week:  ৳${w.total} (${w.cnt} orders)\n` +
    `📅 This Month: ৳${m.total} (${m.cnt} orders)`,
    { parse_mode: 'Markdown' }
  );
});

// /stats
bot.onText(/\/stats/, (msg) => {
  if (!isOwner(msg)) return;
  const active   = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date >= date('now')`).get();
  const expired  = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date < date('now')`).get();
  const total    = db.prepare(`SELECT COALESCE(SUM(amount),0) AS total FROM customers`).get();
  const vip      = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE is_vip = 1 AND expiry_date >= date('now')`).get();
  const best     = db.prepare(`SELECT product, COUNT(*) AS cnt FROM customers GROUP BY product ORDER BY cnt DESC LIMIT 1`).get();
  bot.sendMessage(msg.chat.id,
    `📊 *Business Overview*\n━━━━━━━━━━━━━━━━━━\n` +
    `👥 Total Customers: ${active.cnt + expired.cnt}\n` +
    `✅ Active: ${active.cnt}\n` +
    `❌ Expired: ${expired.cnt}\n` +
    `⭐ VIP: ${vip.cnt}\n` +
    `💰 Total Revenue: ৳${total.total}\n` +
    `🔥 Best Product: ${best?.product || 'N/A'}`,
    { parse_mode: 'Markdown' }
  );
});

// /product
bot.onText(/\/product/, (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT product, COUNT(*) AS cnt, SUM(amount) AS revenue FROM customers GROUP BY product ORDER BY cnt DESC`).all();
  if (!rows.length) return bot.sendMessage(msg.chat.id, '📭 No data.');
  let text = `📦 *Sales by Product*\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach(r => {
    text += `📦 ${r.product}\n👥 ${r.cnt} customers | 💰 ৳${r.revenue}\n\n`;
  });
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

// /retention
bot.onText(/\/retention/, (msg) => {
  if (!isOwner(msg)) return;
  const total    = db.prepare(`SELECT COUNT(DISTINCT phone) AS cnt FROM customers`).get();
  const renewed  = db.prepare(`SELECT COUNT(DISTINCT phone) AS cnt FROM customers WHERE renewal_count > 1`).get();
  const rate     = total.cnt > 0 ? Math.round((renewed.cnt / total.cnt) * 100) : 0;
  const topLoyal = db.prepare(`SELECT name, phone, MAX(renewal_count) AS renewals FROM customers GROUP BY phone ORDER BY renewals DESC LIMIT 5`).all();
  let text = `📊 *Retention Report*\n━━━━━━━━━━━━━━━━━━\n` +
    `👥 Total Customers: ${total.cnt}\n` +
    `🔄 Renewed: ${renewed.cnt}\n` +
    `📈 Retention Rate: ${rate}%\n\n` +
    `⭐ *Most Loyal:*\n`;
  topLoyal.forEach((c, i) => {
    text += `${i + 1}. ${c.name} — ${c.renewals} renewals\n`;
  });
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

// /top
bot.onText(/\/top/, (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT name, phone, MAX(renewal_count) AS renewals, SUM(amount) AS spent FROM customers GROUP BY phone ORDER BY renewals DESC LIMIT 10`).all();
  if (!rows.length) return bot.sendMessage(msg.chat.id, '📭 No data.');
  let text = `🏆 *Top Customers*\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach((c, i) => {
    text += `${i + 1}. ${c.name} | 📱 0${c.phone}\n🔄 ${c.renewals} renewals | 💰 ৳${c.spent}\n\n`;
  });
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

// /pending
bot.onText(/\/pending/, (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT * FROM payments WHERE eps_txn_id NOT IN (SELECT eps_txn_id FROM payments WHERE eps_txn_id IN (SELECT order_id FROM customers)) ORDER BY created_at DESC LIMIT 10`).all();
  // Simpler: just show unmatched payments from last 24h
  const unmatched = db.prepare(`
    SELECT p.* FROM payments p
    WHERE p.created_at >= datetime('now', '-24 hours')
    AND p.phone NOT IN (SELECT phone FROM customers WHERE start_date = date('now'))
  `).all();
  if (!unmatched.length) return bot.sendMessage(msg.chat.id, '✅ No unmatched payments!');
  let text = `⚠️ *Unmatched Payments*\n━━━━━━━━━━━━━━━━━━\n`;
  unmatched.forEach(p => {
    text += `📱 0${p.phone} | 💰 ৳${p.amount}\n🆔 ${p.eps_txn_id}\n\n`;
  });
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

// /search
bot.onText(/\/search (.+)/, (msg, match) => {
  if (!isOwner(msg)) return;
  const query = match[1].trim();
  const rows  = db.prepare(`
    SELECT * FROM customers WHERE phone LIKE ? OR name LIKE ? ORDER BY created_at DESC LIMIT 10
  `).all(`%${normalizePhone(query)}%`, `%${query}%`);
  if (!rows.length) return bot.sendMessage(msg.chat.id, '🔍 No customer found.');
  let text = `🔍 *Search: "${query}"*\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach(c => {
    const d = daysUntil(c.expiry_date);
    text += `${c.is_vip ? '⭐' : '👤'} ${c.name} | 📱 0${c.phone}\n📦 ${c.product}\n📅 ${formatDate(c.expiry_date)} | ${d > 0 ? `✅ Active (${d}d)` : '❌ Expired'}\n🛒 ${c.order_name} | 💰 ৳${c.amount} | 🔄 #${c.renewal_count}\n\n`;
  });
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

// /export
bot.onText(/\/export/, async (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT * FROM customers ORDER BY created_at DESC`).all();
  if (!rows.length) return bot.sendMessage(msg.chat.id, '📭 No data to export.');
  let csv = 'Name,Phone,Email,Product,Amount,Start Date,Expiry Date,Renewals,VIP\n';
  rows.forEach(c => {
    csv += `"${c.name}","0${c.phone}","${c.email}","${c.product}",${c.amount},${c.start_date},${c.expiry_date},${c.renewal_count},${c.is_vip ? 'Yes' : 'No'}\n`;
  });
  const buf = Buffer.from(csv, 'utf8');
  bot.sendDocument(msg.chat.id, buf, {}, { filename: `fanflix_customers_${today()}.csv`, contentType: 'text/csv' });
});

// /add — multi-step
const addState = {};

bot.onText(/\/add/, (msg) => {
  if (!isOwner(msg)) return;
  addState[msg.chat.id] = { step: 'name' };
  bot.sendMessage(msg.chat.id, '👤 Enter customer *name*:', { parse_mode: 'Markdown' });
});

// /edit
const editState = {};

bot.onText(/\/edit/, (msg) => {
  if (!isOwner(msg)) return;
  editState[msg.chat.id] = { step: 'phone' };
  bot.sendMessage(msg.chat.id, '📱 Enter customer *phone number* to edit:', { parse_mode: 'Markdown' });
});

// /delete
const deleteState = {};

bot.onText(/\/delete/, (msg) => {
  if (!isOwner(msg)) return;
  deleteState[msg.chat.id] = { step: 'phone' };
  bot.sendMessage(msg.chat.id, '📱 Enter customer *phone number* to delete:', { parse_mode: 'Markdown' });
});

// SMS settings
bot.onText(/\/setmsg3/, (msg) => {
  if (!isOwner(msg)) return;
  bot.sendMessage(msg.chat.id,
    `Current 3-day SMS:\n_${getSetting('msg3')}_\n\nSend new message (use {product} for product name):`,
    { parse_mode: 'Markdown' }
  );
  bot.once('message', (reply) => {
    if (!isOwner(reply)) return;
    setSetting('msg3', reply.text);
    bot.sendMessage(reply.chat.id, '✅ 3-day reminder SMS updated!');
  });
});

bot.onText(/\/setmsg1/, (msg) => {
  if (!isOwner(msg)) return;
  bot.sendMessage(msg.chat.id,
    `Current 1-day SMS:\n_${getSetting('msg1')}_\n\nSend new message:`,
    { parse_mode: 'Markdown' }
  );
  bot.once('message', (reply) => {
    if (!isOwner(reply)) return;
    setSetting('msg1', reply.text);
    bot.sendMessage(reply.chat.id, '✅ 1-day reminder SMS updated!');
  });
});

bot.onText(/\/setwinback/, (msg) => {
  if (!isOwner(msg)) return;
  bot.sendMessage(msg.chat.id,
    `Current win-back SMS:\n_${getSetting('winback')}_\n\nSend new message:`,
    { parse_mode: 'Markdown' }
  );
  bot.once('message', (reply) => {
    if (!isOwner(reply)) return;
    setSetting('winback', reply.text);
    bot.sendMessage(reply.chat.id, '✅ Win-back SMS updated!');
  });
});

bot.onText(/\/setfollowup/, (msg) => {
  if (!isOwner(msg)) return;
  bot.sendMessage(msg.chat.id,
    `Current follow-up SMS:\n_${getSetting('followup')}_\n\nSend new message (use {link} for payment link):`,
    { parse_mode: 'Markdown' }
  );
  bot.once('message', (reply) => {
    if (!isOwner(reply)) return;
    setSetting('followup', reply.text);
    bot.sendMessage(reply.chat.id, '✅ Follow-up SMS updated!');
  });
});

// Handle multi-step conversations
bot.on('message', (msg) => {
  if (!isOwner(msg)) return;
  const cid  = msg.chat.id;
  const text = msg.text || '';

  // ADD flow
  if (addState[cid]) {
    const state = addState[cid];
    if (state.step === 'name') {
      state.name = text; state.step = 'phone';
      return bot.sendMessage(cid, '📱 Enter phone number:');
    }
    if (state.step === 'phone') {
      state.phone = normalizePhone(text); state.step = 'product';
      return bot.sendMessage(cid, '📦 Enter product name (e.g. Netflix 1 Month):');
    }
    if (state.step === 'product') {
      state.product = text; state.step = 'duration';
      return bot.sendMessage(cid, '⏳ Enter duration in days (e.g. 30):');
    }
    if (state.step === 'duration') {
      const days       = parseInt(text) || 30;
      const expiryDate = addDays(days);
      db.prepare(`
        INSERT INTO customers (name, phone, product, amount, duration_days, start_date, expiry_date, renewal_count)
        VALUES (?, ?, ?, 0, ?, ?, ?, 1)
      `).run(state.name, state.phone, state.product, days, today(), expiryDate);
      delete addState[cid];
      return bot.sendMessage(cid, `✅ Customer added!\n👤 ${state.name}\n📦 ${state.product}\n📅 Expires: ${formatDate(expiryDate)}`);
    }
  }

  // EDIT flow
  if (editState[cid]) {
    const state = editState[cid];
    if (state.step === 'phone') {
      const phone = normalizePhone(text);
      const c     = db.prepare(`SELECT * FROM customers WHERE phone = ? ORDER BY created_at DESC LIMIT 1`).get(phone);
      if (!c) { delete editState[cid]; return bot.sendMessage(cid, '❌ Customer not found.'); }
      state.customer = c; state.step = 'date';
      return bot.sendMessage(cid, `Found: *${c.name}* | Current expiry: ${formatDate(c.expiry_date)}\n\nEnter new expiry date (YYYY-MM-DD):`, { parse_mode: 'Markdown' });
    }
    if (state.step === 'date') {
      db.prepare(`UPDATE customers SET expiry_date = ?, reminder_3_sent = 0, reminder_1_sent = 0 WHERE id = ?`).run(text, state.customer.id);
      delete editState[cid];
      return bot.sendMessage(cid, `✅ Expiry updated to *${formatDate(text)}*`, { parse_mode: 'Markdown' });
    }
  }

  // DELETE flow
  if (deleteState[cid]) {
    const state = deleteState[cid];
    if (state.step === 'phone') {
      const phone = normalizePhone(text);
      const c     = db.prepare(`SELECT * FROM customers WHERE phone = ? ORDER BY created_at DESC LIMIT 1`).get(phone);
      if (!c) { delete deleteState[cid]; return bot.sendMessage(cid, '❌ Customer not found.'); }
      state.customer = c; state.step = 'confirm';
      return bot.sendMessage(cid, `Found: *${c.name}* | ${c.product}\n\nType *YES* to confirm delete:`, { parse_mode: 'Markdown' });
    }
    if (state.step === 'confirm') {
      if (text === 'YES') {
        db.prepare(`DELETE FROM customers WHERE id = ?`).run(state.customer.id);
        bot.sendMessage(cid, `✅ Customer *${state.customer.name}* deleted.`, { parse_mode: 'Markdown' });
      } else {
        bot.sendMessage(cid, '❌ Delete cancelled.');
      }
      delete deleteState[cid];
    }
  }
});


// =============================================================
//  SCHEDULED TASKS
// =============================================================

// 9 AM — renewal reminders + win-back
cron.schedule('0 9 * * *', async () => {

  // 3-day reminder
  const in3 = db.prepare(`SELECT * FROM customers WHERE expiry_date = date('now','+3 days') AND reminder_3_sent = 0`).all();
  for (const c of in3) {
    try {
      await sendSMS(c.phone, formatSMS(getSetting('msg3'), { product: c.product }));
      db.prepare('UPDATE customers SET reminder_3_sent = 1 WHERE id = ?').run(c.id);
      await sendTelegram(`📩 *Renewal SMS (3 days)*\n👤 ${c.name} | 📱 0${c.phone}\n📦 ${c.product}\n📅 Expires: ${formatDate(c.expiry_date)}`);
    } catch (e) { console.error('SMS 3d:', e.message); }
  }

  // 1-day reminder
  const in1 = db.prepare(`SELECT * FROM customers WHERE expiry_date = date('now','+1 day') AND reminder_1_sent = 0`).all();
  for (const c of in1) {
    try {
      await sendSMS(c.phone, formatSMS(getSetting('msg1'), { product: c.product }));
      db.prepare('UPDATE customers SET reminder_1_sent = 1 WHERE id = ?').run(c.id);
      await sendTelegram(`🚨 *Renewal SMS (1 day)*\n👤 ${c.name} | 📱 0${c.phone}\n📦 ${c.product}\n📅 Expires: TOMORROW`);
    } catch (e) { console.error('SMS 1d:', e.message); }
  }

  // Win-back (5 days after expiry)
  const winback = db.prepare(`SELECT * FROM customers WHERE expiry_date = date('now','-${config.WINBACK_DAYS_AFTER_EXPIRY} days') AND winback_sent = 0`).all();
  for (const c of winback) {
    try {
      await sendSMS(c.phone, formatSMS(getSetting('winback'), { product: c.product }));
      db.prepare('UPDATE customers SET winback_sent = 1 WHERE id = ?').run(c.id);
      await sendTelegram(`🏆 *Win-back SMS Sent*\n👤 ${c.name} | 📱 0${c.phone}\n📦 ${c.product}`);
    } catch (e) { console.error('Winback:', e.message); }
  }

  // Lost customer alert (3 days after expiry)
  const lost = db.prepare(`SELECT * FROM customers WHERE expiry_date = date('now','-${config.LOST_ALERT_DAYS_AFTER_EXPIRY} days') AND lost_alert_sent = 0`).all();
  for (const c of lost) {
    try {
      await sendTelegram(`⚠️ *Lost Customer!*\n👤 ${c.name} | 📱 0${c.phone}\n📦 ${c.product}\n💀 Expired ${config.LOST_ALERT_DAYS_AFTER_EXPIRY} days ago`);
      db.prepare('UPDATE customers SET lost_alert_sent = 1 WHERE id = ?').run(c.id);
    } catch (e) { console.error('Lost alert:', e.message); }
  }
});

// 11 PM — daily summary + expiry by product
cron.schedule('0 23 * * *', async () => {
  try {
    const t        = db.prepare(`SELECT COALESCE(SUM(amount),0) AS revenue, COUNT(*) AS orders FROM customers WHERE start_date = date('now')`).get();
    const expiring = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date >= date('now') AND expiry_date <= date('now','+7 days')`).get();
    const active   = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date >= date('now')`).get();

    await sendTelegram(
      `📊 *Daily Summary*\n━━━━━━━━━━━━━━━━━━\n` +
      `✅ New Orders: ${t.orders}\n` +
      `💰 Revenue: ৳${t.revenue}\n` +
      `👥 Active Customers: ${active.cnt}\n` +
      `⚠️ Expiring This Week: ${expiring.cnt}`
    );

    // Expiry by product this month
    const byProduct = db.prepare(`
      SELECT product, COUNT(*) AS cnt FROM customers
      WHERE expiry_date >= date('now') AND expiry_date <= date('now','+30 days')
      GROUP BY product ORDER BY cnt DESC
    `).all();

    if (byProduct.length) {
      let text = `📅 *Expiring This Month by Product*\n━━━━━━━━━━━━━━━━━━\n`;
      byProduct.forEach(r => { text += `📦 ${r.product} → ${r.cnt} expiring\n`; });
      await sendTelegram(text);
    }
  } catch (e) { console.error('Summary:', e.message); }
});

// 1st of month — growth report
cron.schedule('0 10 1 * *', async () => {
  try {
    const thisMonth = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE start_date >= date('now','start of month')`).get();
    const lastMonth = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE start_date >= date('now','start of month','-1 month') AND start_date < date('now','start of month')`).get();
    const growth    = lastMonth.cnt > 0 ? Math.round(((thisMonth.cnt - lastMonth.cnt) / lastMonth.cnt) * 100) : 0;
    const arrow     = growth >= 0 ? '📈' : '📉';

    await sendTelegram(
      `${arrow} *Monthly Growth Report*\n━━━━━━━━━━━━━━━━━━\n` +
      `Last Month: ${lastMonth.cnt} customers\n` +
      `This Month: ${thisMonth.cnt} customers\n` +
      `Growth: ${growth >= 0 ? '+' : ''}${growth}%`
    );
  } catch (e) { console.error('Growth:', e.message); }
});


// =============================================================
//  START
// =============================================================

app.listen(config.PORT, () => {
  console.log(`FanFlix Bot v2.0 running on port ${config.PORT}`);
  sendTelegram('🚀 *FanFlix Bot v2.0 Started!*\nAll systems ready.').catch(() => {});
});
