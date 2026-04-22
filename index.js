// =============================================
//   FANFLIX BOT v3.0 - COMPLETE FINAL
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
    is_delivered      INTEGER DEFAULT 0,
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

const defaults = {
  msg3:     'FanFlix: আপনার {product} subscription ৩ দিন পর শেষ হবে। Renew করুন: fanflixbd.com',
  msg1:     'FanFlix: আপনার {product} subscription আগামীকাল শেষ হবে! Renew করুন: fanflixbd.com',
  winback:  'FanFlix: আপনাকে miss করছি! আজই ফিরে আসুন: fanflixbd.com',
  followup: 'FanFlix: আপনার order টি pending আছে! Payment করুন: {link}',
};
Object.entries(defaults).forEach(([k, v]) => {
  db.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)').run(k, v);
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

function sendTelegram(message, options = {}) {
  return bot.sendMessage(config.TELEGRAM_CHAT_ID, message, { parse_mode: 'Markdown', ...options });
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

// Detect product type
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
  if (type === 'giftcard')    return '🎁 Gift Card';
  if (type === 'software')    return '🔑 Software';
  if (type === 'ai')          return '🤖 AI Tool';
  return '📺 Subscription';
}

function isOneTime(type) {
  return type === 'giftcard' || type === 'software';
}

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

function formatDate(dateStr) {
  return new Date(dateStr).toLocaleDateString('en-BD', {
    day: '2-digit', month: 'short', year: 'numeric'
  });
}

function formatEPSTime(timeStr) {
  try {
    const d = new Date(timeStr);
    const date = d.toLocaleDateString('en-US', { day: '2-digit', month: 'long', year: 'numeric' });
    const time = d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: true });
    return `${date}, ${time}`;
  } catch {
    return timeStr;
  }
}

function daysUntil(dateStr) {
  const t = new Date(); t.setHours(0, 0, 0, 0);
  const e = new Date(dateStr); e.setHours(0, 0, 0, 0);
  return Math.ceil((e - t) / 86_400_000);
}

function today() {
  return new Date().toISOString().split('T')[0];
}

function isLateNight() {
  const hour = new Date().getHours();
  return hour >= 23 || hour < 7;
}

function formatSMS(template, vars = {}) {
  return template
    .replace('{product}', vars.product || '')
    .replace('{link}', vars.link || config.EPS_PAYMENT_LINK)
    .replace('{name}', vars.name || '');
}


// =============================================================
//  SMS - Fixed 880 format
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
    const order  = req.body;
    const phone  = normalizePhone(order.phone || order.billing_address?.phone || '');
    const name   = order.billing_address?.name || order.customer?.first_name || 'Customer';
    const email  = order.email || '';
    const amount = parseFloat(order.total_price || 0);
    const lineItem = order.line_items?.[0] || {};
    const product  = lineItem.name || 'Unknown Product';
    const variant  = lineItem.variant_title || '';

    if (!phone) return;

    db.prepare(`
      INSERT OR IGNORE INTO pending_orders
        (shopify_order_id, order_name, name, phone, email, product, variant, amount)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(String(order.id), order.name, name, phone, email, product, variant, amount);

    // Follow-up after 1 hour if unpaid
    setTimeout(async () => {
      const pending = db.prepare('SELECT * FROM pending_orders WHERE shopify_order_id = ?').get(String(order.id));
      if (!pending || pending.paid === 1 || pending.followup_sent === 1) return;
      try {
        const smsText = formatSMS(getSetting('followup'), { link: config.EPS_PAYMENT_LINK });
        await sendSMS(phone, smsText);
        db.prepare('UPDATE pending_orders SET followup_sent = 1 WHERE shopify_order_id = ?').run(String(order.id));
        await sendTelegram(
          `⏰ *Follow-up SMS Sent!*\n` +
          `👤 Name: ${name}\n📱 Phone: 0${phone}\n` +
          `🛒 Order: ${order.name}\n📦 Product: ${product}\n💰 Amount: ৳${amount}`
        );
      } catch(e) { console.error('Followup SMS error:', e.message); }
    }, config.FOLLOW_UP_DELAY_MS);

  } catch (err) {
    console.error('Shopify webhook error:', err.message);
  }
});


// =============================================================
//  EPS IPN
// =============================================================

app.post('/eps-ipn', async (req, res) => {
  res.json({ status: 'OK', message: 'IPN received' });

  try {
    const { Data } = req.body;
    if (!Data) return;

    const p = decryptEPS(Data);

    // Failed payment notification
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
    const totalAmt   = parseFloat(p.totalAmount  || 0);
    const storeAmt   = parseFloat(p.storeAmount  || 0);
    const gatewayFee = (totalAmt - storeAmt).toFixed(2);
    const epsTxnId   = p.epsTransactionId || '';
    const method     = p.financialEntity  || 'N/A';
    const time       = formatEPSTime(p.transactionDate);

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
        `👤 Name: ${name}\n📱 Phone: ${phone}\n` +
        `💰 Amount: ৳${totalAmt}\n🆔 TXN: ${epsTxnId}`
      );
    }

    // Save payment
    db.prepare('INSERT OR IGNORE INTO payments (eps_txn_id, phone, amount, status) VALUES (?, ?, ?, ?)')
      .run(epsTxnId, normalizePhone(phone), totalAmt, p.status);

    // Find matching pending order
    const pendingOrder = db.prepare(`
      SELECT * FROM pending_orders WHERE phone = ? AND paid = 0
      ORDER BY created_at DESC LIMIT 1
    `).get(normalizePhone(phone));

    if (!pendingOrder) {
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
        `🆔 EPS TXN: ${epsTxnId}\n` +
        `🕐 Time: ${time}\n` +
        `━━━━━━━━━━━━━━━━━━\n` +
        `⚠️ No Shopify Order Found!`
      );
      return;
    }

    // Mark order paid
    db.prepare('UPDATE pending_orders SET paid = 1 WHERE id = ?').run(pendingOrder.id);

    const product      = pendingOrder.product;
    const variant      = pendingOrder.variant;
    const productType  = detectProductType(product);
    const oneTime      = isOneTime(productType);
    const durationDays = oneTime ? null : parseDuration(variant || product);
    const startDate    = today();
    const expiryDate   = oneTime ? null : addDays(durationDays);

    // Check renewal & VIP
    const existing = db.prepare('SELECT * FROM customers WHERE phone = ? ORDER BY created_at DESC LIMIT 1')
      .get(normalizePhone(phone));
    const isFirstTime  = !existing;
    const renewalCount = existing ? existing.renewal_count + 1 : 1;
    const isVip        = renewalCount >= config.VIP_RENEWAL_COUNT ? 1 : 0;

    // Duplicate order detection (same product same customer recently)
    const dupOrder = db.prepare(`
      SELECT * FROM customers WHERE phone = ? AND product = ?
      AND created_at > datetime('now', '-24 hours')
      LIMIT 1
    `).get(normalizePhone(phone), product);

    // Save customer
    const customerId = db.prepare(`
      INSERT INTO customers
        (name, phone, email, product, product_type, variant, order_id, order_name, amount, store_amount, duration_days, start_date, expiry_date, renewal_count, is_vip)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      name, normalizePhone(phone), email,
      product, productType, variant,
      pendingOrder.shopify_order_id, pendingOrder.order_name,
      totalAmt, storeAmt, durationDays, startDate, expiryDate,
      renewalCount, isVip
    ).lastInsertRowid;

    // Build alert
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
      `🆔 EPS TXN: ${epsTxnId}\n` +
      `🕐 Time: ${time}\n` +
      `━━━━━━━━━━━━━━━━━━\n` +
      `🛒 Order: ${pendingOrder.order_name}\n` +
      `${productTypeEmoji(productType)} | ${product}${variant ? ` — ${variant}` : ''}\n`;

    if (oneTime) {
      alert += `🎁 One-time delivery — no expiry\n`;
    } else {
      alert += `📅 Expires: ${formatDate(expiryDate)}\n`;
    }

    if (isFirstTime)        alert += `🎉 First Time Customer!\n`;
    if (renewalCount > 1)   alert += `🔄 Renewal #${renewalCount}\n`;
    if (isVip)              alert += `⭐ VIP Customer\n`;
    if (dupOrder)           alert += `⚠️ Possible Duplicate Order! Same product ordered recently\n`;
    if (isLateNight())      alert += `🌙 Late Night Order — deliver tomorrow morning\n`;

    alert += `━━━━━━━━━━━━━━━━━━`;

    // Send with Delivered/Issue buttons
    await bot.sendMessage(config.TELEGRAM_CHAT_ID, alert, {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [[
          { text: '✅ Delivered', callback_data: `delivered_${customerId}` },
          { text: '❌ Issue', callback_data: `issue_${customerId}` }
        ]]
      }
    });

    // 30 min undelivered reminder
    setTimeout(async () => {
      const c = db.prepare('SELECT * FROM customers WHERE id = ?').get(customerId);
      if (!c || c.is_delivered === 1) return;
      await sendTelegram(
        `⏰ *Delivery Reminder!*\n` +
        `👤 ${name} | 📱 ${phone}\n` +
        `📦 ${product}\n` +
        `💰 ৳${totalAmt}\n` +
        `⚠️ 30 mins passed — delivered?`
      );
    }, 30 * 60 * 1000);

  } catch (err) {
    console.error('IPN Error:', err.message);
    sendTelegram(`❌ *Bot Error:* ${err.message}`).catch(() => {});
  }
});

app.get('/', (req, res) => res.send('✅ FanFlix Bot v3.0 Running'));


// =============================================================
//  CALLBACK HANDLERS (Delivered / Issue buttons)
// =============================================================

bot.on('callback_query', async (query) => {
  const data = query.data;
  const chatId = query.message.chat.id;
  const msgId  = query.message.message_id;

  if (data.startsWith('delivered_')) {
    const id = parseInt(data.split('_')[1]);
    db.prepare('UPDATE customers SET is_delivered = 1 WHERE id = ?').run(id);
    await bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: msgId });
    await bot.answerCallbackQuery(query.id, { text: '✅ Marked as delivered!' });
    await bot.sendMessage(chatId, `✅ *Order delivered!*`, { parse_mode: 'Markdown' });
  }

  if (data.startsWith('issue_')) {
    const id = parseInt(data.split('_')[1]);
    await bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: msgId });
    await bot.answerCallbackQuery(query.id, { text: 'Select issue type' });
    await bot.sendMessage(chatId, `❌ *What is the issue?*`, {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [{ text: '💳 Wrong Payment', callback_data: `issuetype_${id}_wrong_payment` }],
          [{ text: '📦 Wrong Product', callback_data: `issuetype_${id}_wrong_product` }],
          [{ text: '👤 Wrong Customer', callback_data: `issuetype_${id}_wrong_customer` }],
          [{ text: '🔄 Duplicate Order', callback_data: `issuetype_${id}_duplicate` }],
          [{ text: '⚠️ Other', callback_data: `issuetype_${id}_other` }],
        ]
      }
    });
  }

  if (data.startsWith('issuetype_')) {
    const parts   = data.split('_');
    const id      = parts[1];
    const issueType = parts.slice(2).join(' ');
    await bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: msgId });
    await bot.answerCallbackQuery(query.id, { text: 'Issue recorded!' });
    await bot.sendMessage(chatId, `⚠️ *Issue recorded:* ${issueType}\nOrder ID: ${id}`, { parse_mode: 'Markdown' });
  }
});


// =============================================================
//  BOT COMMANDS
// =============================================================

bot.onText(/\/start/, (msg) => {
  if (!isOwner(msg)) return;
  bot.sendMessage(msg.chat.id,
    `👋 *FanFlix Bot v3.0*\n\n` +
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

bot.onText(/\/customers/, (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT * FROM customers WHERE expiry_date >= date('now') ORDER BY expiry_date ASC LIMIT 20`).all();
  if (!rows.length) return bot.sendMessage(msg.chat.id, '📭 No active customers.');
  let text = `👥 *Active Customers (${rows.length})*\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach(c => {
    text += `${c.is_vip ? '⭐' : '👤'} ${c.name} | 📱 0${c.phone}\n📦 ${c.product}\n📅 ${formatDate(c.expiry_date)} (${daysUntil(c.expiry_date)}d left)\n\n`;
  });
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

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

bot.onText(/\/today/, (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT * FROM customers WHERE start_date = date('now') ORDER BY created_at DESC`).all();
  if (!rows.length) return bot.sendMessage(msg.chat.id, '📭 No orders today.');
  const total = rows.reduce((s, c) => s + c.store_amount, 0);
  let text = `📅 *Today's Orders (${rows.length})*\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach((c, i) => { text += `${i + 1}. ${c.name} — ${c.product} — ৳${c.store_amount}\n`; });
  text += `━━━━━━━━━━━━━━━━━━\n💰 Total: ৳${total.toFixed(2)}`;
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

bot.onText(/\/revenue/, (msg) => {
  if (!isOwner(msg)) return;
  const t = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS total, COUNT(*) AS cnt FROM customers WHERE start_date = date('now')`).get();
  const w = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS total, COUNT(*) AS cnt FROM customers WHERE start_date >= date('now','-7 days')`).get();
  const m = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS total, COUNT(*) AS cnt FROM customers WHERE start_date >= date('now','-30 days')`).get();
  bot.sendMessage(msg.chat.id,
    `💰 *Revenue Report*\n━━━━━━━━━━━━━━━━━━\n` +
    `📅 Today:      ৳${t.total.toFixed(2)} (${t.cnt} orders)\n` +
    `📅 This Week:  ৳${w.total.toFixed(2)} (${w.cnt} orders)\n` +
    `📅 This Month: ৳${m.total.toFixed(2)} (${m.cnt} orders)`,
    { parse_mode: 'Markdown' }
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
  bot.sendMessage(msg.chat.id,
    `📊 *Business Overview*\n━━━━━━━━━━━━━━━━━━\n` +
    `👥 Total: ${active.cnt + expired.cnt + onetime.cnt}\n` +
    `✅ Active Subscriptions: ${active.cnt}\n` +
    `🎁 One-time Deliveries: ${onetime.cnt}\n` +
    `❌ Expired: ${expired.cnt}\n` +
    `⭐ VIP: ${vip.cnt}\n` +
    `💰 Total Revenue: ৳${total.total.toFixed(2)}\n` +
    `🔥 Best Product: ${best?.product || 'N/A'}`,
    { parse_mode: 'Markdown' }
  );
});

bot.onText(/\/product/, (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT product, COUNT(*) AS cnt, SUM(store_amount) AS revenue FROM customers GROUP BY product ORDER BY cnt DESC`).all();
  if (!rows.length) return bot.sendMessage(msg.chat.id, '📭 No data.');
  let text = `📦 *Sales by Product*\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach(r => { text += `📦 ${r.product}\n👥 ${r.cnt} | 💰 ৳${r.revenue.toFixed(2)}\n\n`; });
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

bot.onText(/\/retention/, (msg) => {
  if (!isOwner(msg)) return;
  const total   = db.prepare(`SELECT COUNT(DISTINCT phone) AS cnt FROM customers`).get();
  const renewed = db.prepare(`SELECT COUNT(DISTINCT phone) AS cnt FROM customers WHERE renewal_count > 1`).get();
  const rate    = total.cnt > 0 ? Math.round((renewed.cnt / total.cnt) * 100) : 0;
  const top     = db.prepare(`SELECT name, phone, MAX(renewal_count) AS renewals FROM customers GROUP BY phone ORDER BY renewals DESC LIMIT 5`).all();
  let text = `📊 *Retention Report*\n━━━━━━━━━━━━━━━━━━\n` +
    `👥 Total: ${total.cnt}\n🔄 Renewed: ${renewed.cnt}\n📈 Rate: ${rate}%\n\n⭐ *Most Loyal:*\n`;
  top.forEach((c, i) => { text += `${i + 1}. ${c.name} — ${c.renewals} renewals\n`; });
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

bot.onText(/\/top/, (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT name, phone, MAX(renewal_count) AS renewals, SUM(store_amount) AS spent FROM customers GROUP BY phone ORDER BY renewals DESC LIMIT 10`).all();
  if (!rows.length) return bot.sendMessage(msg.chat.id, '📭 No data.');
  let text = `🏆 *Top Customers*\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach((c, i) => { text += `${i + 1}. ${c.name} | 📱 0${c.phone}\n🔄 ${c.renewals} renewals | 💰 ৳${c.spent.toFixed(2)}\n\n`; });
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

bot.onText(/\/pending/, (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`
    SELECT * FROM payments WHERE created_at >= datetime('now', '-24 hours')
    AND phone NOT IN (SELECT phone FROM customers WHERE start_date = date('now'))
  `).all();
  if (!rows.length) return bot.sendMessage(msg.chat.id, '✅ No unmatched payments!');
  let text = `⚠️ *Unmatched Payments*\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach(p => { text += `📱 0${p.phone} | 💰 ৳${p.amount}\n🆔 ${p.eps_txn_id}\n\n`; });
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

bot.onText(/\/search (.+)/, (msg, match) => {
  if (!isOwner(msg)) return;
  const query = match[1].trim();
  const rows  = db.prepare(`SELECT * FROM customers WHERE phone LIKE ? OR name LIKE ? ORDER BY created_at DESC LIMIT 10`)
    .all(`%${normalizePhone(query)}%`, `%${query}%`);
  if (!rows.length) return bot.sendMessage(msg.chat.id, '🔍 No customer found.');
  let text = `🔍 *"${query}"*\n━━━━━━━━━━━━━━━━━━\n`;
  rows.forEach(c => {
    const d = c.expiry_date ? daysUntil(c.expiry_date) : null;
    const status = c.expiry_date ? (d > 0 ? `✅ Active (${d}d)` : '❌ Expired') : '🎁 One-time';
    text += `${c.is_vip ? '⭐' : '👤'} ${c.name} | 📱 0${c.phone}\n📦 ${c.product}\n${status} | 🔄 #${c.renewal_count} | 💰 ৳${c.store_amount}\n\n`;
  });
  bot.sendMessage(msg.chat.id, text, { parse_mode: 'Markdown' });
});

bot.onText(/\/export/, async (msg) => {
  if (!isOwner(msg)) return;
  const rows = db.prepare(`SELECT * FROM customers ORDER BY created_at DESC`).all();
  if (!rows.length) return bot.sendMessage(msg.chat.id, '📭 No data.');
  let csv = 'Name,Phone,Email,Product,Type,Amount,Start,Expiry,Renewals,VIP\n';
  rows.forEach(c => {
    csv += `"${c.name}","0${c.phone}","${c.email}","${c.product}","${c.product_type}",${c.store_amount},${c.start_date},${c.expiry_date || 'N/A'},${c.renewal_count},${c.is_vip ? 'Yes' : 'No'}\n`;
  });
  bot.sendDocument(msg.chat.id, Buffer.from(csv, 'utf8'), {}, { filename: `fanflix_${today()}.csv`, contentType: 'text/csv' });
});

// /add
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
  bot.sendMessage(msg.chat.id, '📱 Enter phone number to edit:', { parse_mode: 'Markdown' });
});

// /delete
const deleteState = {};
bot.onText(/\/delete/, (msg) => {
  if (!isOwner(msg)) return;
  deleteState[msg.chat.id] = { step: 'phone' };
  bot.sendMessage(msg.chat.id, '📱 Enter phone number to delete:', { parse_mode: 'Markdown' });
});

// SMS settings
bot.onText(/\/setmsg3/, (msg) => {
  if (!isOwner(msg)) return;
  bot.sendMessage(msg.chat.id, `Current:\n_${getSetting('msg3')}_\n\nSend new message (use {product}):`, { parse_mode: 'Markdown' });
  bot.once('message', (r) => { if (!isOwner(r)) return; setSetting('msg3', r.text); bot.sendMessage(r.chat.id, '✅ Updated!'); });
});
bot.onText(/\/setmsg1/, (msg) => {
  if (!isOwner(msg)) return;
  bot.sendMessage(msg.chat.id, `Current:\n_${getSetting('msg1')}_\n\nSend new message:`, { parse_mode: 'Markdown' });
  bot.once('message', (r) => { if (!isOwner(r)) return; setSetting('msg1', r.text); bot.sendMessage(r.chat.id, '✅ Updated!'); });
});
bot.onText(/\/setwinback/, (msg) => {
  if (!isOwner(msg)) return;
  bot.sendMessage(msg.chat.id, `Current:\n_${getSetting('winback')}_\n\nSend new message:`, { parse_mode: 'Markdown' });
  bot.once('message', (r) => { if (!isOwner(r)) return; setSetting('winback', r.text); bot.sendMessage(r.chat.id, '✅ Updated!'); });
});
bot.onText(/\/setfollowup/, (msg) => {
  if (!isOwner(msg)) return;
  bot.sendMessage(msg.chat.id, `Current:\n_${getSetting('followup')}_\n\nSend new message (use {link}):`, { parse_mode: 'Markdown' });
  bot.once('message', (r) => { if (!isOwner(r)) return; setSetting('followup', r.text); bot.sendMessage(r.chat.id, '✅ Updated!'); });
});

// Multi-step handler
bot.on('message', (msg) => {
  if (!isOwner(msg)) return;
  const cid  = msg.chat.id;
  const text = msg.text || '';
  if (text.startsWith('/')) return;

  // ADD
  if (addState[cid]) {
    const s = addState[cid];
    if (s.step === 'name')     { s.name = text; s.step = 'phone'; return bot.sendMessage(cid, '📱 Enter phone:'); }
    if (s.step === 'phone')    { s.phone = normalizePhone(text); s.step = 'product'; return bot.sendMessage(cid, '📦 Enter product:'); }
    if (s.step === 'product')  { s.product = text; s.step = 'duration'; return bot.sendMessage(cid, '⏳ Duration in days (0 for one-time):'); }
    if (s.step === 'duration') {
      const days = parseInt(text) || 0;
      const exp  = days > 0 ? addDays(days) : null;
      db.prepare(`INSERT INTO customers (name, phone, product, product_type, amount, store_amount, duration_days, start_date, expiry_date, renewal_count) VALUES (?, ?, ?, ?, 0, 0, ?, ?, ?, 1)`)
        .run(s.name, s.phone, s.product, detectProductType(s.product), days || null, today(), exp);
      delete addState[cid];
      return bot.sendMessage(cid, `✅ Added!\n👤 ${s.name}\n📦 ${s.product}\n📅 ${exp ? formatDate(exp) : 'One-time'}`);
    }
  }

  // EDIT
  if (editState[cid]) {
    const s = editState[cid];
    if (s.step === 'phone') {
      const c = db.prepare(`SELECT * FROM customers WHERE phone = ? ORDER BY created_at DESC LIMIT 1`).get(normalizePhone(text));
      if (!c) { delete editState[cid]; return bot.sendMessage(cid, '❌ Not found.'); }
      s.customer = c; s.step = 'date';
      return bot.sendMessage(cid, `Found: *${c.name}*\nExpiry: ${c.expiry_date ? formatDate(c.expiry_date) : 'One-time'}\n\nNew date (YYYY-MM-DD):`, { parse_mode: 'Markdown' });
    }
    if (s.step === 'date') {
      db.prepare(`UPDATE customers SET expiry_date = ?, reminder_3_sent = 0, reminder_1_sent = 0 WHERE id = ?`).run(text, s.customer.id);
      delete editState[cid];
      return bot.sendMessage(cid, `✅ Updated to *${formatDate(text)}*`, { parse_mode: 'Markdown' });
    }
  }

  // DELETE
  if (deleteState[cid]) {
    const s = deleteState[cid];
    if (s.step === 'phone') {
      const c = db.prepare(`SELECT * FROM customers WHERE phone = ? ORDER BY created_at DESC LIMIT 1`).get(normalizePhone(text));
      if (!c) { delete deleteState[cid]; return bot.sendMessage(cid, '❌ Not found.'); }
      s.customer = c; s.step = 'confirm';
      return bot.sendMessage(cid, `Found: *${c.name}* | ${c.product}\nType *YES* to confirm:`, { parse_mode: 'Markdown' });
    }
    if (s.step === 'confirm') {
      if (text === 'YES') {
        db.prepare(`DELETE FROM customers WHERE id = ?`).run(s.customer.id);
        bot.sendMessage(cid, `✅ *${s.customer.name}* deleted.`, { parse_mode: 'Markdown' });
      } else {
        bot.sendMessage(cid, '❌ Cancelled.');
      }
      delete deleteState[cid];
    }
  }
});


// =============================================================
//  SCHEDULED TASKS
// =============================================================

// 9 AM — reminders
cron.schedule('0 9 * * *', async () => {
  const in3 = db.prepare(`SELECT * FROM customers WHERE expiry_date = date('now','+3 days') AND reminder_3_sent = 0`).all();
  for (const c of in3) {
    try {
      await sendSMS(c.phone, formatSMS(getSetting('msg3'), { product: c.product }));
      db.prepare('UPDATE customers SET reminder_3_sent = 1 WHERE id = ?').run(c.id);
      await sendTelegram(`📩 *Renewal SMS (3 days)*\n👤 ${c.name} | 📱 0${c.phone}\n📦 ${c.product}\n📅 ${formatDate(c.expiry_date)}`);
    } catch(e) { console.error('SMS 3d:', e.message); }
  }

  const in1 = db.prepare(`SELECT * FROM customers WHERE expiry_date = date('now','+1 day') AND reminder_1_sent = 0`).all();
  for (const c of in1) {
    try {
      await sendSMS(c.phone, formatSMS(getSetting('msg1'), { product: c.product }));
      db.prepare('UPDATE customers SET reminder_1_sent = 1 WHERE id = ?').run(c.id);
      await sendTelegram(`🚨 *Renewal SMS (1 day)*\n👤 ${c.name} | 📱 0${c.phone}\n📦 ${c.product}\n📅 TOMORROW`);
    } catch(e) { console.error('SMS 1d:', e.message); }
  }

  const winback = db.prepare(`SELECT * FROM customers WHERE expiry_date = date('now','-${config.WINBACK_DAYS_AFTER_EXPIRY} days') AND winback_sent = 0`).all();
  for (const c of winback) {
    try {
      await sendSMS(c.phone, formatSMS(getSetting('winback'), { product: c.product }));
      db.prepare('UPDATE customers SET winback_sent = 1 WHERE id = ?').run(c.id);
      await sendTelegram(`🏆 *Win-back SMS*\n👤 ${c.name} | 📱 0${c.phone}\n📦 ${c.product}`);
    } catch(e) { console.error('Winback:', e.message); }
  }

  const lost = db.prepare(`SELECT * FROM customers WHERE expiry_date = date('now','-${config.LOST_ALERT_DAYS_AFTER_EXPIRY} days') AND lost_alert_sent = 0`).all();
  for (const c of lost) {
    try {
      await sendTelegram(`⚠️ *Lost Customer!*\n👤 ${c.name} | 📱 0${c.phone}\n📦 ${c.product}\n💀 Expired ${config.LOST_ALERT_DAYS_AFTER_EXPIRY} days ago`);
      db.prepare('UPDATE customers SET lost_alert_sent = 1 WHERE id = ?').run(c.id);
    } catch(e) { console.error('Lost:', e.message); }
  }
});

// 10 PM — tomorrow's expiry preview
cron.schedule('0 22 * * *', async () => {
  try {
    const tomorrow = db.prepare(`SELECT * FROM customers WHERE expiry_date = date('now','+1 day')`).all();
    if (tomorrow.length) {
      let text = `📅 *Expiring Tomorrow (${tomorrow.length})*\n━━━━━━━━━━━━━━━━━━\n`;
      tomorrow.forEach(c => { text += `👤 ${c.name} — ${c.product}\n📱 0${c.phone}\n\n`; });
      await sendTelegram(text);
    }
  } catch(e) { console.error('Tomorrow expiry:', e.message); }
});

// 11 PM — daily summary
cron.schedule('0 23 * * *', async () => {
  try {
    const t        = db.prepare(`SELECT COALESCE(SUM(store_amount),0) AS revenue, COUNT(*) AS orders FROM customers WHERE start_date = date('now')`).get();
    const expiring = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date >= date('now') AND expiry_date <= date('now','+7 days')`).get();
    const active   = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE expiry_date >= date('now')`).get();
    await sendTelegram(
      `📊 *Daily Summary*\n━━━━━━━━━━━━━━━━━━\n` +
      `✅ New Orders: ${t.orders}\n💰 Revenue: ৳${t.revenue.toFixed(2)}\n` +
      `👥 Active: ${active.cnt}\n⚠️ Expiring This Week: ${expiring.cnt}`
    );

    const byProduct = db.prepare(`SELECT product, COUNT(*) AS cnt FROM customers WHERE expiry_date >= date('now') AND expiry_date <= date('now','+30 days') GROUP BY product ORDER BY cnt DESC`).all();
    if (byProduct.length) {
      let text = `📅 *Expiring This Month by Product*\n━━━━━━━━━━━━━━━━━━\n`;
      byProduct.forEach(r => { text += `📦 ${r.product} → ${r.cnt}\n`; });
      await sendTelegram(text);
    }
  } catch(e) { console.error('Summary:', e.message); }
});

// 1st of month — growth report
cron.schedule('0 10 1 * *', async () => {
  try {
    const thisMonth = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE start_date >= date('now','start of month')`).get();
    const lastMonth = db.prepare(`SELECT COUNT(*) AS cnt FROM customers WHERE start_date >= date('now','start of month','-1 month') AND start_date < date('now','start of month')`).get();
    const growth    = lastMonth.cnt > 0 ? Math.round(((thisMonth.cnt - lastMonth.cnt) / lastMonth.cnt) * 100) : 0;
    await sendTelegram(
      `${growth >= 0 ? '📈' : '📉'} *Monthly Growth*\n━━━━━━━━━━━━━━━━━━\n` +
      `Last Month: ${lastMonth.cnt}\nThis Month: ${thisMonth.cnt}\nGrowth: ${growth >= 0 ? '+' : ''}${growth}%`
    );
  } catch(e) { console.error('Growth:', e.message); }
});


// =============================================================
//  START
// =============================================================

app.listen(config.PORT, () => {
  console.log(`FanFlix Bot v3.0 running on port ${config.PORT}`);
  sendTelegram('🚀 *FanFlix Bot v3.0 Started!*\nAll systems ready. 💪').catch(() => {});
});
