const express = require('express');
const Imap = require('imap');
const { simpleParser } = require('mailparser');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const GMAIL_USER = process.env.GMAIL_USER;
const GMAIL_PASS = process.env.GMAIL_PASS;
const PORT = process.env.PORT || 3000;
const TG_TOKEN = process.env.TG_TOKEN || '8653224571:AAEYZfrLWtRk_U-A0t6e3sudBSibrtW2meE';
const TG_CHAT = process.env.TG_CHAT || '-1002242163455';
const ADMIN_PASS = process.env.ADMIN_PASS || '@Orsha420@';
const DATA_DIR = (() => {
  const preferred = '/app/data';
  const fallback = '/tmp/fanflix-data';
  try {
    require('fs').mkdirSync(preferred, { recursive: true });
    // test write
    require('fs').writeFileSync(preferred + '/.test', '1');
    require('fs').unlinkSync(preferred + '/.test');
    return preferred;
  } catch(e) {
    console.log('Volume not available, using fallback:', fallback);
    require('fs').mkdirSync(fallback, { recursive: true });
    return fallback;
  }
})();
const LINKS_FILE = `${DATA_DIR}/links.json`;
const ANALYTICS_FILE = `${DATA_DIR}/analytics.json`;
const IP_FILE = `${DATA_DIR}/ips.json`;
const LOGIN_VIDEO = process.env.LOGIN_VIDEO || 'https://youtu.be/PLACEHOLDER1';
const HOUSEHOLD_VIDEO = process.env.HOUSEHOLD_VIDEO || 'https://youtu.be/PLACEHOLDER2';
const SITE_URL = process.env.SITE_URL || 'https://household.fanflixbd.com';
const WA_NUMBER = '8801928382918';

// EPS Config
const EPS_MERCHANT_ID = process.env.EPS_MERCHANT_ID || '25787e85-78f5-48a8-b8ce-708673492b65';
const EPS_STORE_ID    = process.env.EPS_STORE_ID    || '05983f40-ff21-43e1-acda-a12ac7c271c1';
const EPS_USERNAME    = process.env.EPS_USERNAME    || 'mehedishimanto995@gmail.com';
const EPS_PASSWORD    = process.env.EPS_PASSWORD    || 'FaN@45FLIX';
const EPS_HASH_KEY    = process.env.EPS_HASH_KEY    || 'FMUNISHOY2lWZXDH4600FanFlix';
const EPS_API         = 'https://pgapi.eps.com.bd';

const PLANS = [
  { id:'netflix-mobile-1m', name:'Netflix Mobile 1M', price:350, days:28, product:'Netflix Subscription' },
  { id:'netflix-tv-1m',     name:'Netflix TV 1M',     price:450, days:28, product:'Netflix TV Subscription' },
  { id:'netflix-tv-3m',     name:'Netflix TV 3M',     price:1350,days:85, product:'Netflix TV Subscription 3M' },
  { id:'combo-mobile-1m',   name:'Combo Mobile 1M',   price:389, days:28, product:'Netflix+Prime Mobile 1M' },
  { id:'combo-tv-1m',       name:'Combo TV 1M',       price:489, days:28, product:'Netflix+Prime TV 1M' },
  { id:'combo-tv-3m',       name:'Combo TV 3M',       price:1500,days:85, product:'Netflix+Prime TV 3M' },
];
const MAX_SLOTS = 8;
const BLOCKED_CODES = ['2023','2024','2025','2026','2027','2028','0000'];

const FIXED_PROFILES = [
  { profile: 'Profile A', pin: '5651', slots: 2 },
  { profile: 'Profile B', pin: '5652', slots: 2 },
  { profile: 'Profile C', pin: '5653', slots: 2 },
  { profile: 'Profile D', pin: '5654', slots: 1 },
  { profile: 'Profile E', pin: '5655', slots: 1 },
];

// ── DATA ─────────────────────────────────────────────────────
function ensureDataDir() { try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch(e) {} }
function loadLinks() { try { return JSON.parse(fs.readFileSync(LINKS_FILE, 'utf8')); } catch(e) { return {}; } }
function saveLinks(links) { ensureDataDir(); fs.writeFileSync(LINKS_FILE, JSON.stringify(links, null, 2)); }
function loadAnalytics() { try { return JSON.parse(fs.readFileSync(ANALYTICS_FILE, 'utf8')); } catch(e) { return {}; } }
function saveAnalytics(data) { ensureDataDir(); fs.writeFileSync(ANALYTICS_FILE, JSON.stringify(data, null, 2)); }
function loadIPs() { try { return JSON.parse(fs.readFileSync(IP_FILE, 'utf8')); } catch(e) { return {}; } }
function saveIPs(data) { ensureDataDir(); fs.writeFileSync(IP_FILE, JSON.stringify(data, null, 2)); }
const GEO_FILE = DATA_DIR + '/geo.json';
function loadGeo() { try { return JSON.parse(fs.readFileSync(GEO_FILE, 'utf8')); } catch(e) { return {}; } }
function saveGeo(data) { ensureDataDir(); fs.writeFileSync(GEO_FILE, JSON.stringify(data, null, 2)); }

function trackAnalytics(token) {
  const data = loadAnalytics();
  if (!data[token]) data[token] = { total: 0, daily: {} };
  data[token].total += 1;
  const today = new Date().toISOString().split('T')[0];
  data[token].daily[today] = (data[token].daily[today] || 0) + 1;
  saveAnalytics(data);
}

function trackIP(token, ip) {
  if (!ip || ip === '::1' || ip === '127.0.0.1') return false;
  const data = loadIPs();
  if (!data[token]) data[token] = [];
  const isNew = !data[token].includes(ip);
  if (isNew) {
    data[token].push(ip);
    saveIPs(data);
    if (data[token].length >= 4) {
      try {
      const links = loadLinks();
      const link = links[token];
      sendTelegram(
        `⚠️ <b>Suspicious Activity!</b>\n\n` +
        `🔗 /c/${token}\n📧 ${link?.email || 'unknown'}\n👤 ${link?.profile || 'unknown'}\n` +
        `<b>${data[token].length} different IPs detected!</b>\n\nIPs:\n${data[token].map(i => `• ${i}`).join('\n')}`
      );
      } catch(e) { console.error('IP alert error:', e.message); }
    }
  }
  return data[token].length;
}

function generateToken() { return crypto.randomBytes(4).toString('hex'); }

// ── STATS ─────────────────────────────────────────────────────
let totalToday = 0, lastReset = new Date().toDateString();
const visitors = new Map();
function resetDailyIfNeeded() { const t = new Date().toDateString(); if (t !== lastReset) { totalToday = 0; lastReset = t; } }
function trackVisitor(ip) { visitors.set(ip, Date.now()); const c = Date.now()-5*60*1000; for(const[k,v] of visitors) if(v<c) visitors.delete(k); }
function getLiveVisitors() { const c = Date.now()-5*60*1000; return [...visitors.values()].filter(v=>v>c).length; }

// ── RATE LIMIT ────────────────────────────────────────────────
const rateLimitMap = new Map();
function isRateLimited(ip) {
  const now = Date.now();
  const e = rateLimitMap.get(ip) || { count:0, start:now };
  if (now - e.start > 5*60*1000) { rateLimitMap.set(ip,{count:1,start:now}); return false; }
  if (e.count >= 10) return true;
  e.count++; rateLimitMap.set(ip,e); return false;
}

// ── CACHE ─────────────────────────────────────────────────────
const cache = new Map();
function getCached(key) { const e=cache.get(key); if(e&&Date.now()-e.time<30000) return e.data; return null; }
function setCache(key, data) { cache.set(key, {data, time:Date.now()}); }

// ── TELEGRAM ──────────────────────────────────────────────────
async function sendTelegram(msg, chatId=TG_CHAT) {
  try {
    await fetch(`https://api.telegram.org/bot${TG_TOKEN}/sendMessage`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ chat_id:chatId, text:msg, parse_mode:'HTML', disable_web_page_preview:true })
    });
  } catch(e) { console.error('TG error:', e.message); }
}

// ── MORNING REPORT ────────────────────────────────────────────
function scheduleMorningReport() {
  const now = new Date();
  const bd = new Date(now.getTime() + 6*60*60*1000);
  const next11am = new Date(bd);
  next11am.setUTCHours(5,0,0,0); // 11AM BD = 5AM UTC
  if (bd.getUTCHours() >= 5) next11am.setUTCDate(next11am.getUTCDate()+1);
  const msUntil = next11am.getTime() - now.getTime();
  setTimeout(() => { sendMorningReport(); setInterval(sendMorningReport, 24*60*60*1000); }, msUntil);
}

async function sendMorningReport() {
  const links = loadLinks();
  const now = Date.now();
  const threeDays = 3*24*60*60*1000;
  const sevenDays = 7*24*60*60*1000;
  const expiring3 = Object.values(links).filter(l => l.active && l.expiresAt>now && (l.expiresAt-now)<=threeDays);
  const expiring7 = Object.values(links).filter(l => l.active && l.expiresAt>now && (l.expiresAt-now)>threeDays && (l.expiresAt-now)<=sevenDays);
  const active = Object.values(links).filter(l => l.active && l.expiresAt>now);
  const expired = Object.values(links).filter(l => l.expiresAt<=now);
  let msg = `<b>FanFlix Morning Report</b>\n📅 ${new Date().toLocaleDateString('en-BD',{timeZone:'Asia/Dhaka',weekday:'long',year:'numeric',month:'long',day:'numeric'})}\n\n`;
  msg += `Active: ${active.length} | Expiring 3d: ${expiring3.length} | Expiring 7d: ${expiring7.length} | Expired: ${expired.length}\n\n`;
  if (expiring3.length > 0) {
    msg += `<b>Expiring in 3 days — Renew now:</b>\n`;
    for (const l of expiring3) {
      const days = Math.ceil((l.expiresAt-now)/(24*60*60*1000));
      msg += `• ${l.profile} | ${l.email}\n  ${days}d | /renew ${l.token} 28\n`;
    }
    msg += '\n';
  }
  if (expiring7.length > 0) {
    msg += `<b>Expiring in 7 days:</b>\n`;
    for (const l of expiring7) {
      const days = Math.ceil((l.expiresAt-now)/(24*60*60*1000));
      msg += `• ${l.profile} | ${l.email} | ${days}d\n`;
    }
  }
  if (expiring3.length === 0 && expiring7.length === 0) msg += `All links are healthy!`;
  sendTelegram(msg);
}

// ── EXPIRY CHECKER ────────────────────────────────────────────
function checkExpiringLinks() {
  const links = loadLinks();
  const now = Date.now(), threeDays = 3*24*60*60*1000;
  for (const link of Object.values(links)) {
    if (!link.active) continue;
    const remaining = link.expiresAt - now;
    if (remaining > 0 && remaining <= threeDays && !link.warningSent) {
      const days = Math.ceil(remaining/(24*60*60*1000));
      sendTelegram(`<b>Link Expiring Soon!</b>\n\n📧 ${link.email}\n👤 ${link.profile}\n⏳ <b>${days} day(s) left</b>\n🔗 ${SITE_URL}/c/${link.token}\n\n/renew ${link.token} 28`);
      links[link.token].warningSent = true;
      saveLinks(links);
    }
  }
}
setInterval(checkExpiringLinks, 60*60*1000);
try { scheduleMorningReport(); } catch(e) { console.error('Schedule error:', e.message); }

// ── OTP SCRAPER ───────────────────────────────────────────────
async function scrapeOTP(link) {
  try {
    const res = await fetch(link, {
      headers: { 'User-Agent':'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15', 'Accept':'text/html' },
      redirect: 'follow'
    });
    const html = await res.text();
    const patterns = [/>\s*(\d{4})\s*</g, /"code"\s*:\s*"(\d{4})"/, />\s*(\d{4,6})\s*<\/(?:p|h\d|div|span)/];
    for (const pattern of patterns) {
      const match = html.match(pattern);
      if (match) { const code=(match[1]||match[0]).replace(/\D/g,''); if(code&&code.length>=4&&!BLOCKED_CODES.includes(code)) return code; }
    }
    const allMatches = [...html.matchAll(/(?<![0-9])(\d{4})(?![0-9])/g)];
    const filtered = allMatches.filter(m=>!BLOCKED_CODES.includes(m[1])&&!['1080','1920','1440'].includes(m[1]));
    if (filtered.length > 0) return filtered[0][1];
    return null;
  } catch(e) { return null; }
}

// ── IMAP ──────────────────────────────────────────────────────
function fetchNetflixEmails(filterEmail, includeSignin=false) {
  return new Promise((resolve, reject) => {
    const imap = new Imap({
      user: GMAIL_USER, password: GMAIL_PASS,
      host: 'imap.gmail.com', port: 993, tls: true,
      tlsOptions: { rejectUnauthorized: false },
      connTimeout: 10000,
      authTimeout: 8000
    });
    imap.once('ready', () => {
      imap.openBox('INBOX', true, (err) => {
        if (err) { imap.end(); return reject(err); }
        const since = new Date(Date.now() - 20*60*1000);
        imap.search([['SINCE', since], ['OR', ['FROM', 'netflix'], ['SUBJECT', 'netflix']]], (err, uids) => {
          if (err || !uids || uids.length === 0) { imap.end(); return resolve([]); }
          const fetch = imap.fetch(uids, { bodies: '' });
          const promises = [];
          fetch.on('message', (msg) => {
            const p = new Promise((res) => {
              msg.on('body', (stream) => {
                simpleParser(stream, async (err, mail) => {
                  if (err) return res(null);
                  const toValues = (mail.to?.value || []).map(a => (a.address||'').toLowerCase());
                  const toText = mail.to?.text || '';
                  const subject = (mail.subject || '').toLowerCase();
                  const bodyHtml = mail.html || '';
                  const bodyText = mail.text || '';
                  const bodyPlain = (bodyHtml || bodyText).replace(/<[^>]+>/g,' ').replace(/\s+/g,' ');
                  const ts = mail.date ? new Date(mail.date).getTime() : Date.now();
                  const toEmail = toValues[0] || toText.toLowerCase().trim();

                  if (filterEmail) {
                    const filterLower = filterEmail.toLowerCase().trim();
                    const matched = toValues.some(a => a === filterLower) || toText.toLowerCase().includes(filterLower);
                    if (!matched) return res(null);
                  }

                  const parsed = await classifyEmail({ subject, bodyHtml, bodyText, bodyPlain, toEmail, ts, includeSignin });
                  res(parsed);
                });
              });
            });
            promises.push(p);
          });
          fetch.once('end', async () => {
            const items = (await Promise.all(promises)).filter(Boolean);
            imap.end();
            resolve(items.sort((a,b) => b.ts - a.ts));
          });
          fetch.once('error', (e) => { imap.end(); reject(e); });
        });
      });
    });
    imap.once('error', (err) => { reject(err); });
    imap.connect();
  });
}

function extractLink(body) {
  const b = body.replace(/&amp;/g,'&');
  const m1 = b.match(/https:\/\/www\.netflix\.com\/account\/travel\/verify\?nftoken=[^\s"'<>\\]+/i);
  if (m1) return { link:m1[0], type:'household', label:'Temporary Access Code' };
  const m2 = b.match(/https:\/\/www\.netflix\.com\/account\/update-primary-location\?nftoken=[^\s"'<>\\]+/i);
  if (m2) return { link:m2[0], type:'update', label:'Update Household (TV)' };
  const m3 = b.match(/href=["'](https:\/\/[^"']*netflix\.com\/account[^"']*nftoken[^"']*)/i);
  if (m3) { const link=m3[1].replace(/&amp;/g,'&'); const isUpdate=link.includes('update-primary'); return { link, type:isUpdate?'update':'household', label:isUpdate?'Update Household (TV)':'Temporary Access Code' }; }
  return null;
}

async function classifyEmail({ subject, bodyHtml, bodyText, bodyPlain, toEmail, ts, includeSignin }) {
  const sl = subject.toLowerCase();

  // 6-DIGIT VERIFICATION CODE (new Netflix flow - "Verify with this code")
  // BLOCK account change codes - dangerous, customer could hijack account
  if (sl.includes('verification code') || sl.includes('your verification code')) {
    const isAccountChange = bodyPlain.toLowerCase().includes('account change') ||
                            bodyPlain.toLowerCase().includes('account info') ||
                            bodyPlain.toLowerCase().includes('change to your account');
    if (isAccountChange) return null; // Block completely
  }

  if (includeSignin && (sl.includes('verification code') || sl.includes('verify with') || sl.includes('verify this'))) {
    // Extract 6-digit code — Netflix shows it spaced like "5 0 9 6 1 9"
    // Strategy: spaced format is most reliable, then look for code appearing only once

    // Pattern 1: Spaced digits "5 0 9 6 1 9" in plain text
    const spacedMatch = bodyPlain.match(/(?<![0-9\d])(\d)\s(\d)\s(\d)\s(\d)\s(\d)\s(\d)(?![0-9\d])/);
    if (spacedMatch) {
      const code = spacedMatch[1]+spacedMatch[2]+spacedMatch[3]+spacedMatch[4]+spacedMatch[5]+spacedMatch[6];
      if (!BLOCKED_CODES.includes(code)) return { type:'verify', label:'Verification Code', code, to:toEmail, ts, expiresAt:ts+15*60*1000 };
    }

    // Pattern 2: Look for 6-digit number after "code:" or "Verify with this code" in plain text
    const afterCode = bodyPlain.match(/(?:verify with this code|this code)[^0-9]{0,30}([0-9]{6})(?![0-9])/i);
    if (afterCode && !BLOCKED_CODES.includes(afterCode[1])) {
      return { type:'verify', label:'Verification Code', code:afterCode[1], to:toEmail, ts, expiresAt:ts+15*60*1000 };
    }

    // Pattern 3: 6-digit number that appears only 1-2 times (not a template number)
    const allNums6 = [...bodyHtml.matchAll(/(?<![0-9])(\d{6})(?![0-9])/g)].map(m => m[1]);
    const filtered6 = allNums6.filter(n => !BLOCKED_CODES.includes(n));
    if (filtered6.length > 0) {
      const unique6 = [...new Set(filtered6)];
      // Pick number that appears only once — that's the real code
      const onlyOnce = unique6.filter(n => allNums6.filter(x => x === n).length === 1);
      const verifyCode = onlyOnce[onlyOnce.length - 1] || unique6[unique6.length - 1];
      if (verifyCode) return { type:'verify', label:'Verification Code', code:verifyCode, to:toEmail, ts, expiresAt:ts+15*60*1000 };
    }
  }

  // 4-DIGIT SIGN-IN CODE (existing flow)
  if (includeSignin && (sl.includes('sign-in code') || sl.includes('sign in code'))) {
    // Netflix template contains 8199 hundreds of times in CSS
    // Real code appears rarely at the end — find numbers appearing <= 5 times
    const TEMPLATE_NUMS = [...BLOCKED_CODES, '8199'];
    const allNums = [...bodyHtml.matchAll(/(?<![0-9])(\d{4})(?![0-9])/g)].map(m => m[1]);
    const filtered = allNums.filter(n => !TEMPLATE_NUMS.includes(n));
    if (filtered.length > 0) {
      const unique = [...new Set(filtered)];
      const singleOccurrence = unique.filter(n => allNums.filter(x => x === n).length <= 5);
      const signinCode = singleOccurrence[singleOccurrence.length - 1] || unique[unique.length - 1];
      if (signinCode) return { type:'signin', label:'Sign-in Code', code:signinCode, to:toEmail, ts, expiresAt:ts+15*60*1000 };
    }
  }

  const isRelevant = sl.includes('temporary')||sl.includes('access code')||sl.includes('travel')||sl.includes('household')||sl.includes('update')||sl.includes('verify');
  if (!isRelevant) return null;
  const result = extractLink(bodyHtml) || extractLink(bodyText);
  if (!result) return null;
  if (result.type === 'household') {
    const otp = await scrapeOTP(result.link);
    if (otp && !BLOCKED_CODES.includes(otp)) return { type:'household', label:'Temporary Access Code', code:otp, to:toEmail, ts, expiresAt:ts+15*60*1000 };
    return { ...result, to:toEmail, ts, expiresAt:ts+15*60*1000 };
  }
  return { ...result, to:toEmail, ts };
}

// ── EPS HELPERS ──────────────────────────────────────────────
function epsHash(data) {
  const key = Buffer.from(EPS_HASH_KEY, 'utf8');
  return crypto.createHmac('sha512', key).update(data).digest('base64');
}

async function epsGetToken() {
  console.log('EPS GetToken - username:', EPS_USERNAME);
  const xhash = epsHash(EPS_USERNAME);
  console.log('EPS xhash:', xhash.substring(0,20)+'...');
  const res = await fetch(EPS_API + '/v1/Auth/GetToken', {
    method: 'POST',
    headers: { 'Content-Type':'application/json', 'x-hash': xhash },
    body: JSON.stringify({ userName: EPS_USERNAME, password: EPS_PASSWORD })
  });
  const d = await res.json();
  console.log('EPS GetToken response:', JSON.stringify(d).substring(0,100));
  if (!d.token) throw new Error('EPS auth failed: ' + (d.errorMessage || JSON.stringify(d)));
  return d.token;
}

async function epsInitPayment({ token, amount, productName, customerName, customerPhone, customerEmail, txnId, orderId, successUrl, failUrl, cancelUrl }) {
  const bearerToken = await epsGetToken();
  const xhash = epsHash(txnId);
  const body = {
    merchantId: EPS_MERCHANT_ID,
    storeId: EPS_STORE_ID,
    CustomerOrderId: orderId,
    merchantTransactionId: txnId,
    transactionTypeId: 1,
    financialEntityId: 0,
    transitionStatusId: 0,
    totalAmount: amount,
    ipAddress: '127.0.0.1',
    version: '1',
    successUrl, failUrl, cancelUrl,
    customerName: customerName || 'Customer',
    customerEmail: customerEmail || 'customer@fanflixbd.com',
    CustomerAddress: 'Dhaka, Bangladesh',
    CustomerAddress2: '',
    CustomerCity: 'Dhaka',
    CustomerState: 'Dhaka',
    CustomerPostcode: '1000',
    CustomerCountry: 'BD',
    CustomerPhone: customerPhone || '01700000000',
    ShippingMethod: 'NO',
    NoOfItem: '1',
    ProductName: productName,
    ProductProfile: 'digital-goods',
    ProductCategory: 'Subscription',
    ValueA: token, // store fanflix token for callback
  };
  const res = await fetch(EPS_API + '/v1/EPSEngine/InitializeEPS', {
    method: 'POST',
    headers: { 'Content-Type':'application/json', 'x-hash': xhash, 'Authorization': 'Bearer ' + bearerToken },
    body: JSON.stringify(body)
  });
  const d = await res.json();
  if (!d.RedirectURL) throw new Error('EPS init failed: ' + (d.ErrorMessage || 'Unknown'));
  return d;
}

async function epsVerifyPayment(txnId) {
  const bearerToken = await epsGetToken();
  const xhash = epsHash(txnId);
  const res = await fetch(EPS_API + '/v1/EPSEngine/CheckMerchantTransactionStatus?merchantTransactionId=' + txnId, {
    headers: { 'x-hash': xhash, 'Authorization': 'Bearer ' + bearerToken }
  });
  return await res.json();
}

// ── ADMIN AUTH ────────────────────────────────────────────────
function adminAuth(req, res, next) {
  if (req.headers['x-admin-token'] !== ADMIN_PASS) return res.status(401).json({ error:'Unauthorized' });
  next();
}

// ── TELEGRAM WEBHOOK ──────────────────────────────────────────
app.post('/tg-webhook', async (req, res) => {
  res.sendStatus(200);
  const msg = req.body?.message;
  if (!msg?.text) return;
  const text = msg.text.trim();
  const chatId = msg.chat.id;

  if (text.startsWith('/create')) {
    const parts = text.replace('/create','').trim().split('|').map(s=>s.trim());
    const emailRaw = parts[0];
    if (!emailRaw||!emailRaw.includes('@')) return sendTelegram('❌ Format: /create email@gmail.com\nOptional: /create email@gmail.com | 85', chatId);
    const email = emailRaw.toLowerCase();
    const days = parts[1] ? parseInt(parts[1]) : 28;
    const links = loadLinks();
    const now = Date.now();
    const existing = Object.values(links).filter(l => l.email===email && l.active && l.expiresAt>now);
    if (existing.length >= MAX_SLOTS) return sendTelegram(`❌ Account Full! ${email} has ${MAX_SLOTS}/${MAX_SLOTS} active links.\nUse /list ${email}`, chatId);
    const created = [];
    for (const prof of FIXED_PROFILES) {
      for (let i=0; i<prof.slots; i++) {
        const token = generateToken();
        links[token] = { token, email, profile:prof.profile, pin:prof.pin, days, createdAt:now, expiresAt:now+days*24*60*60*1000, uses:0, lastUsed:null, active:true, warningSent:false };
        created.push({ token, profile:prof.profile, pin:prof.pin, link:`${SITE_URL}/c/${token}` });
      }
    }
    saveLinks(links);
    let msg2 = `✅ <b>8 Links Created!</b>\n📧 ${email}\n⏳ ${days} days\n\n`;
    let lastProf = '';
    for (const l of created) {
      if (l.profile !== lastProf) { msg2 += `\n👤 <b>${l.profile}</b> | PIN: <code>${l.pin}</code>\n`; lastProf = l.profile; }
      msg2 += `🔗 <code>${l.link}</code>\n`;
    }
    msg2 += `\n━━━━━━━━━━━━━━━━━━\n📋 <b>Template:</b>\n\n` + buildCustomerMessage(email,'[PROFILE]','[PIN]','[LINK]',days);
    return sendTelegram(msg2, chatId);
  }

  if (text.startsWith('/replaceall')) {
    const parts = text.replace('/replaceall','').trim().split(' ');
    if (parts.length < 2) return sendTelegram('❌ Format: /replaceall oldemail newemail', chatId);
    const [oldEmail, newEmail] = parts;
    const links = loadLinks();
    let count = 0;
    for (const token of Object.keys(links)) {
      if (links[token].email === oldEmail.toLowerCase()) { links[token].email = newEmail.toLowerCase(); count++; }
    }
    if (count === 0) return sendTelegram(`❌ No links found for ${oldEmail}`, chatId);
    saveLinks(links);
    cache.delete(oldEmail.toLowerCase());
    cache.delete(newEmail.toLowerCase());
    return sendTelegram(`✅ <b>Account Replaced!</b>\n\n📧 Old: ${oldEmail}\n📧 New: ${newEmail}\n🔗 ${count} links updated\n\nAll customer links now fetch from new account!`, chatId);
  }

  if (text.startsWith('/replace')) {
    const parts = text.replace('/replace','').trim().split(' ');
    if (parts.length < 2) return sendTelegram('❌ Format: /replace TOKEN newemail@gmail.com', chatId);
    const [token, newEmail] = parts;
    const links = loadLinks();
    if (!links[token]) return sendTelegram('❌ Link not found: '+token, chatId);
    const oldEmail = links[token].email;
    links[token].email = newEmail.toLowerCase();
    saveLinks(links);
    cache.delete(oldEmail); cache.delete(newEmail.toLowerCase());
    return sendTelegram(`✅ <b>Link Updated!</b>\n\n🔗 /c/${token}\n👤 ${links[token].profile}\n📧 Old: ${oldEmail}\n📧 New: ${newEmail}`, chatId);
  }

  if (text.startsWith('/list')) {
    const emailFilter = text.replace('/list','').trim().toLowerCase();
    const links = loadLinks();
    const now = Date.now();
    const filtered = Object.values(links).filter(l => !emailFilter || l.email.includes(emailFilter));
    if (!filtered.length) return sendTelegram(`No links found`, chatId);
    let msg2 = `📋 <b>Links</b>\n\n`;
    for (const l of filtered.sort((a,b)=>b.createdAt-a.createdAt)) {
      const daysLeft = Math.ceil((l.expiresAt-now)/(24*60*60*1000));
      const status = !l.active?'🚫':daysLeft<=0?'⏰':daysLeft<=3?'⚠️':'✅';
      msg2 += `${status} ${l.profile} | PIN: ${l.pin}\n🔗 /c/${l.token}\n⏳ ${daysLeft}d | Uses: ${l.uses}\n\n`;
    }
    return sendTelegram(msg2, chatId);
  }

  if (text.startsWith('/renew')) {
    const parts = text.replace('/renew','').trim().split(' ');
    const token = parts[0]; const days = parseInt(parts[1])||28;
    const links = loadLinks();
    if (!links[token]) return sendTelegram('❌ Link not found', chatId);
    links[token].expiresAt = Date.now()+days*24*60*60*1000;
    links[token].warningSent = false; links[token].active = true;
    saveLinks(links);
    return sendTelegram(`✅ Renewed /c/${token} for ${days} days`, chatId);
  }

  if (text.startsWith('/extend')) {
    const parts = text.replace('/extend','').trim().split(' ');
    if (parts.length < 2) return sendTelegram('❌ Format: /extend TOKEN days', chatId);
    const [token, daysStr] = parts; const days = parseInt(daysStr)||28;
    const links = loadLinks();
    if (!links[token]) return sendTelegram('❌ Link not found', chatId);
    links[token].expiresAt += days*24*60*60*1000;
    links[token].warningSent = false;
    saveLinks(links);
    return sendTelegram(`✅ Extended /c/${token} by ${days} days`, chatId);
  }

  if (text.startsWith('/revoke')) {
    const token = text.replace('/revoke','').trim();
    const links = loadLinks();
    if (!links[token]) return sendTelegram('❌ Link not found', chatId);
    links[token].active = false; saveLinks(links);
    return sendTelegram(`✅ Revoked /c/${token}`, chatId);
  }

  if (text.startsWith('/ip')) {
    const token = text.replace('/ip','').trim();
    const ips = loadIPs();
    if (!ips[token]) return sendTelegram(`No IPs recorded for /c/${token}`, chatId);
    return sendTelegram(`<b>IPs for /c/${token}</b>\n\n${ips[token].map((ip,i)=>`${i+1}. ${ip}`).join('\n')}\n\nTotal: ${ips[token].length} unique IPs`, chatId);
  }

  if (text.startsWith('/expiry')) {
    const links = loadLinks();
    const now = Date.now();
    const sevenDays = 7*24*60*60*1000;
    const expiring = Object.values(links).filter(l => l.active && l.expiresAt>now && (l.expiresAt-now)<=sevenDays).sort((a,b)=>a.expiresAt-b.expiresAt);
    if (!expiring.length) return sendTelegram('No links expiring this week!', chatId);
    let msg2 = `📅 <b>Expiring This Week</b>\n\n`;
    for (const l of expiring) {
      const days = Math.ceil((l.expiresAt-now)/(24*60*60*1000));
      msg2 += `${days}d | ${l.profile} | ${l.email}\n/renew ${l.token} 28\n\n`;
    }
    return sendTelegram(msg2, chatId);
  }

  if (text === '/slots') {
    const links = loadLinks(); const now = Date.now();
    const byEmail = {};
    for (const l of Object.values(links)) {
      if (!byEmail[l.email]) byEmail[l.email] = { active:0, total:0 };
      byEmail[l.email].total++;
      if (l.active && l.expiresAt>now) byEmail[l.email].active++;
    }
    let msg2 = '📊 <b>Slot Usage</b>\n\n';
    for (const [email, info] of Object.entries(byEmail)) {
      const bar = '█'.repeat(info.active)+'░'.repeat(Math.max(0,MAX_SLOTS-info.active));
      msg2 += `📧 ${email}\n${bar} ${info.active}/${MAX_SLOTS}\n\n`;
    }
    return sendTelegram(msg2||'No active links.', chatId);
  }

  if (text === '/stats') {
    const links = loadLinks(); const now = Date.now();
    const active = Object.values(links).filter(l=>l.active&&l.expiresAt>now).length;
    const expired = Object.values(links).filter(l=>l.expiresAt<=now).length;
    const totalUses = Object.values(links).reduce((s,l)=>s+l.uses,0);
    return sendTelegram(`📊 <b>FanFlix Stats</b>\n\nActive: ${active}\nExpired: ${expired}\n👁 Total uses: ${totalUses}\n👥 Live: ${getLiveVisitors()}\n📈 Today: ${totalToday}`, chatId);
  }

  if (text === '/help' || text === '/start') {
    return sendTelegram(
      `🎬 <b>FanFlix Bot Commands</b>\n\n` +
      `<b>Create:</b>\n/create email | days\n\n` +
      `<b>Replace Account:</b>\n/replace TOKEN newemail\n/replaceall oldemail newemail\n\n` +
      `<b>Manage:</b>\n/list email\n/renew TOKEN days\n/extend TOKEN days\n/revoke TOKEN\n\n` +
      `<b>Info:</b>\n/slots\n/stats\n/expiry\n/ip TOKEN\n/help`, chatId
    );
  }
});

function buildCustomerMessage(email, profile, pin, link, days) {
  return `🎬 <b>FanFlix BD</b>\n\n📧 Email: <code>${email}</code>\n👤 Profile: ${profile}\n🔑 PIN: ${pin}\n\n🔗 Your Code Link:\n${link}\n\n📺 Login Tutorial:\n${LOGIN_VIDEO}\n\n🏠 Household Fix:\n${HOUSEHOLD_VIDEO}\n\n⚠️ Important:\n• No account changes allowed\n• 1 device at a time\n• BD use only\n• Sign in anytime if logged out\n\n✅ Valid for ${days} days`;
}

// ── ADMIN ROUTES ──────────────────────────────────────────────
app.post('/api/admin/login', (req, res) => {
  const { password } = req.body;
  if (password === ADMIN_PASS) res.json({ success:true, token:ADMIN_PASS });
  else res.status(401).json({ success:false, error:'Wrong password' });
});

app.get('/api/admin/links', adminAuth, (req, res) => {
  const links = loadLinks();
  const analytics = loadAnalytics();
  const ips = loadIPs();
  for (const token of Object.keys(links)) {
    links[token].analytics = analytics[token] || { total:0, daily:{} };
    links[token].ipCount = (ips[token] || []).length;
    links[token].ips = ips[token] || [];
  }
  res.json({ success:true, links });
});

app.post('/api/admin/create', adminAuth, (req, res) => {
  const { email, profile, pin, days } = req.body;
  if (!email||!profile||!pin||!days) return res.status(400).json({ error:'Missing fields' });
  const links = loadLinks();
  const now = Date.now();
  const existing = Object.values(links).find(l => l.email===email.toLowerCase()&&l.profile===profile&&l.active&&l.expiresAt>now);
  if (existing) return res.json({ success:true, token:existing.token, link:`/c/${existing.token}`, existing:true });
  const activeCount = Object.values(links).filter(l => l.email===email.toLowerCase()&&l.active&&l.expiresAt>now).length;
  if (activeCount >= MAX_SLOTS) return res.status(400).json({ error:`Account full (${MAX_SLOTS}/${MAX_SLOTS})` });
  const token = generateToken();
  links[token] = { token, email:email.toLowerCase(), profile, pin, days:parseInt(days), createdAt:now, expiresAt:now+parseInt(days)*24*60*60*1000, uses:0, lastUsed:null, active:true, warningSent:false };
  saveLinks(links);
  res.json({ success:true, token, link:`/c/${token}` });
});

app.post('/api/admin/revoke/:token', adminAuth, (req, res) => {
  const links = loadLinks();
  if (!links[req.params.token]) return res.status(404).json({ error:'Not found' });
  links[req.params.token].active = false; saveLinks(links); res.json({ success:true });
});

app.post('/api/admin/activate/:token', adminAuth, (req, res) => {
  const links = loadLinks();
  if (!links[req.params.token]) return res.status(404).json({ error:'Not found' });
  links[req.params.token].active = true; saveLinks(links); res.json({ success:true });
});

app.post('/api/admin/extend/:token', adminAuth, (req, res) => {
  const links = loadLinks();
  if (!links[req.params.token]) return res.status(404).json({ error:'Not found' });
  const { days } = req.body;
  links[req.params.token].expiresAt += parseInt(days)*24*60*60*1000;
  links[req.params.token].warningSent = false;
  saveLinks(links); res.json({ success:true });
});

app.post('/api/admin/renew/:token', adminAuth, (req, res) => {
  const links = loadLinks();
  if (!links[req.params.token]) return res.status(404).json({ error:'Not found' });
  const { days } = req.body; const d = parseInt(days)||28;
  links[req.params.token].expiresAt = Date.now()+d*24*60*60*1000;
  links[req.params.token].warningSent = false;
  links[req.params.token].active = true;
  saveLinks(links); res.json({ success:true });
});

// Replace account email for single link
app.post('/api/admin/replace/:token', adminAuth, (req, res) => {
  const links = loadLinks();
  if (!links[req.params.token]) return res.status(404).json({ error:'Not found' });
  const { newEmail } = req.body;
  if (!newEmail) return res.status(400).json({ error:'Missing newEmail' });
  const oldEmail = links[req.params.token].email;
  links[req.params.token].email = newEmail.toLowerCase().trim();
  saveLinks(links);
  cache.clear(); // Clear all cache to ensure fresh data
  res.json({ success:true, oldEmail, newEmail });
});

// Replace all links for an email
app.post('/api/admin/replaceall', adminAuth, (req, res) => {
  const { oldEmail, newEmail } = req.body;
  if (!oldEmail||!newEmail) return res.status(400).json({ error:'Missing fields' });
  const links = loadLinks();
  let count = 0;
  for (const token of Object.keys(links)) {
    if (links[token].email === oldEmail.toLowerCase()) { links[token].email = newEmail.toLowerCase(); count++; }
  }
  saveLinks(links);
  cache.clear(); // Clear all cache
  res.json({ success:true, count });
});

app.delete('/api/admin/delete/:token', adminAuth, (req, res) => {
  const links = loadLinks();
  if (!links[req.params.token]) return res.status(404).json({ error:'Not found' });
  delete links[req.params.token]; saveLinks(links); res.json({ success:true });
});

app.get('/api/admin/slots', adminAuth, (req, res) => {
  const links = loadLinks(); const now = Date.now();
  const byEmail = {};
  for (const l of Object.values(links)) {
    if (!byEmail[l.email]) byEmail[l.email] = { active:0, total:0 };
    byEmail[l.email].total++;
    if (l.active && l.expiresAt>now) byEmail[l.email].active++;
  }
  res.json({ success:true, slots:byEmail, maxSlots:MAX_SLOTS });
});

// ── CUSTOMER LINK ─────────────────────────────────────────────

// Fast endpoint - returns link info instantly without IMAP
app.get('/api/link/:token/info', (req, res) => {
  const links = loadLinks();
  const link = links[req.params.token];
  if (!link) return res.status(404).json({ success:false, error:'invalid', message:'Invalid link.' });
  if (!link.active) return res.status(403).json({ success:false, error:'revoked', message:'Access revoked. Contact FanFlix BD.' });
  const now = Date.now();
  const daysLeft = Math.ceil((link.expiresAt-now)/(24*60*60*1000));
  const totalDays = link.days || 28;
  if (now > link.expiresAt) return res.status(403).json({ success:false, error:'expired', message:'Subscription expired!', daysLeft:0, expiresAt:link.expiresAt, profile:link.profile, token:req.params.token });
  res.json({ success:true, profile:link.profile, pin:link.pin, email:link.email, daysLeft, totalDays });
});

app.get('/api/link/:token', async (req, res) => {
  // Overall timeout - respond within 15 seconds
  const timeout = setTimeout(() => {
    if (!res.headersSent) res.status(504).json({ success:false, error:'timeout', message:'Request timed out. Please refresh.' });
  }, 15000);
  const origJson = res.json.bind(res);
  res.json = (data) => { clearTimeout(timeout); return origJson(data); };
  const links = loadLinks();
  const link = links[req.params.token];
  if (!link) return res.status(404).json({ success:false, error:'invalid', message:'Invalid link.' });
  if (!link.active) return res.status(403).json({ success:false, error:'revoked', message:'Access revoked. Contact FanFlix BD.' });
  const now = Date.now();
  const daysLeft = Math.ceil((link.expiresAt-now)/(24*60*60*1000));
  const totalDays = link.days || 28;
  if (now > link.expiresAt) return res.status(403).json({ success:false, error:'expired', message:'Subscription expired!', daysLeft:0, expiresAt:link.expiresAt, profile:link.profile, token:req.params.token });
  link.uses += 1; link.lastUsed = now; saveLinks(links);
  trackAnalytics(req.params.token);
  const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket.remoteAddress;
  trackVisitor(ip);
  const ipCount = trackIP(req.params.token, ip);
  try {
    const codes = await fetchNetflixEmails(link.email, true);
    if (codes.length > 0) totalToday += 1;
    res.json({ success:true, codes, count:codes.length, profile:link.profile, pin:link.pin, email:link.email, daysLeft, totalDays, ipCount, uses:link.uses });
  } catch(err) {
    res.status(500).json({ success:false, error:'server', message:'Server error. Try again.' });
  }
});

// ── DEBUG ─────────────────────────────────────────────────────
app.get('/api/debug-email', async (req, res) => {
  const filterEmail = (req.query.email || '').trim().toLowerCase();
  try {
    const results = await new Promise((resolve, reject) => {
      const imap = new Imap({ user:GMAIL_USER, password:GMAIL_PASS, host:'imap.gmail.com', port:993, tls:true, tlsOptions:{rejectUnauthorized:false}, connTimeout:10000, authTimeout:8000 });
      imap.once('ready', () => {
        imap.openBox('INBOX', true, (err) => {
          if (err) { imap.end(); return reject(err); }
          const since = new Date(Date.now() - 20*60*1000);
          imap.search([['SINCE', since], ['OR', ['FROM', 'netflix'], ['SUBJECT', 'netflix']]], (err, uids) => {
            if (err || !uids || uids.length === 0) { imap.end(); return resolve([]); }
            const fetch = imap.fetch(uids, { bodies: '' });
            const promises = [];
            fetch.on('message', (msg) => {
              const p = new Promise((res2) => {
                msg.on('body', (stream) => {
                  simpleParser(stream, (err, mail) => {
                    if (err) return res2(null);
                    const toValues = (mail.to?.value || []).map(a => a.address?.toLowerCase());
                    res2({ subject:mail.subject, to:mail.to?.text, to_parsed:toValues, matches_filter: filterEmail ? toValues.some(a=>a===filterEmail) : true });
                  });
                });
              });
              promises.push(p);
            });
            fetch.once('end', async () => { const items=(await Promise.all(promises)).filter(Boolean); imap.end(); resolve(items); });
            fetch.once('error', (e) => { imap.end(); reject(e); });
          });
        });
      });
      imap.once('error', reject);
      imap.connect();
    });
    res.json({ success:true, filter:filterEmail, count:results.length, emails:results });
  } catch(err) { res.status(500).json({ success:false, error:err.message }); }
});

// ── PUBLIC ROUTES ─────────────────────────────────────────────
app.get('/api/stats', (req, res) => {
  const ip = req.headers['x-forwarded-for']?.split(',')[0] || req.socket.remoteAddress;
  trackVisitor(ip); resetDailyIfNeeded();
  res.json({ live:getLiveVisitors(), today:totalToday });
});

// Geo flags endpoint for admin
app.get('/api/admin/geo', (req, res) => {
  try {
    if (req.headers['x-admin-token'] !== ADMIN_PASS) return res.status(401).json({ error:'Unauthorized' });
    const geoData = loadGeo();
    res.json({ success: true, geo: geoData });
  } catch(e) {
    res.json({ success: true, geo: {} });
  }
});

app.get('/api/health', (req, res) => {
  res.json({ ok:true, user:GMAIL_USER?GMAIL_USER.replace(/(.{3}).*(@.*)/,'$1***$2'):'NOT SET' });
});

app.get('/api/codes', async (req, res) => {
  const email = (req.query.email||'').trim();
  const ip = req.headers['x-forwarded-for']?.split(',')[0] || req.socket.remoteAddress;
  trackVisitor(ip); resetDailyIfNeeded();
  if (isRateLimited(ip)) return res.status(429).json({ success:false, error:'Too many requests. Wait 5 minutes.' });
  const cached = getCached(email);
  if (cached) return res.json({ success:true, codes:cached, count:cached.length, cached:true, fetchTime:0 });
  const start = Date.now();
  try {
    const codes = await fetchNetflixEmails(email, false);
    const fetchTime = ((Date.now()-start)/1000).toFixed(1);
    setCache(email, codes);
    if (codes.length > 0) totalToday += 1;
    res.json({ success:true, codes, count:codes.length, fetchTime });
  } catch(err) { res.status(500).json({ success:false, error:err.message }); }
});

app.get('/admin', (req, res) => res.sendFile(path.join(__dirname,'public','admin.html')));
app.get('/c/:token', (req, res) => res.sendFile(path.join(__dirname,'public','customer.html')));
app.get('*', (req, res) => res.sendFile(path.join(__dirname,'public','index.html')));

// Prevent crashes from uncaught errors
process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception:', err.message);
  console.error(err.stack);
});
process.on('unhandledRejection', (reason) => {
  console.error('Unhandled Rejection:', reason);
});

ensureDataDir();
app.listen(PORT, () => {
  console.log(`FanFlix running on port ${PORT}`);
  try { sendTelegram('<b>FanFlix Started</b>\nType /help for commands'); } catch(e) { console.error('TG startup error:', e.message); }
});
