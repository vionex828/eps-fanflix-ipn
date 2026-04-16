const express = require('express');
const crypto = require('crypto');

const app = express();
app.use(express.json());

// ─── CONFIG (Railway environment variables থেকে আসবে) ───
const EPS_SECRET_KEY = process.env.FMUNISHOY2lWZXDH4600FanFlix;   // EPS dashboard থেকে
const TELEGRAM_BOT_TOKEN = process.env.8677029895:AAGMnkomWY0K2y51WyaZBNRzodcEJbn6IE8;
const TELEGRAM_CHAT_ID = process.env.-1002242163455;
const PORT = process.env.PORT || 3000;

// ─── Duplicate prevention ───
const processedTransactions = new Set();

// ─── AES-256-CBC Decrypt ───
function decryptPayload(data) {
  const [ivBase64, cipherBase64] = data.split(':');
  const iv = Buffer.from(ivBase64, 'base64');
  const cipher = Buffer.from(cipherBase64, 'base64');

  // EPS secret key — 32 bytes হতে হবে
  const key = Buffer.from(EPS_SECRET_KEY.padEnd(32, '0').slice(0, 32), 'utf8');

  const decipher = crypto.createDecipheriv('aes-256-cbc', key, iv);
  decipher.setAutoPadding(true); // PKCS7
  let decrypted = decipher.update(cipher);
  decrypted = Buffer.concat([decrypted, decipher.final()]);
  return JSON.parse(decrypted.toString('utf8'));
}

// ─── Telegram Message পাঠানো ───
async function sendTelegram(message) {
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      chat_id: TELEGRAM_CHAT_ID,
      text: message,
      parse_mode: 'HTML',
    }),
  });
  const json = await res.json();
  if (!json.ok) console.error('Telegram error:', json);
}

// ─── Notification Message Format ───
function buildMessage(p) {
  const statusEmoji = p.status === 'SUCCESS' ? '✅' : '❌';
  return `
${statusEmoji} <b>নতুন পেমেন্ট ${p.status === 'SUCCESS' ? 'সফল' : 'ব্যর্থ'}</b>

🔖 <b>Transaction ID:</b> <code>${p.transaction_id || 'N/A'}</code>
🛒 <b>Order ID:</b> <code>${p.merchant_transaction_id || 'N/A'}</code>
💰 <b>Amount:</b> ৳${parseFloat(p.amount || 0).toFixed(2)}
📱 <b>Payment Method:</b> ${p.payment_method || 'N/A'}
👤 <b>Customer:</b> ${p.customer_name || 'N/A'}
📞 <b>Phone:</b> ${p.customer_phone || 'N/A'}
🕐 <b>Time:</b> ${p.timestamp ? new Date(p.timestamp).toLocaleString('bn-BD', { timeZone: 'Asia/Dhaka' }) : new Date().toLocaleString('bn-BD', { timeZone: 'Asia/Dhaka' })}
`.trim();
}

// ─── IPN Endpoint ───
app.post('/ipn', async (req, res) => {
  try {
    const { Data } = req.body;

    if (!Data) {
      return res.status(400).json({ status: 'ERROR', message: 'No data received' });
    }

    // Decrypt
    let payload;
    try {
      payload = decryptPayload(Data);
    } catch (err) {
      console.error('Decryption failed:', err.message);
      return res.status(400).json({ status: 'ERROR', message: 'Decryption failed' });
    }

    console.log('IPN received:', JSON.stringify(payload, null, 2));

    // Duplicate check
    const txId = payload.transaction_id;
    if (txId && processedTransactions.has(txId)) {
      console.log('Duplicate transaction, skipping:', txId);
      return res.json({ status: 'OK', message: 'Already processed' });
    }
    if (txId) processedTransactions.add(txId);

    // Telegram notification পাঠাও
    const message = buildMessage(payload);
    await sendTelegram(message);

    return res.json({ status: 'OK', message: 'IPN received and saved successfully' });

  } catch (err) {
    console.error('IPN handler error:', err);
    return res.status(500).json({ status: 'ERROR', message: 'Internal server error' });
  }
});

// ─── Health check ───
app.get('/', (req, res) => {
  res.send('EPS IPN Bot is running ✅');
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
