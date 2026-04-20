const express = require('express');
const crypto = require('crypto');
const axios = require('axios');

const app = express();
app.use(express.json());

// ── Config from environment variables ──────────────────────
const EPS_SECRET_KEY    = process.env.EPS_SECRET_KEY;
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID  = process.env.TELEGRAM_CHAT_ID;

// ── Decrypt EPS AES-256-CBC payload ────────────────────────
function decryptEPS(data) {
    const [ivBase64, cipherBase64] = data.split(':');
    const iv         = Buffer.from(ivBase64, 'base64');
    const cipherText = Buffer.from(cipherBase64, 'base64');

    // Build 32-byte key from secret key
    const key = Buffer.alloc(32);
    Buffer.from(EPS_SECRET_KEY, 'utf8').copy(key);

    const decipher = crypto.createDecipheriv('aes-256-cbc', key, iv);
    const decrypted = Buffer.concat([decipher.update(cipherText), decipher.final()]);
    return JSON.parse(decrypted.toString('utf8'));
}

// ── Send Telegram message ───────────────────────────────────
async function sendTelegram(text) {
    await axios.post(
        `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
        { chat_id: TELEGRAM_CHAT_ID, text, parse_mode: 'HTML' }
    );
}

// ── IPN Endpoint ────────────────────────────────────────────
app.post('/eps-ipn', async (req, res) => {
    try {
        const { Data } = req.body;
        if (!Data) return res.status(400).json({ status: 'ERROR', message: 'No data' });

        const p = decryptEPS(Data);

        const icon = p.status === 'SUCCESS' ? '✅' : '❌';

        const msg = `
${icon} <b>New Payment — FanFlix</b>
━━━━━━━━━━━━━━━━━━
👤 <b>Name:</b> ${p.customer_name || 'N/A'}
📱 <b>Phone:</b> ${p.customer_phone || 'N/A'}
📧 <b>Email:</b> ${p.customer_email || 'N/A'}
━━━━━━━━━━━━━━━━━━
💰 <b>Amount:</b> ৳${p.amount}
🏪 <b>Store Amount:</b> ৳${p.store_amount}
💳 <b>Method:</b> ${p.payment_method || 'N/A'}
📋 <b>Status:</b> ${p.status}
━━━━━━━━━━━━━━━━━━
🆔 <b>EPS TXN:</b> ${p.transaction_id}
🔖 <b>Order ID:</b> ${p.merchant_transaction_id}
🕐 <b>Time:</b> ${p.timestamp}
`.trim();

        await sendTelegram(msg);

        console.log(`[IPN] ${p.status} | ৳${p.amount} | ${p.customer_name}`);
        res.json({ status: 'OK', message: 'IPN received and saved successfully' });

    } catch (err) {
        console.error('[IPN Error]', err.message);
        res.status(500).json({ status: 'ERROR', message: 'Decryption failed or internal error' });
    }
});

// ── Health check ────────────────────────────────────────────
app.get('/', (req, res) => {
    res.send('✅ FanFlix EPS IPN Server is Running');
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
