const express = require('express');
const crypto = require('crypto');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const EPS_SECRET_KEY = process.env.EPS_SECRET_KEY;
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const processedTransactions = new Set();

function decryptPayload(data) {
  try {
    const [ivBase64, cipherBase64] = data.split(':');
    const iv = Buffer.from(ivBase64, 'base64');
    const cipher = Buffer.from(cipherBase64, 'base64');

    const key = crypto.createHash('sha256').update(EPS_SECRET_KEY).digest();
    const decipher = crypto.createDecipheriv('aes-256-cbc', key, iv);
    decipher.setAutoPadding(true);

    let decrypted = decipher.update(cipher);
    decrypted = Buffer.concat([decrypted, decipher.final()]);

    return JSON.parse(decrypted.toString('utf8'));
  } catch (error) {
    console.error('Decrypt failed:', error.message);
    return null;
  }
}

async function sendTelegramMessage(text) {
  const response = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      chat_id: TELEGRAM_CHAT_ID,
      text
    })
  });

  const result = await response.json();
  console.log('Telegram response:', result);
  return result;
}

app.get('/', (req, res) => {
  res.send('EPS Telegram Bot is running');
});

app.post('/ipn', async (req, res) => {
  console.log('IPN received:', JSON.stringify(req.body));

  try {
    if (!req.body || !req.body.Data) {
      await sendTelegramMessage(`⚠️ IPN hit but no Data field found.\n\nBody: ${JSON.stringify(req.body)}`);
      return res.status(400).json({ error: 'Missing Data field' });
    }

    const decrypted = decryptPayload(req.body.Data);

    if (!decrypted) {
      await sendTelegramMessage(`⚠️ IPN received but decrypt failed.\n\nRaw Body: ${JSON.stringify(req.body)}`);
      return res.status(400).json({ error: 'Decrypt failed' });
    }

    const transactionId =
      decrypted.trx_id ||
      decrypted.transaction_id ||
      decrypted.merchant_txn_id ||
      decrypted.payment_id ||
      'unknown_txn';

    if (processedTransactions.has(transactionId)) {
      console.log('Duplicate transaction ignored:', transactionId);
      return res.status(200).json({ message: 'Duplicate ignored' });
    }

    processedTransactions.add(transactionId);

    const message =
`✅ New EPS Payment

Txn ID: ${decrypted.trx_id || decrypted.transaction_id || 'N/A'}
Amount: ${decrypted.amount || 'N/A'}
Status: ${decrypted.status || 'N/A'}
Name: ${decrypted.customer_name || decrypted.name || 'N/A'}
Phone: ${decrypted.customer_phone || decrypted.phone || 'N/A'}
Email: ${decrypted.customer_email || decrypted.email || 'N/A'}`;

    await sendTelegramMessage(message);

    return res.status(200).json({ success: true });
  } catch (error) {
    console.error('IPN error:', error);
    try {
      await sendTelegramMessage(`❌ IPN server error:\n${error.message}`);
    } catch (_) {}
    return res.status(500).json({ error: 'Internal server error' });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
