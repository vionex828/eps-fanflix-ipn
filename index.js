const express = require('express');
const crypto = require('crypto');
const axios = require('axios');

const app = express();
app.use(express.json());

const EPS_SECRET_KEY = process.env.EPS_SECRET_KEY;     // আপনার ২৭ ক্যারেক্টারের key
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const CHAT_ID = process.env.CHAT_ID;

if (!EPS_SECRET_KEY || !TELEGRAM_TOKEN || !CHAT_ID) {
  console.error("❌ Missing environment variables! Please set EPS_SECRET_KEY, TELEGRAM_TOKEN, and CHAT_ID in Railway.");
}

app.post('/eps-ipn', async (req, res) => {
  try {
    const encryptedData = req.body.Data;

    if (!encryptedData || !encryptedData.includes(':')) {
      console.error("Invalid Data format from EPS");
      return res.status(400).json({ status: 'ERROR', message: 'Invalid payload' });
    }

    // Decrypt করা (২৭ ক্যারেক্টার key সাপোর্ট সহ)
    const [ivBase64, cipherTextBase64] = encryptedData.split(':');
    const iv = Buffer.from(ivBase64, 'base64');
    const cipherText = Buffer.from(cipherTextBase64, 'base64');

    // Key কে Buffer-এ কনভার্ট করা (UTF-8)
    const keyBuffer = Buffer.from(EPS_SECRET_KEY, 'utf8');

    const decipher = crypto.createDecipheriv('aes-256-cbc', keyBuffer, iv);
    let decrypted = decipher.update(cipherText, 'base64', 'utf8');
    decrypted += decipher.final('utf8');

    const payload = JSON.parse(decrypted);

    // সুন্দর Telegram মেসেজ
    let statusEmoji = payload.status === 'SUCCESS' ? '✅' : (payload.status === 'FAILED' ? '❌' : '📢');
    let message = `${statusEmoji} EPS পেমেন্ট আপডেট!\n\n`;
    message += `Status: ${payload.status}\n`;
    message += `Transaction ID: ${payload.transaction_id || 'N/A'}\n`;
    message += `Order ID: ${payload.merchant_transaction_id || 'N/A'}\n`;
    message += `Amount: ৳${payload.amount || 'N/A'}\n`;
    message += `Method: ${payload.payment_method || 'N/A'}\n`;
    message += `Customer: ${payload.customer_name || 'N/A'}\n`;
    message += `Phone: ${payload.customer_phone || 'N/A'}\n`;
    message += `Time: ${new Date().toLocaleString('en-BD')}\n`;

    // Telegram-এ পাঠানো
    await axios.post(`https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`, {
      chat_id: CHAT_ID,
      text: message,
      parse_mode: 'HTML'
    });

    console.log(`✅ Notification sent for ${payload.status} - Order: ${payload.merchant_transaction_id}`);
    res.json({ status: 'OK' });

  } catch (error) {
    console.error('❌ IPN Decryption Error:', error.message);
    res.status(500).json({ status: 'ERROR', message: 'Decryption failed' });
  }
});

// Health check route
app.get('/', (req, res) => {
  res.send('EPS Telegram IPN Bot is running successfully ✅');
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
