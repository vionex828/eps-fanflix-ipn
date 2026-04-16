const express = require('express');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

app.get('/', (req, res) => {
  res.send('EPS Telegram Bot is running');
});

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

app.post('/ipn', async (req, res) => {
  console.log('IPN received:', JSON.stringify(req.body));

  try {
    await sendTelegramMessage(`New IPN received:\n${JSON.stringify(req.body)}`);
    return res.status(200).send('OK');
  } catch (error) {
    console.error('Telegram send error:', error.message);
    return res.status(500).send('Telegram failed');
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
