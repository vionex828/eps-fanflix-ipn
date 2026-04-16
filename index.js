const express = require('express');
const app = express();

app.use(express.json());

const PORT = process.env.PORT || 3000;
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

app.get('/', (req, res) => {
  res.send('Bot is running');
});

app.get('/test', async (req, res) => {
  try {
    const response = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: TELEGRAM_CHAT_ID,
        text: 'Test message from Railway'
      })
    });

    const result = await response.json();
    console.log('Telegram test result:', result);
    res.send('Test sent');
  } catch (error) {
    console.error('Test failed:', error);
    res.status(500).send('Test failed');
  }
});

app.post('/ipn', async (req, res) => {
  console.log('=== EPS IPN HIT ===');
  console.log('Headers:', JSON.stringify(req.headers));
  console.log('Body:', JSON.stringify(req.body));

  try {
    const response = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: TELEGRAM_CHAT_ID,
        text: `New IPN received:\n${JSON.stringify(req.body)}`
      })
    });

    const result = await response.json();
    console.log('Telegram response:', result);

    return res.status(200).send('OK');
  } catch (error) {
    console.error('Telegram send error:', error);
    return res.status(500).send('Telegram failed');
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
