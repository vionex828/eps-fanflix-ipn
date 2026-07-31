// =============================================
//   FANFLIX BOT v6.2 - CONFIG
// =============================================

module.exports = {

  // TELEGRAM
  TELEGRAM_BOT_TOKEN: process.env.TELEGRAM_BOT_TOKEN || 'YOUR_BOT_TOKEN',
  TELEGRAM_CHAT_ID:   process.env.TELEGRAM_CHAT_ID   || 'YOUR_CHAT_ID',

  // EPS
  EPS_SECRET_KEY: process.env.EPS_SECRET_KEY || 'YOUR_EPS_SECRET_KEY',

  // SHOPIFY
  SHOPIFY_STORE:         process.env.SHOPIFY_STORE         || 'fanflixbd.myshopify.com',
  SHOPIFY_CLIENT_ID:     process.env.SHOPIFY_CLIENT_ID     || 'YOUR_CLIENT_ID',
  SHOPIFY_CLIENT_SECRET: process.env.SHOPIFY_CLIENT_SECRET || 'YOUR_CLIENT_SECRET',

  // FANFLIX HOUSEHOLD — auto-create link after payment
  FANFLIX_HOUSEHOLD_URL:  process.env.FANFLIX_HOUSEHOLD_URL  || 'https://household.fanflixbd.com',
  FANFLIX_ADMIN_SECRET:   process.env.FANFLIX_ADMIN_SECRET   || '@Orsha420@',

  // RESPOND.IO — WhatsApp template messages (order confirmation, payment pending)
  RESPONDIO_API_KEY: process.env.RESPONDIO_API_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MzMzNjQsInNwYWNlSWQiOjM1MjU5OCwib3JnSWQiOjM0NzU3MywidHlwZSI6ImFwaSIsImlhdCI6MTc4NTQxMTk5M30.K1MNnRwqq2kZDdW8lg-EXH1vLEc8p_yTYNKr_uEWVF4',
  RESPONDIO_CHANNEL_ID: 442671,

  // WHITELISTED DOMAINS
  WHITELISTED_DOMAINS: [
    'eps.com.bd',               // EPS — payment gateway
    'household.fanflixbd.com',  // FanFlix household server
    'respond.io',               // Respond.io — WhatsApp template messages
  ],

  // EPS Payment Link
  EPS_PAYMENT_LINK: 'https://pg.eps.com.bd/DefaultPaymentLink?id=805A9AEE',

  // SETTINGS
  PORT:                         process.env.PORT || 3000,
  DUPLICATE_WINDOW_MINUTES:     30,
  VIP_RENEWAL_COUNT:            3,
  FOLLOW_UP_DELAY_MS:           60 * 60 * 1000,  // 1 hour
  LOST_ALERT_DAYS_AFTER_EXPIRY: 3,
};
