// =============================================
//   FANFLIX BOT v2.0 - CONFIG
// =============================================

module.exports = {

  // TELEGRAM
  TELEGRAM_BOT_TOKEN: process.env.TELEGRAM_BOT_TOKEN || 'YOUR_BOT_TOKEN',
  TELEGRAM_CHAT_ID:   process.env.TELEGRAM_CHAT_ID   || 'YOUR_CHAT_ID',

  // EPS
  EPS_SECRET_KEY: process.env.EPS_SECRET_KEY || 'YOUR_EPS_SECRET_KEY',

  // SHOPIFY
  SHOPIFY_STORE: process.env.SHOPIFY_STORE || 'fanflixbd.myshopify.com',
  SHOPIFY_TOKEN: process.env.SHOPIFY_TOKEN || 'YOUR_SHOPIFY_ADMIN_TOKEN',

  // BULKSMSBD
  SMS_API_KEY:   process.env.SMS_API_KEY   || 'YOUR_BULKSMSBD_API_KEY',
  SMS_SENDER_ID: process.env.SMS_SENDER_ID || 'FanFlix',

  // EPS Default Payment Link
  EPS_PAYMENT_LINK: 'https://pg.eps.com.bd/DefaultPaymentLink?id=805A9AEE',

  // SETTINGS
  PORT:                     process.env.PORT || 3000,
  DUPLICATE_WINDOW_MINUTES: 30,
  VIP_RENEWAL_COUNT:        3,
  FOLLOW_UP_DELAY_MS:       60 * 60 * 1000, // 1 hour
  WINBACK_DAYS_AFTER_EXPIRY: 5,
  LOST_ALERT_DAYS_AFTER_EXPIRY: 3,
};
