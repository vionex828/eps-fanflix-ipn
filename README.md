# FanFlix Bot v2.0 — Setup Guide

## Step 1 — Telegram Bot
1. Open @BotFather → /newbot → copy Token
2. Open @userinfobot → /start → copy your Chat ID

## Step 2 — Shopify Admin Token
Settings → Apps → Develop apps → Create app
Enable: read_orders, write_orders → Install → copy token

## Step 3 — BulkSMSBD
Login → API section → copy API Key + Sender ID

## Step 4 — Fill config.js or Railway Environment Variables
```
TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID
EPS_SECRET_KEY
SHOPIFY_STORE      → fanflixbd.myshopify.com
SHOPIFY_TOKEN
SMS_API_KEY
SMS_SENDER_ID
```

## Step 5 — Deploy to Railway
1. Upload to GitHub
2. railway.app → New Project → Deploy from GitHub
3. Add environment variables in Railway settings
4. Copy your Railway URL

## Step 6 — Set EPS IPN URL
In EPS dashboard → IPN URL:
https://your-railway-url.up.railway.app/eps-ipn

## Step 7 — Set Shopify Webhook (for follow-up)
Shopify Admin → Settings → Notifications → Webhooks
Event: Order creation
URL: https://your-railway-url.up.railway.app/shopify-order

## Step 8 — Test
Make a test payment → Telegram alert should arrive instantly ✅

---

## Commands
/start /customers /expiring /today /revenue /stats
/product /retention /top /pending /search /add /edit
/delete /export /setmsg3 /setmsg1 /setwinback /setfollowup
