# EPS IPN → Telegram Bot

পেমেন্ট হলে সাথে সাথে Telegram-এ notification পাবেন।

-----

## ধাপ ১ — Telegram Bot তৈরি করুন

1. Telegram-এ @BotFather খুলুন
1. /newbot টাইপ করুন
1. Bot-এর নাম দিন (যেমন: My EPS Bot)
1. Bot Token কপি করুন → এটাই TELEGRAM_BOT_TOKEN
1. এরপর আপনার bot-কে একটা chat/group-এ add করুন
1. Chat ID পেতে: https://api.telegram.org/bot<TOKEN>/getUpdates — এখান থেকে chat.id নিন → এটাই TELEGRAM_CHAT_ID

-----

## ধাপ ২ — Railway-তে Deploy করুন

1. https://railway.app — GitHub দিয়ে sign up
1. New Project → Deploy from GitHub repo
1. এই ফোল্ডারটা GitHub-এ upload করুন (বা Railway CLI ব্যবহার করুন)
1. Deploy হলে Settings → Environment Variables এ যান

নিচের তিনটা variable যোগ করুন:
EPS_SECRET_KEY      = (EPS dashboard থেকে আপনার secret key)
TELEGRAM_BOT_TOKEN  = (BotFather থেকে পাওয়া token)
TELEGRAM_CHAT_ID    = (আপনার chat/group এর ID)

1. Railway আপনাকে একটা URL দেবে, যেমন:
   https://eps-ipn-bot.up.railway.app

-----

## ধাপ ৩ — EPS Dashboard-এ IPN URL দিন

আপনার EPS Merchant Dashboard-এ লগইন করুন।
IPN URL হিসেবে দিন:
https://eps-ipn-bot.up.railway.app/ipn

-----

## Notification দেখতে এরকম হবে
✅ নতুন পেমেন্ট সফল

🔖 Transaction ID: EPS123456789
🛒 Order ID: ORDER-001
💰 Amount: ৳1,500.00
📱 Payment Method: bKash
👤 Customer: John Doe
📞 Phone: +8801234567890
🕐 Time: ৬ এপ্রিল ২০২৫, রাত ৮:৩০

-----

## সমস্যা হলে

- Railway logs চেক করুন (Deploy → View Logs)
- EPS Secret Key সঠিক আছে কিনা নিশ্চিত করুন
- Telegram Bot আপনার chat-এ add আছে কিনা দেখুন
