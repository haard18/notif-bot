 require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const TelegramBot = require('node-telegram-bot-api');
const AWS = require('aws-sdk');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const CHAT_ID = process.env.CHAT_ID;
const AWS_REGION = process.env.AWS_REGION;
const AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID;
const AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY;
const SQS_QUEUE_URL = process.env.SQS_QUEUE_URL;

if (!SUPABASE_URL || !SUPABASE_KEY || !TELEGRAM_TOKEN || !CHAT_ID) {
  console.error('Missing required environment variables.');
  process.exit(1);
}

if (!AWS_REGION || !AWS_ACCESS_KEY_ID || !AWS_SECRET_ACCESS_KEY || !SQS_QUEUE_URL) {
  console.error('Missing required AWS/SQS environment variables.');
  process.exit(1);
}

// Configure AWS
AWS.config.update({
  region: AWS_REGION,
  accessKeyId: AWS_ACCESS_KEY_ID,
  secretAccessKey: AWS_SECRET_ACCESS_KEY
});

const sqs = new AWS.SQS({ apiVersion: '2012-11-05' });
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  realtime: { params: { eventsPerSecond: 10 } }
});
const bot = new TelegramBot(TELEGRAM_TOKEN, { polling: false });

// Deduplication system
const processedEvents = new Map();
const EVENT_CACHE_TTL = 5 * 60 * 1000; // 5 minutes
const milestoneCache = new Map(); // Cache for milestone tracking

// Clean up old events periodically
setInterval(() => {
  const now = Date.now();
  for (const [key, timestamp] of processedEvents.entries()) {
    if (now - timestamp > EVENT_CACHE_TTL) {
      processedEvents.delete(key);
    }
  }
  
  // Clean milestone cache (keep for 1 hour)
  for (const [key, timestamp] of milestoneCache.entries()) {
    if (now - timestamp > 60 * 60 * 1000) {
      milestoneCache.delete(key);
    }
  }
}, 60000); // Clean every minute

function generateEventId(table, event, payload) {
  // Create unique ID based on table, event type, primary key, and key field values
  const pk = payload.new?.id || payload.old?.id;
  const timestamp = payload.new?.updated_at || payload.new?.created_at || Date.now();
  
  // Include relevant field values in hash for UPDATE events
  let fieldHash = '';
  if (event === 'UPDATE' && payload.new) {
    const relevantFields = {
      Users: ['amount_deposited', 'total_pnl', 'total_volume', 'txns_executed', 'is_copytrading_enabled', 'fees_total'],
      Auto_Trade: ['status'],
      Copy_Wallets: ['is_enabled', 'percent_ratio']
    };
    
    if (relevantFields[table]) {
      const values = relevantFields[table].map(field => payload.new[field]).join('|');
      fieldHash = Buffer.from(values).toString('base64').slice(0, 10);
    }
  }
  
  return `${table}:${event}:${pk}:${timestamp}:${fieldHash}`;
}

function isDuplicateEvent(eventId) {
  if (processedEvents.has(eventId)) {
    return true;
  }
  processedEvents.set(eventId, Date.now());
  return false;
}

function sendTelegramMessage(text) {
  return bot.sendMessage(CHAT_ID, text, { parse_mode: 'HTML', disable_web_page_preview: true })
    .catch(err => {
      console.error('Failed to send Telegram message:', err?.message || err);
    });
}

function escapeHtml(str) {
  if (str === null || str === undefined) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function fmt(n) {
  if (n === null || n === undefined) return '0';
  if (typeof n === 'number') return n.toString();
  return String(n);
}

// Enhanced milestone tracking with deduplication
function checkAndNotifyMilestone(userId, milestoneType, oldValue, newValue, milestones, messageGenerator) {
  const cacheKey = `${userId}:${milestoneType}`;
  const lastNotified = milestoneCache.get(cacheKey) || 0;
  
  for (const milestone of milestones) {
    if (oldValue < milestone && newValue >= milestone && milestone > lastNotified) {
      milestoneCache.set(cacheKey, milestone);
      return messageGenerator(milestone, newValue);
    }
  }
  return null;
}

console.log('Connecting to Supabase...');
console.log('Connecting to AWS SQS...');

// SQS polling function (unchanged)
async function pollSQSMessages() {
  const params = {
    QueueUrl: SQS_QUEUE_URL,
    MaxNumberOfMessages: 10,
    WaitTimeSeconds: 20,
    VisibilityTimeout: 30
  };

  try {
    const data = await sqs.receiveMessage(params).promise();
    
    if (data.Messages && data.Messages.length > 0) {
      console.log(`Received ${data.Messages.length} SQS messages`);
      
      for (const message of data.Messages) {
        try {
          const orderData = JSON.parse(message.Body);
          await processSQSOrder(orderData);
          
          await sqs.deleteMessage({
            QueueUrl: SQS_QUEUE_URL,
            ReceiptHandle: message.ReceiptHandle
          }).promise();
          
        } catch (parseError) {
          console.error('Error parsing SQS message:', parseError);
          console.log('Raw message body:', message.Body);
          
          await sqs.deleteMessage({
            QueueUrl: SQS_QUEUE_URL,
            ReceiptHandle: message.ReceiptHandle
          }).promise();
        }
      }
    }
  } catch (error) {
    console.error('Error polling SQS:', error);
  }
  
  setTimeout(pollSQSMessages, 1000);
}

// Process SQS order message (unchanged)
async function processSQSOrder(orderData) {
  const {
    userId, username, orderId, orderType, side, marketId, marketQuestion,
    amount, shares, executionPrice, txHash, timestamp, status, outcome
  } = orderData;

  console.log('Processing SQS order:', orderData);

  const statusEmoji = {
    'matched': '✅', 'filled': '✅', 'partial': '⚠️',
    'cancelled': '❌', 'failed': '❌', 'pending': '⏳'
  };

  const sideEmoji = side === 'YES' ? '🟢' : '🔴';
  const emoji = statusEmoji[status] || '📊';

  const msg = `${emoji} <b>Order ${status}</b> ${sideEmoji}\n` +
    `User: @${escapeHtml(username)} (<code>${escapeHtml(userId)}</code>)\n` +
    `Market: <b>${escapeHtml(marketQuestion)}</b>\n` +
    `Side: <b>${escapeHtml(side)}</b> | Outcome: <b>${escapeHtml(outcome)}</b>\n` +
    `Amount: <b>$${fmt(amount)}</b> | Shares: <b>${fmt(shares)}</b>\n` +
    `Price: <b>${fmt(executionPrice)}</b>\n` +
    `Order ID: <code>${escapeHtml(orderId)}</code>\n` +
    `TX Hash: <code>${escapeHtml(txHash)}</code>\n` +
    `Time: <code>${escapeHtml(timestamp)}</code>`;

  await sendTelegramMessage(msg);
}

// Start SQS polling
pollSQSMessages();

(async () => {
  // Single consolidated channel for all database events
  const dbChannel = supabase.channel('db-events')
    // Users INSERT
    .on('postgres_changes', {
      event: 'INSERT',
      schema: 'public',
      table: 'Users'
    }, async (payload) => {
      const eventId = generateEventId('Users', 'INSERT', payload);
      if (isDuplicateEvent(eventId)) {
        console.log('Skipping duplicate Users INSERT event');
        return;
      }

      const user = payload.new;
      const msg = `👤 <b>New user registered</b>\n` +
        `ID: <code>${user.id}</code>\n` +
        `Telegram: @${escapeHtml(user.telegram_username)}\n` +
        `Wallet: <code>${escapeHtml(user.wallet_address)}</code>\n` +
        `Deposited: <b>${fmt(user.amount_deposited)}</b>\n` +
        `Copytrading: <b>${user.is_copytrading_enabled ? 'Enabled' : 'Disabled'}</b>\n` +
        `Referral code: <code>${escapeHtml(user.referral_code || 'None')}</code>\n` +
        `Created: <code>${user.created_at}</code>`;
      console.log('Users INSERT:', user);
      await sendTelegramMessage(msg);
    })
    
    // Users UPDATE with improved logic
    .on('postgres_changes', {
      event: 'UPDATE',
      schema: 'public',
      table: 'Users'
    }, async (payload) => {
      const eventId = generateEventId('Users', 'UPDATE', payload);
      if (isDuplicateEvent(eventId)) {
        console.log('Skipping duplicate Users UPDATE event');
        return;
      }

      const userNew = payload.new;
      const userOld = payload.old || {};
      const id = userNew.id;
      const username = userNew.telegram_username;
      
      // Only process if we have meaningful old values
      const hasRelevantOldData = userOld && Object.keys(userOld).length > 1;
      if (!hasRelevantOldData) {
        console.log('Skipping Users UPDATE - insufficient old data:', userOld);
        return;
      }

      // Deposit detection
      const newAmt = Number(userNew.amount_deposited || 0);
      const oldAmt = Number(userOld.amount_deposited || 0);
      if (newAmt > oldAmt && oldAmt >= 0) {
        const depositAmount = newAmt - oldAmt;
        if (depositAmount > 0.01) { // Minimum threshold to avoid dust
          const msg = `💰 <b>Deposit detected</b>\n` +
            `User: @${escapeHtml(username)} (<code>${fmt(id)}</code>)\n` +
            `Amount: <b>+${fmt(depositAmount)}</b>\n` +
            `New total: <b>${fmt(newAmt)}</b>`;
          console.log('Users UPDATE (deposit):', { before: oldAmt, after: newAmt, deposit: depositAmount });
          await sendTelegramMessage(msg);
        }
      }

      // PnL changes
      const newPnL = Number(userNew.total_pnl || 0);
      const oldPnL = Number(userOld.total_pnl || 0);
      const pnlChange = newPnL - oldPnL;
      if (Math.abs(pnlChange) >= 100) {
        const emoji = pnlChange > 0 ? '📈' : '📉';
        const msg = `${emoji} <b>Significant PnL change</b>\n` +
          `User: @${escapeHtml(username)} (<code>${fmt(id)}</code>)\n` +
          `Change: <b>${pnlChange > 0 ? '+' : ''}${fmt(pnlChange)}</b>\n` +
          `Total PnL: <b>${fmt(newPnL)}</b>`;
        console.log('Users UPDATE (PnL change):', { before: oldPnL, after: newPnL, change: pnlChange });
        await sendTelegramMessage(msg);
      }

      // Volume milestones with deduplication
      const newVolume = Number(userNew.total_volume || 0);
      const oldVolume = Number(userOld.total_volume || 0);
      const volumeMilestones = [1000, 5000, 10000, 25000, 50000, 100000];
      
      const volumeMsg = checkAndNotifyMilestone(id, 'volume', oldVolume, newVolume, volumeMilestones, (milestone, current) => {
        return `🎯 <b>Volume milestone reached!</b>\n` +
          `User: @${escapeHtml(username)} (<code>${fmt(id)}</code>)\n` +
          `Milestone: <b>$${milestone.toLocaleString()}</b>\n` +
          `Current volume: <b>$${fmt(current)}</b>`;
      });
      
      if (volumeMsg) {
        console.log('Users UPDATE (volume milestone):', { user: id, volume: newVolume });
        await sendTelegramMessage(volumeMsg);
      }

      // Trading milestones with deduplication
      const newTxns = Number(userNew.txns_executed || 0);
      const oldTxns = Number(userOld.txns_executed || 0);
      const txnMilestones = [10, 50, 100, 500, 1000];
      
      const txnMsg = checkAndNotifyMilestone(id, 'txns', oldTxns, newTxns, txnMilestones, (milestone, current) => {
        return `🏆 <b>Trading milestone reached!</b>\n` +
          `User: @${escapeHtml(username)} (<code>${fmt(id)}</code>)\n` +
          `Transactions: <b>${fmt(current)}</b>\n` +
          `Markets traded: <b>${fmt(userNew.markets_traded)}</b>`;
      });
      
      if (txnMsg) {
        console.log('Users UPDATE (txn milestone):', { user: id, txns: newTxns });
        await sendTelegramMessage(txnMsg);
      }

      // Copytrading status changes
      if ('is_copytrading_enabled' in userOld) {
        const newCopytrading = Boolean(userNew.is_copytrading_enabled);
        const oldCopytrading = Boolean(userOld.is_copytrading_enabled);
        if (newCopytrading !== oldCopytrading) {
          const status = newCopytrading ? 'enabled' : 'disabled';
          const emoji = newCopytrading ? '🔄' : '⏸️';
          const msg = `${emoji} <b>Copytrading ${status}</b>\n` +
            `User: @${escapeHtml(username)} (<code>${fmt(id)}</code>)`;
          console.log('Users UPDATE (copytrading):', { user: id, status });
          await sendTelegramMessage(msg);
        }
      }

      // Fee accumulation alerts
      const newFees = Number(userNew.fees_total || 0);
      const oldFees = Number(userOld.fees_total || 0);
      const feeIncrease = newFees - oldFees;
      if (feeIncrease >= 50) {
        const msg = `💸 <b>High fees incurred</b>\n` +
          `User: @${escapeHtml(username)} (<code>${fmt(id)}</code>)\n` +
          `Session fees: <b>$${fmt(feeIncrease)}</b>\n` +
          `Total fees: <b>$${fmt(newFees)}</b>`;
        console.log('Users UPDATE (high fees):', { user: id, sessionFees: feeIncrease, totalFees: newFees });
        await sendTelegramMessage(msg);
      }
    })

    // Auto_Trade events
    .on('postgres_changes', {
      event: 'UPDATE',
      schema: 'public',
      table: 'Auto_Trade'
    }, async (payload) => {
      const eventId = generateEventId('Auto_Trade', 'UPDATE', payload);
      if (isDuplicateEvent(eventId)) {
        console.log('Skipping duplicate Auto_Trade UPDATE event');
        return;
      }

      const status = String(payload.new?.status || '').toLowerCase();
      const prevStatus = String(payload.old?.status || '').toLowerCase();
      const trade = payload.new;

      if (status !== prevStatus) {
        if (status === 'executed') {
          const msg = `✅ <b>Trade executed successfully</b>\n` +
            `User: <code>${escapeHtml(fmt(trade.user_id))}</code>\n` +
            `Market: <b>${escapeHtml(trade.market_title)}</b>\n` +
            `Side: <b>${escapeHtml(trade.side)}</b>\n` +
            `Size: <b>${fmt(trade.copied_size || trade.original_size)}</b> @ <b>${fmt(trade.copied_price || trade.original_price)}</b>\n` +
            `Trade hash: <code>${escapeHtml(trade.copied_trade_hash)}</code>`;
          console.log('Auto_Trade UPDATE (executed):', trade);
          await sendTelegramMessage(msg);
        } else if (status === 'failed') {
          const msg = `❌ <b>Trade failed</b>\n` +
            `User: <code>${escapeHtml(fmt(trade.user_id))}</code>\n` +
            `Market: <b>${escapeHtml(trade.market_title)}</b>\n` +
            `Side: <b>${escapeHtml(trade.side)}</b>\n` +
            `Error: <code>${escapeHtml(trade.error_message || 'Unknown error')}</code>\n` +
            `Original hash: <code>${escapeHtml(trade.original_trade_hash)}</code>`;
          console.log('Auto_Trade UPDATE (failed):', trade);
          await sendTelegramMessage(msg);
        } else if (status === 'skipped') {
          const msg = `⏭️ <b>Trade skipped</b>\n` +
            `User: <code>${escapeHtml(fmt(trade.user_id))}</code>\n` +
            `Market: <b>${escapeHtml(trade.market_title)}</b>\n` +
            `Reason: <code>${escapeHtml(trade.error_message || 'Trade conditions not met')}</code>`;
          console.log('Auto_Trade UPDATE (skipped):', trade);
          await sendTelegramMessage(msg);
        }
      }
    })

    .on('postgres_changes', {
      event: 'INSERT',
      schema: 'public',
      table: 'Auto_Trade'
    }, async (payload) => {
      const eventId = generateEventId('Auto_Trade', 'INSERT', payload);
      if (isDuplicateEvent(eventId)) {
        console.log('Skipping duplicate Auto_Trade INSERT event');
        return;
      }

      const t = payload.new;
      const msg = `🆕 <b>New trade detected</b>\n` +
        `User: <code>${escapeHtml(fmt(t.user_id))}</code>\n` +
        `Market: <b>${escapeHtml(t.market_title)}</b>\n` +
        `Outcome: <b>${escapeHtml(t.outcome)}</b>\n` +
        `Side: <b>${escapeHtml(t.side)}</b>\n` +
        `Original hash: <code>${escapeHtml(t.original_trade_hash)}</code>\n` +
        `Size: <b>${escapeHtml(fmt(t.original_size))}</b> @ <b>${escapeHtml(fmt(t.original_price))}</b>\n` +
        `Position %: <b>${fmt(t.position_percentage)}%</b>\n` +
        `Status: <b>${escapeHtml(t.status)}</b>\n` +
        `Watched wallet: <code>${escapeHtml(t.watched_wallet)}</code>`;
      console.log('Auto_Trade INSERT:', t);
      await sendTelegramMessage(msg);
    })

    // Copy_Wallets events
    .on('postgres_changes', {
      event: 'INSERT',
      schema: 'public',
      table: 'Copy_Wallets'
    }, async (payload) => {
      const eventId = generateEventId('Copy_Wallets', 'INSERT', payload);
      if (isDuplicateEvent(eventId)) {
        console.log('Skipping duplicate Copy_Wallets INSERT event');
        return;
      }

      const wallet = payload.new;
      const msg = `👁️ <b>New wallet added for copying</b>\n` +
        `User: <code>${escapeHtml(fmt(wallet.user_id))}</code>\n` +
        `Wallet: <code>${escapeHtml(wallet.wallet_address)}</code>\n` +
        `Copy ratio: <b>${fmt(wallet.percent_ratio * 100)}%</b>\n` +
        `Status: <b>${wallet.is_enabled ? 'Enabled' : 'Disabled'}</b>`;
      console.log('Copy_Wallets INSERT:', wallet);
      await sendTelegramMessage(msg);
    })

    .on('postgres_changes', {
      event: 'UPDATE',
      schema: 'public',
      table: 'Copy_Wallets'
    }, async (payload) => {
      const eventId = generateEventId('Copy_Wallets', 'UPDATE', payload);
      if (isDuplicateEvent(eventId)) {
        console.log('Skipping duplicate Copy_Wallets UPDATE event');
        return;
      }

      const walletNew = payload.new;
      const walletOld = payload.old || {};
      
      if (Object.keys(walletOld).length <= 1) {
        console.log('Skipping Copy_Wallets UPDATE - insufficient old data');
        return;
      }
      
      const newEnabled = walletNew.is_enabled;
      const oldEnabled = walletOld.is_enabled;
      const newRatio = walletNew.percent_ratio;
      const oldRatio = walletOld.percent_ratio;
      
      if (newEnabled !== oldEnabled) {
        const status = newEnabled ? 'enabled' : 'disabled';
        const emoji = newEnabled ? '✅' : '❌';
        const msg = `${emoji} <b>Copy wallet ${status}</b>\n` +
          `User: <code>${escapeHtml(fmt(walletNew.user_id))}</code>\n` +
          `Wallet: <code>${escapeHtml(walletNew.wallet_address)}</code>\n` +
          `Copy ratio: <b>${fmt(walletNew.percent_ratio * 100)}%</b>`;
        console.log('Copy_Wallets UPDATE (enabled/disabled):', walletNew);
        await sendTelegramMessage(msg);
      } else if (Math.abs(newRatio - oldRatio) > 0.001) {
        const msg = `⚙️ <b>Copy ratio updated</b>\n` +
          `User: <code>${escapeHtml(fmt(walletNew.user_id))}</code>\n` +
          `Wallet: <code>${escapeHtml(walletNew.wallet_address)}</code>\n` +
          `Old ratio: <b>${fmt(oldRatio * 100)}%</b> → <b>${fmt(newRatio * 100)}%</b>`;
        console.log('Copy_Wallets UPDATE (ratio change):', { old: oldRatio, new: newRatio });
        await sendTelegramMessage(msg);
      }
    })

    // Monthly_Active_Users INSERT
    .on('postgres_changes', {
      event: 'INSERT',
      schema: 'public',
      table: 'Monthly_Active_Users'
    }, async (payload) => {
      const eventId = generateEventId('Monthly_Active_Users', 'INSERT', payload);
      if (isDuplicateEvent(eventId)) {
        console.log('Skipping duplicate Monthly_Active_Users INSERT event');
        return;
      }

      const record = payload.new;
      const msg = `📊 <b>Monthly active user recorded</b>\n` +
        `User: <code>${escapeHtml(fmt(record.user_id))}</code>\n` +
        `Month: <b>${record.month_number}/${record.year}</b>\n` +
        `Transactions: <b>${fmt(record.transaction_count)}</b>`;
      console.log('Monthly_Active_Users INSERT:', record);
      await sendTelegramMessage(msg);
    })

    .subscribe((status) => {
      console.log('Database events subscription status:', status);
      if (status === 'SUBSCRIBED') {
        console.log('Successfully subscribed to all database events');
      } else if (status === 'CHANNEL_ERROR') {
        console.error('Channel error - attempting to reconnect in 5 seconds...');
        setTimeout(() => {
          console.log('Attempting to reconnect...');
          // Could implement reconnection logic here
        }, 5000);
      }
    });

  console.log('Enhanced bot with deduplication is running and listening for events...');
})();