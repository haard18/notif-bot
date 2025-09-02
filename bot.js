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

console.log('Connecting to Supabase...');
console.log('Connecting to AWS SQS...');

// SQS polling function
async function pollSQSMessages() {
  const params = {
    QueueUrl: SQS_QUEUE_URL,
    MaxNumberOfMessages: 10,
    WaitTimeSeconds: 20, // Long polling
    VisibilityTimeout: 30       // <-- correct key
  };

  try {
    const data = await sqs.receiveMessage(params).promise();
    
    if (data.Messages && data.Messages.length > 0) {
      console.log(`Received ${data.Messages.length} SQS messages`);
      
      for (const message of data.Messages) {
        try {
          const orderData = JSON.parse(message.Body);
          await processSQSOrder(orderData);
          
          // Delete message after successful processing
          await sqs.deleteMessage({
            QueueUrl: SQS_QUEUE_URL,
            ReceiptHandle: message.ReceiptHandle
          }).promise();
          
        } catch (parseError) {
          console.error('Error parsing SQS message:', parseError);
          console.log('Raw message body:', message.Body);
          
          // Delete malformed messages to prevent infinite reprocessing
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
  
  // Continue polling
  setTimeout(pollSQSMessages, 1000);
}

// Process SQS order message
async function processSQSOrder(orderData) {
  const {
    userId,
    username,
    orderId,
    orderType,
    side,
    marketId,
    marketQuestion,
    amount,
    shares,
    executionPrice,
    txHash,
    timestamp,
    status,
    outcome
  } = orderData;

  console.log('Processing SQS order:', orderData);

  const statusEmoji = {
    'matched': '✅',
    'filled': '✅',
    'partial': '⚠️',
    'cancelled': '❌',
    'failed': '❌',
    'pending': '⏳'
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
  // Users INSERT
  const usersInsert = supabase.channel('users-insert')
    .on('postgres_changes', {
      event: 'INSERT',
      schema: 'public',
      table: 'Users'
    }, async (payload) => {
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
    .subscribe((status) => {
      if (status === 'SUBSCRIBED') console.log('Subscribed to Users INSERT');
    });

  // Users UPDATE - Multiple important changes
  const usersUpdate = supabase.channel('users-update')
    .on('postgres_changes', {
      event: 'UPDATE',
      schema: 'public',
      table: 'Users'
    }, async (payload) => {
      const userNew = payload.new;
      const userOld = payload.old || {};
      const id = userNew.id;
      const username = userNew.telegram_username;
      
      // Deposit detection (amount_deposited increases)
      const newAmt = Number(userNew.amount_deposited || 0);
      const oldAmt = Number(userOld.amount_deposited ?? 0);
      if (!isNaN(newAmt) && newAmt > oldAmt) {
        const depositAmount = newAmt - oldAmt;
        const msg = `💰 <b>Deposit detected</b>\n` +
          `User: @${escapeHtml(username)} (<code>${fmt(id)}</code>)\n` +
          `Amount: <b>+${fmt(depositAmount)}</b>\n` +
          `New total: <b>${fmt(newAmt)}</b>`;
        console.log('Users UPDATE (deposit):', { before: oldAmt, after: newAmt, deposit: depositAmount });
        await sendTelegramMessage(msg);
      }

      // PnL changes (significant gains/losses)
      const newPnL = Number(userNew.total_pnl || 0);
      const oldPnL = Number(userOld.total_pnl ?? 0);
      const pnlChange = newPnL - oldPnL;
      if (Math.abs(pnlChange) >= 100) { // Alert for PnL changes >= $100
        const emoji = pnlChange > 0 ? '📈' : '📉';
        const msg = `${emoji} <b>Significant PnL change</b>\n` +
          `User: @${escapeHtml(username)} (<code>${fmt(id)}</code>)\n` +
          `Change: <b>${pnlChange > 0 ? '+' : ''}${fmt(pnlChange)}</b>\n` +
          `Total PnL: <b>${fmt(newPnL)}</b>`;
        console.log('Users UPDATE (PnL change):', { before: oldPnL, after: newPnL, change: pnlChange });
        await sendTelegramMessage(msg);
      }

      // Volume milestones
      const newVolume = Number(userNew.total_volume || 0);
      const oldVolume = Number(userOld.total_volume ?? 0);
      const volumeMilestones = [1000, 5000, 10000, 25000, 50000, 100000];
      for (const milestone of volumeMilestones) {
        if (oldVolume < milestone && newVolume >= milestone) {
          const msg = `🎯 <b>Volume milestone reached!</b>\n` +
            `User: @${escapeHtml(username)} (<code>${fmt(id)}</code>)\n` +
            `Milestone: <b>$${milestone.toLocaleString()}</b>\n` +
            `Current volume: <b>$${fmt(newVolume)}</b>`;
          console.log('Users UPDATE (volume milestone):', { user: id, milestone, volume: newVolume });
          await sendTelegramMessage(msg);
          break; // Only alert for the first milestone reached
        }
      }

      // Trading activity milestones
      const newTxns = Number(userNew.txns_executed || 0);
      const oldTxns = Number(userOld.txns_executed ?? 0);
      const txnMilestones = [10, 50, 100, 500, 1000];
      for (const milestone of txnMilestones) {
        if (oldTxns < milestone && newTxns >= milestone) {
          const msg = `🏆 <b>Trading milestone reached!</b>\n` +
            `User: @${escapeHtml(username)} (<code>${fmt(id)}</code>)\n` +
            `Transactions: <b>${fmt(newTxns)}</b>\n` +
            `Markets traded: <b>${fmt(userNew.markets_traded)}</b>`;
          console.log('Users UPDATE (txn milestone):', { user: id, milestone, txns: newTxns });
          await sendTelegramMessage(msg);
          break;
        }
      }

      // Copytrading status changes - only notify if we have a previous value
      const hasOldCopyField = Object.prototype.hasOwnProperty.call(userOld, 'is_copytrading_enabled');
      const newCopytrading = Boolean(userNew.is_copytrading_enabled);
      const oldCopytrading = Boolean(userOld.is_copytrading_enabled);
      if (!hasOldCopyField) {
        // Sometimes realtime payload.old contains only the primary key or limited columns.
        // In that case we skip notifying to avoid spurious messages.
        console.log('Users UPDATE (copytrading): skipping notification because payload.old lacks is_copytrading_enabled', { user: id });
      } else if (newCopytrading !== oldCopytrading) {
        const status = newCopytrading ? 'enabled' : 'disabled';
        const emoji = newCopytrading ? '🔄' : '⏸️';
        const msg = `${emoji} <b>Copytrading ${status}</b>\n` +
          `User: @${escapeHtml(username)} (<code>${fmt(id)}</code>)`;
        console.log('Users UPDATE (copytrading):', { user: id, status });
        await sendTelegramMessage(msg);
      }

      // Fee accumulation alerts (for high-volume traders)
      const newFees = Number(userNew.fees_total || 0);
      const oldFees = Number(userOld.fees_total ?? 0);
      const feeIncrease = newFees - oldFees;
      if (feeIncrease >= 50) { // Alert for fee increases >= $50
        const msg = `💸 <b>High fees incurred</b>\n` +
          `User: @${escapeHtml(username)} (<code>${fmt(id)}</code>)\n` +
          `Session fees: <b>$${fmt(feeIncrease)}</b>\n` +
          `Total fees: <b>$${fmt(newFees)}</b>`;
        console.log('Users UPDATE (high fees):', { user: id, sessionFees: feeIncrease, totalFees: newFees });
        await sendTelegramMessage(msg);
      }
    })
    .subscribe((status) => {
      if (status === 'SUBSCRIBED') console.log('Subscribed to Users UPDATE');
    });

  // Auto_Trade UPDATE (status changes)
  const autoTradeUpdate = supabase.channel('auto-trade-update')
    .on('postgres_changes', {
      event: 'UPDATE',
      schema: 'public',
      table: 'Auto_Trade'
    }, async (payload) => {
      try {
        console.log('Auto_Trade UPDATE payload.old:', payload.old);
        console.log('Auto_Trade UPDATE payload.new:', payload.new);
      } catch (e) {
        console.log('Auto_Trade UPDATE payload (raw):', payload);
      }

      const status = String(payload.new?.status || '').toLowerCase();
      const prevStatus = String(payload.old?.status || '').toLowerCase();
      const trade = payload.new;

      if (status === 'executed' && prevStatus !== 'executed') {
        const msg = `✅ <b>Trade executed successfully</b>\n` +
          `User: <code>${escapeHtml(fmt(trade.user_id))}</code>\n` +
          `Market: <b>${escapeHtml(trade.market_title)}</b>\n` +
          `Side: <b>${escapeHtml(trade.side)}</b>\n` +
          `Size: <b>${fmt(trade.copied_size || trade.original_size)}</b> @ <b>${fmt(trade.copied_price || trade.original_price)}</b>\n` +
          `Trade hash: <code>${escapeHtml(trade.copied_trade_hash)}</code>`;
        console.log('Auto_Trade UPDATE (executed):', trade);
        await sendTelegramMessage(msg);
      } else if (status === 'failed' && prevStatus !== 'failed') {
        const msg = `❌ <b>Trade failed</b>\n` +
          `User: <code>${escapeHtml(fmt(trade.user_id))}</code>\n` +
          `Market: <b>${escapeHtml(trade.market_title)}</b>\n` +
          `Side: <b>${escapeHtml(trade.side)}</b>\n` +
          `Error: <code>${escapeHtml(trade.error_message || 'Unknown error')}</code>\n` +
          `Original hash: <code>${escapeHtml(trade.original_trade_hash)}</code>`;
        console.log('Auto_Trade UPDATE (failed):', trade);
        await sendTelegramMessage(msg);
      } else if (status === 'skipped' && prevStatus !== 'skipped') {
        const msg = `⏭️ <b>Trade skipped</b>\n` +
          `User: <code>${escapeHtml(fmt(trade.user_id))}</code>\n` +
          `Market: <b>${escapeHtml(trade.market_title)}</b>\n` +
          `Reason: <code>${escapeHtml(trade.error_message || 'Trade conditions not met')}</code>`;
        console.log('Auto_Trade UPDATE (skipped):', trade);
        await sendTelegramMessage(msg);
      } else {
        console.log(`Auto_Trade UPDATE (no relevant transition): status=${status} prev=${prevStatus}`);
      }
    })
    .subscribe((status) => {
      if (status === 'SUBSCRIBED') console.log('Subscribed to Auto_Trade UPDATE');
    });

  // Auto_Trade INSERT (new trade detected)
  const autoTradeInsert = supabase.channel('auto-trade-insert')
    .on('postgres_changes', {
      event: 'INSERT',
      schema: 'public',
      table: 'Auto_Trade'
    }, async (payload) => {
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
    .subscribe((status) => {
      if (status === 'SUBSCRIBED') console.log('Subscribed to Auto_Trade INSERT');
    });

  // Copy_Wallets INSERT (new wallet added for copying)
  const copyWalletsInsert = supabase.channel('copy-wallets-insert')
    .on('postgres_changes', {
      event: 'INSERT',
      schema: 'public',
      table: 'Copy_Wallets'
    }, async (payload) => {
      const wallet = payload.new;
      const msg = `👁️ <b>New wallet added for copying</b>\n` +
        `User: <code>${escapeHtml(fmt(wallet.user_id))}</code>\n` +
        `Wallet: <code>${escapeHtml(wallet.wallet_address)}</code>\n` +
        `Copy ratio: <b>${fmt(wallet.percent_ratio * 100)}%</b>\n` +
        `Status: <b>${wallet.is_enabled ? 'Enabled' : 'Disabled'}</b>`;
      console.log('Copy_Wallets INSERT:', wallet);
      await sendTelegramMessage(msg);
    })
    .subscribe((status) => {
      if (status === 'SUBSCRIBED') console.log('Subscribed to Copy_Wallets INSERT');
    });

  // Copy_Wallets UPDATE (wallet settings changed)
  const copyWalletsUpdate = supabase.channel('copy-wallets-update')
    .on('postgres_changes', {
      event: 'UPDATE',
      schema: 'public',
      table: 'Copy_Wallets'
    }, async (payload) => {
      const walletNew = payload.new;
      const walletOld = payload.old || {};
      
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
      } else if (Math.abs(newRatio - oldRatio) > 0.001) { // Ratio changed significantly
        const msg = `⚙️ <b>Copy ratio updated</b>\n` +
          `User: <code>${escapeHtml(fmt(walletNew.user_id))}</code>\n` +
          `Wallet: <code>${escapeHtml(walletNew.wallet_address)}</code>\n` +
          `Old ratio: <b>${fmt(oldRatio * 100)}%</b> → <b>${fmt(newRatio * 100)}%</b>`;
        console.log('Copy_Wallets UPDATE (ratio change):', { old: oldRatio, new: newRatio });
        await sendTelegramMessage(msg);
      }
    })
    .subscribe((status) => {
      if (status === 'SUBSCRIBED') console.log('Subscribed to Copy_Wallets UPDATE');
    });

  // Monthly_Active_Users INSERT (monthly activity tracking)
  const monthlyActiveUsersInsert = supabase.channel('monthly-active-users-insert')
    .on('postgres_changes', {
      event: 'INSERT',
      schema: 'public',
      table: 'Monthly_Active_Users'
    }, async (payload) => {
      const record = payload.new;
      const msg = `📊 <b>Monthly active user recorded</b>\n` +
        `User: <code>${escapeHtml(fmt(record.user_id))}</code>\n` +
        `Month: <b>${record.month_number}/${record.year}</b>\n` +
        `Transactions: <b>${fmt(record.transaction_count)}</b>`;
      console.log('Monthly_Active_Users INSERT:', record);
      await sendTelegramMessage(msg);
    })
    .subscribe((status) => {
      if (status === 'SUBSCRIBED') console.log('Subscribed to Monthly_Active_Users INSERT');
    });

  console.log('Enhanced bot is running and listening for events...');
})();