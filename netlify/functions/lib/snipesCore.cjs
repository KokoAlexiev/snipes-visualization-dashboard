/**
 * Discord Message Parser and Chart Generator
 *
 * Fetches trade success messages from Discord channel and generates an interactive
 * scatter plot: one class (name + color) per WS hostname (e.g. api.csgoroll.com, router.csgoroll.com).
 */

const { Client, GatewayIntentBits } = require('discord.js');
const fs = require('fs');
const path = require('path');

// Configure via environment (Netlify: Site settings → Environment variables)
const DISCORD_BOT_TOKEN = (process.env.DISCORD_BOT_TOKEN || '').trim();
const GUILD_ID = process.env.DISCORD_GUILD_ID || '1361349930660397207';
const CREATE_TRADES_CHANNEL_ID = process.env.DISCORD_CREATE_TRADES_CHANNEL_ID || '1469438373839114300';
const TRADE_SUCCESS_CHANNEL_ID = process.env.DISCORD_TRADE_SUCCESS_CHANNEL_ID || '1483793380675686430';
const CHANNEL_ID = TRADE_SUCCESS_CHANNEL_ID;

// Blacklisted items/keywords: bots never try these, so they must NOT be counted as "missed" snipes
const BLACKLIST_ITEMS = [
  'AWP | The End', 'AK-47 | Breakthrough', 'Glock-18 | Trace Lock', 'Glock-18 | Gamma Doppler Phase 4',
  'AUG | Creep', 'Desert Eagle | The Daily Deagle', 'P2000 | Grip Tape', 'P90 | Aeolian Light',
  'FAMAS | Vendetta', 'M4A4 | Aeolian Dark', 'MAC-10 | Snow Splash', 'MP5-SD | Snow Splash',
  'R8 Revolver | Dark Chamber', 'PP-Bizon | Bizoom', 'Dual Berettas | Silver Pour', 'M249 | Sleet',
  'MP9 | Dizzy', 'MP9 | Hydra', 'MP9 | Setting Sun', 'Nova | Currents', 'P250 | Sleet',
  'SCAR-20 | Zinc', 'SSG 08 | Sans Comic', 'PP-Bizon | Modern Hunter', 'StatTrak™ Nova | Bloomstick',
  'G3SG1 | New Roots', 'AWP | Exothermic', 'Glock-18 | Brass (Factory New)',
  'CZ75-Auto | Emerald Quartz (Factory New)', 'Falchion Knife | Urban Masked (Factory New)',
  'UMP-45 | Fallout Warning (Minimal Wear)', 'P250 | Modern Hunter (Factory New)',
  'StatTrak™ Dual Berettas | Black Limba (Factory New)', 'Galil AR | Aqua Terrace',
  'Huntsman Knife | Stained', 'Galil AR | Dusk Ruins (Factory New)', 'Ursus Knife | Scorched (Factory New)',
  'P2000 | Dispatch', 'Five-SeveN | Neon Kimono', 'Bloodhound Gloves | Guerrilla (Minimal Wear)',
  'M4A4 | The Coalition (Field-Tested)'
];
const BLACKLIST_KEYWORDS = ['sticker', 'charm', 'music', 'souvenir', 'xm1014', 'negev', 'm249', 'blueberries', 'modern hunter'];

/** Parse numeric price from create-trades Price field. Strips <:rollcoin:ID> so we don't capture the ID as price. */
function parsePriceFromMarketFeed(event) {
  let raw = event.totalValue || event.price || '';
  if (!raw || typeof raw !== 'string') return null;
  raw = raw.replace(/<:\w+:\d+>\s*/g, '').trim();
  const beforeParen = raw.split('(')[0].trim();
  const m = beforeParen.match(/(\d+(?:\.\d+)?)/);
  if (!m) return null;
  const num = parseFloat(m[1]);
  return Number.isFinite(num) ? num : null;
}

/** Min/max price (USD) for "missed" — items outside this range are not counted as missed (e.g. knives > 1200). */
const MISSED_PRICE_MIN = 30;
const MISSED_PRICE_MAX = 1200;

/**
 * True if this create-trades event is blacklisted (bots don't try for it) — should not count as "missed".
 */
function isBlacklisted(event) {
  const text = ((event.itemName || '') + '\n' + (event.rawContent || '')).toLowerCase();
  const hasStatTrak = /stattrak/i.test(text);
  const hasFade = /fade/i.test(text);
  const hasSymbol = /★| knife | gloves |karambit|bayonet|butterfly|gut |flip |huntsman|falchion|bowie|ursus|navaja|stiletto|talon|classic knife/i.test(text) || text.includes('★');

  for (const item of BLACKLIST_ITEMS) {
    if (text.includes(item.toLowerCase())) return true;
  }
  for (const kw of BLACKLIST_KEYWORDS) {
    if (text.includes(kw.toLowerCase())) return true;
  }
  // ★ + StatTrak combo → blocked
  if (hasSymbol && hasStatTrak) return true;
  // Symbol + fade + stattrak → blocked
  if (hasSymbol && hasFade && hasStatTrak) return true;

  return false;
}

/**
 * Parse a Discord message to extract trade event data
 */
function parseTradeMessage(message) {
  // Build searchable content from message body and embeds (old and new format)
  let content = message.content || '';
  if (message.embeds && message.embeds.length > 0) {
    for (const embed of message.embeds) {
      if (embed.title) content += '\n' + embed.title;
      if (embed.description) content += '\n' + embed.description;
      if (embed.author && embed.author.name) content += '\n' + embed.author.name;
      if (embed.fields) {
        for (const field of embed.fields) {
          if (field.name) content += '\n' + field.name;
          if (field.value) content += '\n' + field.value;
        }
      }
      if (embed.footer && embed.footer.text) content += '\n' + embed.footer.text;
    }
  }
  const contentLower = content.toLowerCase();
  // Only actual trade success messages (e.g. "✅ Covert Trade Success"), not "Covert Sniper Started" / "started successfully"
  if (!contentLower.includes('trade success') && !contentLower.includes('covert trade success')) {
    return null;
  }

  const event = {
    timestamp: message.createdTimestamp,
    date: new Date(message.createdTimestamp),
    tradeId: null,
    wsWinner: null,
    sessionUsed: null,
    markup: null,
    totalValue: null,
    newBalance: null,
    ttfb: null,
    tls: null,
    backend: null,
    total: null,
    connection: null,
    cookie: null,
    hostVM: null,
    botName: null,
    type: null, // WS Hostname = class name (e.g. api.csgoroll.com, router.csgoroll.com)
    region: null,
    zone: null,
    wsHost: null,
    wsBackend: null,
    postHost: null,
    postBackend: null,
    postHostname: null
  };
  
  // Extract Trade ID — always capture only the alphanumeric base64 token
  let tradeIdMatch = content.match(/Trade ID:\s*([A-Za-z0-9=]+)/);
  if (tradeIdMatch) {
    event.tradeId = tradeIdMatch[1].trim();
  } else {
    const tradeIdLineMatch = content.match(/(?:Trade ID|🆔 Trade ID)\s*:?\s*\n\s*([A-Za-z0-9=]+)/);
    if (tradeIdLineMatch) {
      event.tradeId = tradeIdLineMatch[1].trim();
    }
  }
  
  // Extract VM first - try new format first, then fallback
  const vmMatch = content.match(/🖥️ VM\s+([^\n]+)/);
  if (vmMatch) {
    event.hostVM = vmMatch[1].trim();
  } else {
    // Fallback to old format
    const hostVMMatch = content.match(/Host VM\s+([^\n]+)/);
    if (hostVMMatch) {
      event.hostVM = hostVMMatch[1].trim();
    }
  }
  
  // Extract WS Host first (needed for classification)
  // Order: WS Winner Host, WS Host, WS Hostname (Rust format), Hostname, then fallback
  const wsWinnerHostMatch = content.match(/🌐 WS Winner Host\s+([^\n]+)/);
  let wsHost = null;
  if (wsWinnerHostMatch) {
    wsHost = wsWinnerHostMatch[1].trim();
  } else {
    const wsHostMatch = content.match(/🌐 WS Host\s+([^\n]+)/);
    if (wsHostMatch) {
      wsHost = wsHostMatch[1].trim();
    } else {
      const wsHostnameMatch = content.match(/🌐 WS Hostname\s+([^\n]+)/);
      if (wsHostnameMatch) {
        wsHost = wsHostnameMatch[1].trim();
      } else {
        const hostnameMatch = content.match(/🌐 Hostname\s+([^\n]+)/);
        if (hostnameMatch) {
          wsHost = hostnameMatch[1].trim();
        }
      }
    }
  }
  // Fallback: infer host from anywhere in content (old and new formats)
  if (!wsHost) {
    if (/api-trader\.csgoroll\.com/.test(content)) {
      wsHost = 'api-trader.csgoroll.com';
    } else if (/api\.csgoroll\.com/.test(content)) {
      wsHost = 'api.csgoroll.com';
    } else if (/router\.csgoroll(tr)?\.com/.test(content)) {
      wsHost = content.match(/router\.csgoroll(tr)?\.com/)[0];
    }
  }
  // Each hostname is its own class (name + color in chart)
  if (wsHost) {
    event.type = wsHost;
  } else {
    return null;
  }
  
  // Extract WS Winner
  const wsWinnerMatch = content.match(/🏆 WS Winner\s+([\s\S]*?)(?=🎯|📈|💵|💰|⏱️|🔒|⚙️|🏁|🌐|🔌|🍪|Host VM|🖥️|$)/);
  if (wsWinnerMatch) {
    // Get the full WS Winner text (may span multiple lines)
    const wsWinnerText = wsWinnerMatch[1].trim();
    // Extract lines - prefer domain over IP address
    const wsWinnerLines = wsWinnerText.split('\n').filter(line => line.trim().length > 0);
    // Look for domain first (contains .com, .gg, etc.), otherwise use first line
    const domainLine = wsWinnerLines.find(line => /\.(com|gg|net|org|io)/.test(line));
    event.wsWinner = domainLine ? domainLine.trim() : (wsWinnerLines.length > 0 ? wsWinnerLines[0].trim() : wsWinnerText);
  }
  
  // Extract Session Used / Session (also extract bot name from here)
  const sessionMatch = content.match(/🎯 Session Used\s+([^\n]+)/) || content.match(/🎯 Session\s+([^\n]+)/);
  if (sessionMatch) {
    event.sessionUsed = sessionMatch[1].trim();
    const botNameMatch = event.sessionUsed.match(/^(Bot\d+)/);
    if (botNameMatch) {
      event.botName = botNameMatch[1];
    }
  }
  
  // Extract Markup
  const markupMatch = content.match(/📈 Markup\s+([^\n]+)/);
  if (markupMatch) {
    event.markup = markupMatch[1].trim();
  }
  
  // Extract Total Value / Value
  const totalValueMatch = content.match(/💵 Total Value\s+([^\n]+)/) || content.match(/💵 Value\s+([^\n]+)/);
  if (totalValueMatch) {
    event.totalValue = totalValueMatch[1].trim();
  }
  
  // Extract New Balance
  const balanceMatch = content.match(/💰 New Balance\s+([^\n]+)/);
  if (balanceMatch) {
    event.newBalance = balanceMatch[1].trim();
  }
  
  // Extract TTFB (or legacy "Response Time")
  const ttfbMatch = content.match(/⏱️ TTFB\s+([^\n]+)/) || content.match(/⏱️ Response Time\s+([^\n]+)/);
  if (ttfbMatch) {
    event.ttfb = ttfbMatch[1].trim();
  }

  // Extract backend / total timing (HTTP3 embed fields)
  const backendMatch = content.match(/⚙️ Backend\s+([^\n]+)/);
  if (backendMatch) {
    event.backend = backendMatch[1].trim();
  }
  const totalMsMatch = content.match(/📥 Total\s+([^\n]+)/);
  if (totalMsMatch) {
    event.total = totalMsMatch[1].trim();
  }
  
  // Extract Region
  const regionMatch = content.match(/🌍 Region\s+([^\n]+)/);
  if (regionMatch) {
    event.region = regionMatch[1].trim();
  }
  
  // Extract Zone
  const zoneMatch = content.match(/📍 Zone\s+([^\n]+)/);
  if (zoneMatch) {
    event.zone = zoneMatch[1].trim();
  }
  
  // WS Host already extracted earlier for classification (WS Hostname)
  event.wsHost = wsHost;

  // Extract WS Backend (if present in message)
  const wsBackendMatch = content.match(/🌐 WS Backend\s+([^\n]+)/) || content.match(/WS Backend\s+([^\n]+)/i);
  if (wsBackendMatch) {
    event.wsBackend = wsBackendMatch[1].trim();
  }

  // Extract POST Hostname, POST Backend, and keep postHost for display (first available)
  const postWinnerHostMatch = content.match(/📮 POST Winner Host\s+([^\n]+)/);
  const postHostnameMatch = content.match(/📮 POST Host\s+([^\n]+)/) || content.match(/📮 POST Hostname\s+([^\n]+)/);
  const postBackendMatch = content.match(/📮 POST Backend\s+([^\n]+)/);
  if (postWinnerHostMatch) {
    event.postHost = event.postHostname = postWinnerHostMatch[1].trim();
  }
  if (postHostnameMatch) {
    event.postHostname = postHostnameMatch[1].trim();
    if (!event.postHost) event.postHost = event.postHostname;
  }
  if (postBackendMatch) {
    event.postBackend = postBackendMatch[1].trim();
    if (!event.postHost) event.postHost = event.postBackend;
  }
  
  // Bot name is already extracted from Session Used above
  // If not found, try to extract from message footer as fallback
  if (!event.botName) {
    const botMatch = content.match(/Bot\d+\s+\|/);
    if (botMatch) {
      event.botName = botMatch[0].replace(' |', '').trim();
    }
  }

  // Only count as snipe if we have a Trade ID (real trades have it; startup/heartbeat messages do not)
  if (!event.tradeId || event.tradeId === 'N/A') {
    return null;
  }

  return event;
}

/**
 * Build full content string from a Discord message (body + embeds)
 */
function getMessageContent(message) {
  let content = message.content || '';
  if (message.embeds && message.embeds.length > 0) {
    for (const embed of message.embeds) {
      if (embed.title) content += '\n' + embed.title;
      if (embed.description) content += '\n' + embed.description;
      if (embed.author && embed.author.name) content += '\n' + embed.author.name;
      if (embed.fields) {
        for (const field of embed.fields) {
          if (field.name) content += '\n' + field.name;
          if (field.value) content += '\n' + field.value;
        }
      }
      if (embed.footer && embed.footer.text) content += '\n' + embed.footer.text;
    }
  }
  return content;
}

/**
 * Parse a create-trades message to extract trade ID, markup, price, and display info.
 * Returns null if no trade ID. Caller should filter to markup ≤ 3%.
 */
function parseMarketFeedMessage(message) {
  const content = getMessageContent(message);
  const event = {
    timestamp: message.createdTimestamp,
    date: new Date(message.createdTimestamp),
    tradeId: null,
    markup: null,
    markupPercent: null,
    totalValue: null,
    rawContent: content,
    // Extra fields we might find in market feed
    itemName: null,
    price: null
  };

  // Trade ID: "Trade ID: VHJhZGU6... | Markup: 0.00%•..." — capture only the token
  let tradeIdMatch = content.match(/Trade ID:\s*([A-Za-z0-9=]+)/i);
  if (tradeIdMatch) {
    event.tradeId = tradeIdMatch[1].trim();
  } else {
    const tradeIdLineMatch = content.match(/(?:Trade ID|🆔 Trade ID)\s*:?\s*\n\s*([A-Za-z0-9=]+)/);
    if (tradeIdLineMatch) {
      event.tradeId = tradeIdLineMatch[1].trim();
    }
  }
  if (!event.tradeId || event.tradeId === 'N/A') return null;

  // Markup: "Markup" or "markup" followed by number and optional %
  const markupMatch = content.match(/(?:Markup|markup)\s*:?\s*([^\n]+)/i);
  if (markupMatch) {
    event.markup = markupMatch[1].trim();
    const numMatch = event.markup.match(/([-+]?\d*\.?\d+)/);
    if (numMatch) {
      event.markupPercent = parseFloat(numMatch[1]);
    }
  }
  // Price, Liquidity, Buff Price: create-trades embed fields
  if (message.embeds?.[0]?.fields) {
    const fields = message.embeds[0].fields;
    const byName = (name) => (fields.find((f) => (f.name || '').trim().toLowerCase() === name) || {}).value;
    const priceVal = byName('price');
    if (priceVal) event.totalValue = priceVal.trim();
    const liqVal = byName('liquidity');
    if (liqVal) event.liquidity = liqVal.trim();
    const buffVal = byName('buff price');
    if (buffVal) event.buffPrice = buffVal.trim();
  }
  // Item name: embed title is the item name
  const itemMatch = content.match(/(?:Item|Item Name|Name|🎮 Items?)\s*:?\s*([^\n]+)/i);
  if (itemMatch) {
    event.itemName = itemMatch[1].trim();
  }
  if (!event.itemName && message.embeds?.[0]?.title) {
    event.itemName = message.embeds[0].title.trim();
  }

  return event;
}

/**
 * Fetch messages from a Discord channel (legacy rolling-window helper, still used by main CLI).
 */
async function fetchMessages(client, daysBack = 7, channelId = CHANNEL_ID) {
  const endMs   = Date.now();
  const startMs = endMs - (daysBack * 24 * 60 * 60 * 1000);
  return fetchMessagesInRange(client, startMs, endMs, channelId);
}

/**
 * Fetch messages from a Discord channel that fall inside [rangeStartMs, rangeEndMs].
 * Pagination stops when we go older than rangeStartMs.
 * Returns an array of parsed event objects.
 */
async function fetchMessagesInRange(client, rangeStartMs, rangeEndMs, channelId = CHANNEL_ID) {
  const guild = await client.guilds.fetch(GUILD_ID);
  const channel = await guild.channels.fetch(channelId);

  if (!channel) {
    throw new Error(`Channel ${channelId} not found`);
  }

  const events = [];
  const cutoffTime = rangeStartMs;
  const parseFn = channelId === TRADE_SUCCESS_CHANNEL_ID ? parseTradeMessage : parseMarketFeedMessage;

  const rangeLabel = `${new Date(rangeStartMs).toISOString().slice(0,10)}..${new Date(rangeEndMs).toISOString().slice(0,10)}`;
  console.log(`📥 Fetching messages from channel ${channelId} (${rangeLabel})...`);

  let lastMessageId = null;
  let fetchedCount = 0;
  let hasMore = true;

  while (hasMore) {
    const options = { limit: 100 };
    if (lastMessageId) {
      options.before = lastMessageId;
    }

    const messages = await channel.messages.fetch(options);
    fetchedCount += messages.size;

    if (messages.size === 0) {
      hasMore = false;
      break;
    }

    for (const message of messages.values()) {
      if (message.createdTimestamp < cutoffTime) {
        hasMore = false;
        break;
      }
      // Skip messages newer than range end (only relevant when range ends before now)
      if (message.createdTimestamp > rangeEndMs) continue;
      const event = parseFn(message);
      if (event) {
        events.push(event);
      }
    }

    if (messages.size < 100) {
      hasMore = false;
    } else {
      lastMessageId = messages.last().id;
    }

    console.log(`   Fetched ${fetchedCount} messages, found ${events.length} events...`);
  }

  console.log(`✅ Found ${events.length} total events`);
  return events;
}

/** create-trades rows with markup ≤ 3% (same rule as fetchMarketFeedUnder3). */
function filterCreateTradesMarkupAtMost3(raw) {
  return raw.filter((e) => {
    const pct = e.markupPercent != null ? e.markupPercent : (e.markup && parseFloat(String(e.markup).replace(/[^0-9.-]/g, '')));
    return typeof pct === 'number' && !isNaN(pct) && pct <= 3;
  });
}

/**
 * Fetch create-trades messages. Returns { under3, all } — all messages for lookup enrichment, under3 for missed-snipe logic.
 */
async function fetchMarketFeedUnder3(client, daysBack = 7) {
  const raw = await fetchMessages(client, daysBack, CREATE_TRADES_CHANNEL_ID);
  const under3 = filterCreateTradesMarkupAtMost3(raw);
  console.log(`📊 create-trades: ${raw.length} messages with trade ID, ${under3.length} with markup ≤3%`);
  return { under3, all: raw };
}

/**
 * Print distribution (% of trades) for WS Hostname, WS Backend, POST Backend, POST Hostname
 */
function printDistribution(events) {
  if (events.length === 0) return;
  const total = events.length;

  function dist(fieldName, getValue) {
    const counts = {};
    events.forEach(e => {
      const v = getValue(e) || 'N/A';
      counts[v] = (counts[v] || 0) + 1;
    });
    const sorted = Object.entries(counts).sort((a, b) => b[1] - a[1]);
    return sorted.map(([value, count]) => ({ value, count, pct: (100 * count / total).toFixed(1) }));
  }

  console.log('\n📊 Distribution by WS Hostname (% of trades):');
  dist('wsHost', e => e.wsHost).forEach(({ value, count, pct }) => console.log(`   ${pct}%  ${value}  (${count})`));

  console.log('\n📊 Distribution by WS Backend (% of trades):');
  const wsBackendDist = dist('wsBackend', e => e.wsBackend);
  if (wsBackendDist.every(x => x.value === 'N/A')) {
    console.log('   (no WS Backend in messages)');
  } else {
    wsBackendDist.forEach(({ value, count, pct }) => console.log(`   ${pct}%  ${value}  (${count})`));
  }

  console.log('\n📊 Distribution by POST Backend (% of trades):');
  const postBackendDist = dist('postBackend', e => e.postBackend);
  if (postBackendDist.every(x => x.value === 'N/A')) {
    console.log('   (no POST Backend in messages)');
  } else {
    postBackendDist.forEach(({ value, count, pct }) => console.log(`   ${pct}%  ${value}  (${count})`));
  }

  console.log('\n📊 Distribution by POST Hostname (% of trades):');
  dist('postHostname', e => e.postHostname || e.postHost).forEach(({ value, count, pct }) => console.log(`   ${pct}%  ${value}  (${count})`));
  console.log('');
}

/**
 * Drop rows where the same trade ID repeats within windowMs of an earlier kept row (e.g. duplicate Discord posts seconds apart).
 * IDs can show up again after a long gap; only close-in-time repeats are treated as duplicates.
 */
const TRADE_ID_DEDUP_WINDOW_MS = 12 * 60 * 60 * 1000; // 12 hours

function dedupeEventsByTradeIdWindow(rows, getTimestamp, getTradeId, windowMs) {
  if (!rows || rows.length === 0) return [];
  const sorted = [...rows].sort((a, b) => getTimestamp(a) - getTimestamp(b));
  const out = [];
  const lastKeptTime = new Map();
  for (const row of sorted) {
    const tm = getTimestamp(row);
    if (!Number.isFinite(tm)) continue;
    const id = String(getTradeId(row) || '').trim();
    if (!id) {
      out.push(row);
      continue;
    }
    const prev = lastKeptTime.get(id);
    if (prev != null && tm - prev < windowMs) continue;
    lastKeptTime.set(id, tm);
    out.push(row);
  }
  return out;
}

/**
 * Generate HTML with interactive Plotly chart
 * @param {Array} events - trade-success (our buys)
 * @param {number} daysBack
 * @param {Array} missedSnipes - create-trades ≤3% that we did not get (optional)
 * @param {{ startTime?: number, endTime?: number }} options - explicit time range for x-axis (ms); avoids Plotly snapping to midnight
 */
function generateHTML(events, daysBack, missedSnipes = [], options = {}) {
  const endTime = options.endTime != null ? options.endTime : Date.now();
  const startTime = options.startTime != null ? options.startTime : (endTime - (daysBack * 24 * 60 * 60 * 1000));
  const channelBUnder3 = options.channelBUnder3 || [];
  const allCreateTrades = options.allCreateTrades || channelBUnder3;
  // Plotly date axis needs range in milliseconds and autorange: false to respect it
  const xAxisRange = [startTime, endTime];
  // Sort events by timestamp
  events.sort((a, b) => a.timestamp - b.timestamp);
  (missedSnipes || []).sort((a, b) => a.timestamp - b.timestamp);

  // Create-trades lookup by trade ID — uses ALL create-trades (not just ≤3%) so Got trade data is always enriched
  const tradeIdToCreateTrade = {};
  allCreateTrades.forEach((e) => {
    const tid = (e.tradeId || '').trim();
    if (tid) tradeIdToCreateTrade[tid] = e;
  });
  const trimRollcoin = (s) => (s || '').replace(/<:\w+:\d+>\s*/g, '').replace(/:rollcoin:\s*/gi, '').trim();
  const getExternalPrice = (totalValue) => {
    if (!totalValue || typeof totalValue !== 'string') return '—';
    const m = totalValue.match(/\(([^)]+)\)/);
    return m ? m[1].trim() : '—';
  };
  const getPriceNumber = (e) => {
    const n = parsePriceFromMarketFeed(e);
    return n != null ? String(n) : '—';
  };
  
  // Each WS hostname is its own class (name + color)
  const hostnames = [...new Set(events.map(e => e.type).filter(Boolean))].sort();
  const PALETTE = ['#ff6b6b', '#4ecdc4', '#45b7d1', '#96ceb4', '#ffeaa7', '#fd79a8', '#a29bfe', '#6c5ce7', '#00b894', '#e17055', '#0984e3', '#fdcb6e', '#e84393', '#00cec9'];
  const hostnameToColor = {};
  hostnames.forEach((h, i) => { hostnameToColor[h] = PALETTE[i % PALETTE.length]; });
  
  const eventsByHostname = {};
  hostnames.forEach(h => { eventsByHostname[h] = events.filter(e => e.type === h); });
  
  // Senko machine breakdown (any event with senko in host VM)
  const senkoMachineCounts = {};
  events.forEach(event => {
    const hostVM = event.hostVM || 'N/A';
    if (hostVM.includes('senko.network')) {
      senkoMachineCounts[hostVM] = (senkoMachineCounts[hostVM] || 0) + 1;
    }
  });
  const sortedSenkoMachines = Object.entries(senkoMachineCounts)
    .sort((a, b) => b[1] - a[1]);

  // Calculate breakdown statistics
  const regionZoneCounts = {}; // Combined Region+Zone counts
  const wsHostCounts = {};
  const hostVMToEndpoint = {}; // Host VM -> Endpoint mapping with counts
  const endpointToRegionZone = {}; // Endpoint -> Region+Zone mapping with counts

  events.forEach(event => {
    // Count by region+zone (combined)
    const region = event.region || 'N/A';
    const zone = event.zone || 'N/A';
    const regionZone = zone !== 'N/A' ? `${region} (Zone ${zone})` : region;
    regionZoneCounts[regionZone] = (regionZoneCounts[regionZone] || 0) + 1;

    // Count by WS Host (Endpoint)
    const wsHost = event.wsHost || 'N/A';
    wsHostCounts[wsHost] = (wsHostCounts[wsHost] || 0) + 1;

    // Map Host VM to Endpoint (which machine uses which endpoint)
    const hostVM = event.hostVM || 'N/A';
    if (!hostVMToEndpoint[hostVM]) {
      hostVMToEndpoint[hostVM] = {};
    }
    hostVMToEndpoint[hostVM][wsHost] = (hostVMToEndpoint[hostVM][wsHost] || 0) + 1;

    // Map Endpoint to Region+Zone (which endpoint in which region and zone)
    if (!endpointToRegionZone[wsHost]) {
      endpointToRegionZone[wsHost] = {};
    }
    endpointToRegionZone[wsHost][regionZone] = (endpointToRegionZone[wsHost][regionZone] || 0) + 1;
  });

  // Sort counts by value (descending)
  const sortedRegionZones = Object.entries(regionZoneCounts)
    .sort((a, b) => b[1] - a[1]);
  const sortedWsHosts = Object.entries(wsHostCounts)
    .sort((a, b) => b[1] - a[1]);

  // Convert Host VM -> Endpoint mapping to sorted array format
  const sortedHostVMToEndpoint = Object.entries(hostVMToEndpoint)
    .map(([hostVM, endpoints]) => {
      const sortedEndpoints = Object.entries(endpoints)
        .sort((a, b) => b[1] - a[1]);
      return [hostVM, sortedEndpoints];
    })
    .sort((a, b) => {
      // Sort by total count for that host VM
      const totalA = a[1].reduce((sum, [, count]) => sum + count, 0);
      const totalB = b[1].reduce((sum, [, count]) => sum + count, 0);
      return totalB - totalA;
    });

  // Convert Endpoint -> Region+Zone mapping to sorted array format
  const sortedEndpointToRegionZone = Object.entries(endpointToRegionZone)
    .map(([endpoint, regionZones]) => {
      const sortedRegionZones = Object.entries(regionZones)
        .sort((a, b) => b[1] - a[1]);
      return [endpoint, sortedRegionZones];
    })
    .sort((a, b) => {
      // Sort by total count for that endpoint
      const totalA = a[1].reduce((sum, [, count]) => sum + count, 0);
      const totalB = b[1].reduce((sum, [, count]) => sum + count, 0);
      return totalB - totalA;
    });
  
  // Helper function to parse price string to number
  function parsePrice(priceStr) {
    if (!priceStr || priceStr === 'N/A') return 0;
    // Remove $ and parse to float
    const cleaned = priceStr.replace(/[^0-9.]/g, '');
    return parseFloat(cleaned) || 0;
  }
  
  const parseMarkup = (markupStr) => {
    if (!markupStr || markupStr === 'N/A') return 0;
    const cleaned = markupStr.replace(/[^0-9.-]/g, '');
    return parseFloat(cleaned) || 0;
  };

  /** First finite number in a string (e.g. "384.69ms", "N/A") */
  function parseMsField(str) {
    if (!str || str === 'N/A') return null;
    const m = String(str).match(/([-+]?\d*\.?\d+)/);
    if (!m) return null;
    const n = parseFloat(m[1]);
    return Number.isFinite(n) ? n : null;
  }

  function sortedFiniteNums(arr) {
    return arr.filter((x) => typeof x === 'number' && Number.isFinite(x)).sort((a, b) => a - b);
  }

  function medianOfSorted(sorted) {
    const n = sorted.length;
    if (n === 0) return null;
    const mid = Math.floor(n / 2);
    return n % 2 === 1 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
  }

  /** Median, Q1, Q3 (Tukey hinges), n — for table */
  function quartileStats(values) {
    const s = sortedFiniteNums(values);
    const n = s.length;
    if (n === 0) return { median: null, q1: null, q3: null, n: 0 };
    if (n === 1) {
      const v = s[0];
      return { median: v, q1: v, q3: v, n: 1 };
    }
    const med = medianOfSorted(s);
    const half = Math.floor(n / 2);
    const lower = n % 2 === 1 ? s.slice(0, half) : s.slice(0, half);
    const upper = n % 2 === 1 ? s.slice(half + 1) : s.slice(half);
    const q1 = medianOfSorted(lower);
    const q3 = medianOfSorted(upper);
    return { median: med, q1, q3, n };
  }

  function fmtStat(st, digits = 2) {
    if (!st || st.n === 0) return '—';
    const d = (x) => (x == null || !Number.isFinite(x) ? '—' : Number(x).toFixed(digits));
    return `${d(st.median)} (${d(st.q1)}–${d(st.q3)}), n=${st.n}`;
  }

  // Per-hostname data for time series and y-axis
  const timesByHostname = {};
  const pricesByHostname = {};
  const markupsByHostname = {};
  const distTtfbMsByHostname = {};
  const distBackendMsByHostname = {};
  const distTotalMsByHostname = {};
  hostnames.forEach(h => {
    const evs = eventsByHostname[h];
    timesByHostname[h] = evs.map(e => new Date(e.timestamp));
    pricesByHostname[h] = evs.map(e => parsePrice(e.totalValue));
    markupsByHostname[h] = evs.map(e => parseMarkup(e.markup));
    distTtfbMsByHostname[h] = evs.map(e => parseMsField(e.ttfb)).filter((x) => x != null);
    distBackendMsByHostname[h] = evs.map(e => parseMsField(e.backend)).filter((x) => x != null);
    distTotalMsByHostname[h] = evs.map(e => parseMsField(e.total)).filter((x) => x != null);
  });

  const distTableRows = hostnames.map((h) => {
    const t = quartileStats(distTtfbMsByHostname[h]);
    const b = quartileStats(distBackendMsByHostname[h]);
    const tot = quartileStats(distTotalMsByHostname[h]);
    return { h, ttfb: t, backend: b, total: tot };
  });
  
  // Format timestamp for display
  function formatTimestamp(timestamp) {
    const date = new Date(timestamp);
    return date.toLocaleString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: true
    });
  }
  
  // Create hover text for each event
  const createHoverText = (e, isPriceMode) => {
    const primaryValue = isPriceMode ?
      `Total Value: ${e.totalValue || 'N/A'}` :
      `Markup: ${e.markup || 'N/A'}`;
    const secondaryValue = isPriceMode ?
      `Markup: ${e.markup || 'N/A'}` :
      `Total Value: ${e.totalValue || 'N/A'}`;

    const regionZone = e.zone && e.zone !== 'N/A' ? 
      `${e.region || 'N/A'} (Zone ${e.zone})` : 
      (e.region || 'N/A');
    
    return `Time: ${formatTimestamp(e.timestamp)}<br>` +
      `WS Winner: ${e.wsWinner || 'N/A'}<br>` +
      `Session Used: ${e.sessionUsed || 'N/A'}<br>` +
      `${primaryValue}<br>` +
      `${secondaryValue}<br>` +
      `Region + Zone: ${regionZone}<br>` +
      `WS Host: ${e.wsHost || 'N/A'}<br>` +
      `POST Host: ${e.postHost || 'N/A'}<br>` +
      `Trade ID: ${e.tradeId || 'N/A'}<br>` +
      `Host VM: ${e.hostVM || 'N/A'}<br>` +
      `Bot: ${e.botName || 'N/A'}`;
  };

  const hoverTextByHostname = {};
  hostnames.forEach(h => {
    hoverTextByHostname[h] = eventsByHostname[h].map(e => createHoverText(e, true));
  });
  
  function addJitterToValues(values) {
    if (values.length === 0) return values;
    const maxValue = Math.max(...values);
    const minValue = Math.min(...values);
    const range = maxValue - minValue;
    const jitterPercent = 0.02;
    const jitterAmount = Math.max(range * jitterPercent, 0.1);
    return values.map(value => {
      const jitter = (Math.random() - 0.5) * 2 * jitterAmount;
      return value + jitter;
    });
  }

  const priceYByHostname = {};
  const markupYByHostname = {};
  hostnames.forEach(h => {
    priceYByHostname[h] = addJitterToValues(pricesByHostname[h]);
    markupYByHostname[h] = addJitterToValues(markupsByHostname[h]);
  });

  // Y-axis = class bands (each hostname gets a chunk of y). Dot size = value (30–2000), 1000 = 2× min size
  const SIZE_VALUE_MIN = 30;
  const SIZE_VALUE_MAX = 2000;
  const SIZE_MIN = 6;
  const SIZE_AT_1000 = 12; // 2× min
  const SIZE_MAX = 20;
  function valueToSize(value) {
    const v = Math.max(SIZE_VALUE_MIN, Math.min(SIZE_VALUE_MAX, value));
    if (v <= SIZE_VALUE_MIN) return SIZE_MIN;
    if (v >= SIZE_VALUE_MAX) return SIZE_MAX;
    if (v <= 1000) {
      return SIZE_MIN + (v - SIZE_VALUE_MIN) / (1000 - SIZE_VALUE_MIN) * (SIZE_AT_1000 - SIZE_MIN);
    }
    return SIZE_AT_1000 + (v - 1000) / (SIZE_VALUE_MAX - 1000) * (SIZE_MAX - SIZE_AT_1000);
  }
  const classYByHostname = {};
  const sizeByHostname = {};
  hostnames.forEach((h, classIndex) => {
    const evs = eventsByHostname[h];
    const bandCenter = classIndex + 0.5;
    const jitterRange = 0.4;
    classYByHostname[h] = evs.map(() => bandCenter + (Math.random() - 0.5) * 2 * jitterRange);
    sizeByHostname[h] = evs.map((e) => valueToSize(parsePrice(e.totalValue)));
  });

  // --- Got vs Missed scatter + rolling 1h % line (trade-success vs create-trades ≤3%) ---
  const HOUR_MS = 60 * 60 * 1000;
  const JITTER_Y = 0.45; // heavy jitter on y (0 or 1) so dots spread
  const eventsGotMissedDeduped = dedupeEventsByTradeIdWindow(
    events,
    (e) => e.timestamp,
    (e) => e.tradeId,
    TRADE_ID_DEDUP_WINDOW_MS
  );
  const missedSnipesDeduped = dedupeEventsByTradeIdWindow(
    missedSnipes || [],
    (e) => e.timestamp,
    (e) => e.tradeId,
    TRADE_ID_DEDUP_WINDOW_MS
  );
  const gotPoints = eventsGotMissedDeduped.map((e) => {
    const p = {
      t: e.timestamp,
      date: new Date(e.timestamp),
      got: 1,
      tradeId: e.tradeId,
      hover: createHoverText(e, true)
    };
    p.y = 60 + Math.random() * 40;
    return p;
  });
  const missedPoints = missedSnipesDeduped.map((e) => {
    const p = {
      t: e.timestamp,
      date: new Date(e.timestamp),
      got: 0,
      tradeId: e.tradeId,
      markup: e.markup || (e.markupPercent != null ? e.markupPercent + '%' : 'N/A'),
      totalValue: e.totalValue || 'N/A',
      itemName: e.itemName || 'N/A',
      hover: `Time: ${formatTimestamp(e.timestamp)}<br>Trade ID: ${e.tradeId || 'N/A'}<br>Markup: ${e.markup || 'N/A'}<br>Value: ${e.totalValue || 'N/A'}<br>Item: ${e.itemName || 'N/A'}`
    };
    p.y = Math.random() * 40;
    return p;
  });
  const allGotMissedTimes = [...gotPoints.map((p) => p.t), ...missedPoints.map((p) => p.t)].filter(Boolean);
  const tMin = allGotMissedTimes.length ? Math.min(...allGotMissedTimes) : Date.now();
  const tMax = allGotMissedTimes.length ? Math.max(...allGotMissedTimes) : Date.now();
  // Rolling 1h % at each 15-min grid point
  const gridStep = 15 * 60 * 1000;
  const rollingPctX = [];
  const rollingPctY = [];
  for (let t = tMin; t <= tMax; t += gridStep) {
    const windowStart = t - HOUR_MS;
    const gotInWindow = gotPoints.filter((p) => p.t >= windowStart && p.t <= t).length;
    const missedInWindow = missedPoints.filter((p) => p.t >= windowStart && p.t <= t).length;
    const total = gotInWindow + missedInWindow;
    if (total > 0) {
      rollingPctX.push(new Date(t));
      rollingPctY.push((100 * gotInWindow) / total);
    }
  }
  // Smooth the line (3-point moving average)
  const smoothedRollingPctY = [];
  for (let i = 0; i < rollingPctY.length; i++) {
    const prev = i > 0 ? rollingPctY[i - 1] : rollingPctY[i];
    const next = i < rollingPctY.length - 1 ? rollingPctY[i + 1] : rollingPctY[i];
    smoothedRollingPctY.push((prev + rollingPctY[i] + next) / 3);
  }

  // 1-hour bins: snipe count per endpoint (hostname) over time — for line chart above Got vs Missed
  const endpointLineBinTimes = [];
  const endpointLineCountsByHostname = {};
  for (let t = startTime; t < endTime; t += HOUR_MS) {
    const binEnd = t + HOUR_MS;
    endpointLineBinTimes.push(new Date(t));
    hostnames.forEach((h) => {
      if (!endpointLineCountsByHostname[h]) endpointLineCountsByHostname[h] = [];
      const count = events.filter((e) => e.type === h && e.timestamp >= t && e.timestamp < binEnd).length;
      endpointLineCountsByHostname[h].push(count);
    });
  }

  // 1-hour bins for grouped bar + got line (alternative view: counts + efficiency)
  const BIN_STEP_MS = 60 * 60 * 1000;
  const stackedBarBinTimes = [];
  const stackedBarGotCounts = [];
  const stackedBarMissedCounts = [];
  const stackedBarWinRate = [];
  const stackedBarBinDetails = []; // per-bin rows for click-to-show table: { got: [...], missed: [...] }
  for (let t = tMin; t <= tMax; t += BIN_STEP_MS) {
    const binEnd = t + BIN_STEP_MS;
    const gotInBinPoints = gotPoints.filter((p) => p.t >= t && p.t < binEnd);
    const missedInBinPoints = missedPoints.filter((p) => p.t >= t && p.t < binEnd);
    const gotInBin = gotInBinPoints.length;
    const missedInBin = missedInBinPoints.length;
    const total = gotInBin + missedInBin;
    stackedBarBinTimes.push(new Date(t));
    stackedBarGotCounts.push(gotInBin);
    stackedBarMissedCounts.push(missedInBin);
    stackedBarWinRate.push(total > 0 ? (100 * gotInBin) / total : null);
    // Detail rows: data from create-trades (by trade ID); got/missed = type only
    const gotInBinEvents = eventsGotMissedDeduped.filter((e) => e.timestamp >= t && e.timestamp < binEnd);
    const missedInBinEvents = missedSnipesDeduped.filter((e) => e.timestamp >= t && e.timestamp < binEnd);
    const rowFromEvent = (e, type) => {
      const ct = tradeIdToCreateTrade[(e.tradeId || '').trim()] || e;
      const totalValue = ct.totalValue || '';
      return {
        type,
        dateTime: formatTimestamp(e.timestamp),
        tradeId: e.tradeId || 'N/A',
        markup: ct.markup || (ct.markupPercent != null ? ct.markupPercent + '%' : 'N/A'),
        liquidity: ct.liquidity || '—',
        buffPrice: trimRollcoin(ct.buffPrice || '—'),
        price: getPriceNumber(ct),
        externalPrice: getExternalPrice(totalValue)
      };
    };
    stackedBarBinDetails.push({
      binLabel: formatTimestamp(t) + ' – ' + formatTimestamp(binEnd - 1),
      got: gotInBinEvents.map((e) => rowFromEvent(e, 'got')),
      missed: missedInBinEvents.map((e) => rowFromEvent(e, 'missed'))
    });
  }

  // Volume of snipes (create-trades, <=3%): 1-hour bins, then smooth. Also snipes we got per hour.
  const volumeBinStep = HOUR_MS;
  const volumeBinTimes = [];
  const volumeBinCounts = [];
  const volumeBinGotCounts = [];
  for (let t = startTime; t < endTime; t += volumeBinStep) {
    const binEnd = t + volumeBinStep;
    volumeBinTimes.push(new Date(t));
    volumeBinCounts.push(channelBUnder3.filter((e) => e.timestamp >= t && e.timestamp < binEnd).length);
    volumeBinGotCounts.push(events.filter((e) => e.timestamp >= t && e.timestamp < binEnd).length);
  }
  const smoothedVolumeCounts = [];
  const smoothedGotCounts = [];
  for (let i = 0; i < volumeBinCounts.length; i++) {
    const prev = i > 0 ? volumeBinCounts[i - 1] : volumeBinCounts[i];
    const next = i < volumeBinCounts.length - 1 ? volumeBinCounts[i + 1] : volumeBinCounts[i];
    smoothedVolumeCounts.push((prev + volumeBinCounts[i] + next) / 3);
    const gPrev = i > 0 ? volumeBinGotCounts[i - 1] : volumeBinGotCounts[i];
    const gNext = i < volumeBinGotCounts.length - 1 ? volumeBinGotCounts[i + 1] : volumeBinGotCounts[i];
    smoothedGotCounts.push((gPrev + volumeBinGotCounts[i] + gNext) / 3);
  }

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Snipes by Hostname</title>
    <script src="https://cdn.plot.ly/plotly-2.26.0.min.js"></script>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 10px;
            padding: 30px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
        }
        h1 {
            color: #333;
            margin-top: 0;
            text-align: center;
            font-size: 2.5em;
        }
        .stats {
            display: flex;
            justify-content: space-around;
            margin: 20px 0;
            padding: 20px;
            background: #f8f9fa;
            border-radius: 8px;
        }
        .stat-box {
            text-align: center;
        }
        .stat-value {
            font-size: 2em;
            font-weight: bold;
            color: #667eea;
        }
        .stat-label {
            color: #666;
            margin-top: 5px;
        }
        .breakdown-stats {
            margin: 30px 0;
            padding: 20px;
            background: #f8f9fa;
            border-radius: 8px;
        }
        .breakdown-title {
            font-size: 1.3em;
            font-weight: bold;
            color: #333;
            margin-bottom: 15px;
            text-align: center;
        }
        .breakdown-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-top: 15px;
        }
        .breakdown-section {
            background: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .breakdown-section-title {
            font-size: 1.1em;
            font-weight: bold;
            color: #667eea;
            margin-bottom: 10px;
            border-bottom: 2px solid #667eea;
            padding-bottom: 5px;
        }
        .breakdown-item {
            display: flex;
            justify-content: space-between;
            padding: 5px 0;
            border-bottom: 1px solid #eee;
        }
        .breakdown-item:last-child {
            border-bottom: none;
        }
        .breakdown-item-label {
            color: #666;
            font-size: 0.9em;
        }
        .breakdown-item-value {
            font-weight: bold;
            color: #333;
        }
        #chart {
            width: 100%;
            height: 600px;
            margin-top: 20px;
        }
        .info {
            margin-top: 20px;
            padding: 15px;
            background: #e3f2fd;
            border-left: 4px solid #2196f3;
            border-radius: 4px;
        }
        .table-container {
            margin-top: 40px;
            overflow-x: auto;
        }
        .table-header {
            font-size: 1.5em;
            font-weight: bold;
            margin-bottom: 15px;
            color: #333;
        }
        .search-box {
            margin-bottom: 15px;
            padding: 10px;
            width: 100%;
            max-width: 400px;
            border: 2px solid #ddd;
            border-radius: 5px;
            font-size: 14px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            background: white;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        th {
            background: #667eea;
            color: white;
            padding: 12px;
            text-align: left;
            cursor: pointer;
            user-select: none;
            position: sticky;
            top: 0;
        }
        th:hover {
            background: #5568d3;
        }
        th.sortable::after {
            content: ' ↕';
            opacity: 0.5;
        }
        th.sort-asc::after {
            content: ' ↑';
            opacity: 1;
        }
        th.sort-desc::after {
            content: ' ↓';
            opacity: 1;
        }
        td {
            padding: 10px 12px;
            border-bottom: 1px solid #eee;
        }
        tr:hover {
            background: #f5f5f5;
        }
        .type-host {
            font-weight: bold;
        }
        .cluster-controls {
            margin: 20px 0;
            text-align: center;
        }
        .cluster-btn {
            background: #667eea;
            color: white;
            border: none;
            padding: 12px 24px;
            font-size: 16px;
            border-radius: 5px;
            cursor: pointer;
            transition: background 0.3s;
        }
        .cluster-btn:hover {
            background: #5568d3;
        }
        .cluster-btn.active {
            background: #4caf50;
        }
        .cluster-info {
            margin-top: 10px;
            padding: 10px;
            background: #f0f0f0;
            border-radius: 5px;
            font-size: 14px;
            color: #666;
        }
        .filter-controls {
            margin: 20px 0;
            display: flex;
            gap: 15px;
            flex-wrap: wrap;
            align-items: center;
            justify-content: center;
        }
        .filter-group {
            display: flex;
            flex-direction: column;
            gap: 5px;
        }
        .filter-label {
            font-size: 14px;
            font-weight: bold;
            color: #333;
        }
        .filter-select {
            padding: 8px 12px;
            font-size: 14px;
            border: 2px solid #ddd;
            border-radius: 5px;
            background: white;
            cursor: pointer;
            min-width: 200px;
            transition: border-color 0.3s;
        }
        .filter-select:hover {
            border-color: #667eea;
        }
        .filter-select:focus {
            outline: none;
            border-color: #667eea;
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🎯 Snipes by Hostname</h1>
        
        <div class="stats">
            ${hostnames.map(h => `
            <div class="stat-box">
                <div class="stat-value" style="color: ${hostnameToColor[h]};">${eventsByHostname[h].length}</div>
                <div class="stat-label">${h}</div>
            </div>
            `).join('')}
            <div class="stat-box">
                <div class="stat-value" style="color: #95a5a6;">${events.length}</div>
                <div class="stat-label">Total Events</div>
            </div>
            <div class="stat-box">
                <div class="stat-value" style="color: #f39c12;">${daysBack}</div>
                <div class="stat-label">Days Analyzed</div>
            </div>
        </div>
        
        <div class="breakdown-stats">
            <div class="breakdown-title">📊 Breakdown Statistics</div>
            <div class="breakdown-grid">
                <div class="breakdown-section">
                    <div class="breakdown-section-title">🌍 Region + Zone</div>
                    ${sortedRegionZones.map(([regionZone, count]) => 
                      `<div class="breakdown-item">
                          <span class="breakdown-item-label">${regionZone}</span>
                          <span class="breakdown-item-value">${count}</span>
                       </div>`
                    ).join('')}
                </div>
                <div class="breakdown-section">
                    <div class="breakdown-section-title">🌐 Endpoint (WS Host)</div>
                    ${sortedWsHosts.map(([host, count]) => 
                      `<div class="breakdown-item">
                          <span class="breakdown-item-label">${host}</span>
                          <span class="breakdown-item-value">${count}</span>
                       </div>`
                    ).join('')}
                </div>
            </div>
        </div>
        
        <div class="breakdown-stats">
            <div class="breakdown-title">🖥️ Host VM → Endpoint Mapping</div>
            <div class="breakdown-grid">
                ${sortedHostVMToEndpoint.map(([hostVM, endpoints]) => {
                  const totalCount = endpoints.reduce((sum, [, count]) => sum + count, 0);
                  return `
                    <div class="breakdown-section">
                        <div class="breakdown-section-title">${hostVM} <span style="color: #999; font-size: 0.8em;">(Total: ${totalCount})</span></div>
                        ${endpoints.map(([endpoint, count]) => 
                          `<div class="breakdown-item">
                              <span class="breakdown-item-label">${endpoint}</span>
                              <span class="breakdown-item-value">${count}</span>
                           </div>`
                        ).join('')}
                    </div>
                  `;
                }).join('')}
            </div>
        </div>
        
        <div class="breakdown-stats">
            <div class="breakdown-title">🌍 Endpoint → Region + Zone Mapping</div>
            <div class="breakdown-grid">
                ${sortedEndpointToRegionZone.map(([endpoint, regionZones]) => {
                  const totalCount = regionZones.reduce((sum, [, count]) => sum + count, 0);
                  return `
                    <div class="breakdown-section">
                        <div class="breakdown-section-title">${endpoint} <span style="color: #999; font-size: 0.8em;">(Total: ${totalCount})</span></div>
                        ${regionZones.map(([regionZone, count]) => 
                          `<div class="breakdown-item">
                              <span class="breakdown-item-label">${regionZone}</span>
                              <span class="breakdown-item-value">${count}</span>
                           </div>`
                        ).join('')}
                    </div>
                  `;
                }).join('')}
            </div>
        </div>
        
        <div class="breakdown-stats">
            <div class="breakdown-title">🖥️ Senko Machine Breakdown</div>
            <div class="breakdown-grid">
                <div class="breakdown-section">
                    <div class="breakdown-section-title">Senko Machines</div>
                    ${sortedSenkoMachines.length > 0 ? sortedSenkoMachines.map(([machine, count]) => 
                      `<div class="breakdown-item">
                          <span class="breakdown-item-label">${machine}</span>
                          <span class="breakdown-item-value">${count}</span>
                       </div>`
                    ).join('') : '<div class="breakdown-item"><span class="breakdown-item-label">No senko machines found</span></div>'}
                </div>
            </div>
        </div>
        
        <div class="cluster-controls">
            <button class="cluster-btn" id="clusterBtn">🔍 Enable Cluster Finder</button>
            <button class="cluster-btn" id="yAxisBtn" style="margin-left: 15px;">📊 Show Markup</button>
            <button class="cluster-btn" id="kdeBtn" style="margin-left: 15px;">📈 Show KDE Plot</button>
            <div class="cluster-info" id="clusterInfo" style="display: none;"></div>
        </div>
        
        <div class="filter-controls">
            <div class="filter-group">
                <label class="filter-label">🌍 Region</label>
                <select class="filter-select" id="regionFilter">
                    <option value="all">All Regions</option>
                </select>
            </div>
            <div class="filter-group">
                <label class="filter-label">🌐 Endpoint</label>
                <select class="filter-select" id="wsHostFilter">
                    <option value="all">All Endpoints</option>
                </select>
            </div>
        </div>
        
        <div id="chart"></div>
        
        <div class="section-title" style="margin-top: 32px; font-size: 1.4em; font-weight: bold; color: #333;">📐 Distributions by hostname</div>
        <div class="info" style="margin-top: 8px;">
            Violin plots (with embedded box) show timing fields per WS hostname. Table: <strong>median</strong> and <strong>Q1–Q3</strong> (quartiles); <em>n</em> = non-missing values.
        </div>
        <div class="table-container" style="margin-top: 16px;">
            <table id="distStatsTable" style="font-size: 14px;">
                <thead>
                    <tr>
                        <th>WS hostname</th>
                        <th>TTFB (ms)</th>
                        <th>Backend (ms)</th>
                        <th>Total (ms)</th>
                    </tr>
                </thead>
                <tbody>
                    ${distTableRows.map((row) => `
                    <tr>
                        <td class="type-host">${row.h}</td>
                        <td>${fmtStat(row.ttfb, 1)}</td>
                        <td>${fmtStat(row.backend, 1)}</td>
                        <td>${fmtStat(row.total, 1)}</td>
                    </tr>`).join('')}
                </tbody>
            </table>
        </div>
        <p id="distChartsEmpty" style="display: none; color: #666; margin-top: 12px;">No numeric timing fields in this window — violins hidden.</p>
        <div class="subsection-dist" style="margin-top: 24px;">
            <div class="table-header" style="font-size: 1.15em;">TTFB (ms)</div>
            <div id="chartDistTtfb" class="dist-violin-chart" style="width: 100%; height: 400px;"></div>
        </div>
        <div class="subsection-dist" style="margin-top: 16px;">
            <div class="table-header" style="font-size: 1.15em;">Backend time (ms)</div>
            <div id="chartDistBackend" class="dist-violin-chart" style="width: 100%; height: 400px;"></div>
        </div>
        <div class="subsection-dist" style="margin-top: 16px;">
            <div class="table-header" style="font-size: 1.15em;">Total time (ms)</div>
            <div id="chartDistTotal" class="dist-violin-chart" style="width: 100%; height: 400px;"></div>
        </div>
        
        <div class="info">
            <strong>📊 Chart Information:</strong><br>
            • X-axis: Time of day<br>
            • Y-axis: Class (hostname) — each class in its own band<br>
            • Dot size = snipe value (min 30, max 2000; 1000 = 2× min size)<br>
            • Use the filter dropdowns above to filter by Region or Endpoint (WS Host)<br>
            • Hover over data points to see full details<br>
            • Data from the last ${daysBack} days
        </div>
        
        <div class="section-title" style="margin-top: 40px; font-size: 1.5em; font-weight: bold; color: #333;">📈 Snipe count by endpoint over time (1h bins)</div>
        <div class="info" style="margin-top: 8px;">
            One line per hostname/endpoint. X: datetime, Y: snipe count per hour.
        </div>
        <div id="chartEndpointLines" style="width: 100%; height: 450px; margin-top: 10px;"></div>
        
        <div class="section-title" style="margin-top: 32px; font-size: 1.5em; font-weight: bold; color: #333;">📈 Snipes We Got vs We Missed (trade-success vs create-trades ≤3%)</div>
        <div class="info" style="margin-top: 8px;">
            Y-axis: % of snipes we got (dots: green = got, red = missed, with jitter). Line: smoothed % of snipes we are taking (rolling 1h).
            Same <strong>12h trade-ID dedupe</strong> as the hourly bar chart (repeated IDs within 12h count once).
        </div>
        <div id="chartGotMissed" style="width: 100%; height: 500px; margin-top: 10px;"></div>
        
        <div class="section-title" style="margin-top: 32px; font-size: 1.3em; font-weight: bold; color: #333;">📊 Counts per 1 hour (grouped bars + % got line)</div>
        <div class="info" style="margin-top: 8px;">
            <strong>Left Y-axis:</strong> counts per hour — green = got, red = missed (grouped bars). <strong>Right Y-axis (0–100%):</strong> line = got ÷ (got + missed) for that hour (no activity → gap in the line).
            <strong>Trade IDs</strong> deduped: if the same ID appears again within <strong>12 hours</strong> of a previous message, only the first is counted (duplicate notifications seconds apart). The same ID after a longer gap is kept as a separate row.
        </div>
        <div id="chartGotMissedStacked" style="width: 100%; height: 450px; margin-top: 10px;"></div>
        <div id="barChartTableSection" style="display: none; margin-top: 20px;">
            <div class="table-header" style="font-size: 1.2em;">📋 Snipes in selected hour</div>
            <p id="barChartTableBinLabel" style="color: #666; margin-bottom: 10px;"></p>
            <table id="barChartDetailTable" style="width: 100%; border-collapse: collapse; font-size: 13px;">
                <thead>
                    <tr style="background: #667eea; color: white;">
                        <th style="padding: 8px; text-align: left;">Type</th>
                        <th style="padding: 8px; text-align: left;">Date & time</th>
                        <th style="padding: 8px; text-align: left;">Trade ID</th>
                        <th style="padding: 8px; text-align: left;">Markup</th>
                        <th style="padding: 8px; text-align: left;">Liquidity</th>
                        <th style="padding: 8px; text-align: left;">Buff price</th>
                        <th style="padding: 8px; text-align: left;">Price</th>
                        <th style="padding: 8px; text-align: left;">External price</th>
                    </tr>
                </thead>
                <tbody id="barChartDetailTableBody"></tbody>
            </table>
        </div>
        
        <div class="section-title" style="margin-top: 32px; font-size: 1.3em; font-weight: bold; color: #333;">📊 Volume (create-trades ≤3%) + Snipes we got (1h bins, smoothed)</div>
        <div class="info" style="margin-top: 8px;">
            Blue: items ≤3% in create-trades per hour. Green: snipes we got per hour. Same 3-point smoothing.
        </div>
        <div id="chartVolume" style="width: 100%; height: 400px; margin-top: 10px;"></div>
        
        <div class="table-container">
            <div class="table-header">📋 All Trades</div>
            <input type="text" class="search-box" id="searchBox" placeholder="Search trades by bot, trade ID, host VM, etc...">
            <table id="tradesTable">
                <thead>
                    <tr>
                        <th class="sortable" data-sort="time">Time</th>
                        <th class="sortable" data-sort="type">Type</th>
                        <th class="sortable" data-sort="bot">Bot</th>
                        <th class="sortable" data-sort="value">Total Value</th>
                        <th class="sortable" data-sort="markup">Markup</th>
                        <th class="sortable" data-sort="session">Session</th>
                        <th class="sortable" data-sort="hostvm">Host VM</th>
                        <th>Trade ID</th>
                        <th>WS Winner</th>
                    </tr>
                </thead>
                <tbody id="tradesTableBody">
                </tbody>
            </table>
        </div>
        
        <div class="table-container" style="margin-top: 40px;">
            <div class="table-header">❌ Snipes We Did NOT Get (create-trades ≤3%, not in trade-success, price 30–1200)</div>
            <input type="text" class="search-box" id="searchMissedBox" placeholder="Search missed snipes by Trade ID, value, markup...">
            <table id="missedSnipesTable">
                <thead>
                    <tr>
                        <th class="sortable" data-sort="time">Time</th>
                        <th>Trade ID</th>
                        <th class="sortable" data-sort="markup">Markup</th>
                        <th class="sortable" data-sort="value">Value</th>
                        <th>Item</th>
                        <th>Raw (snippet)</th>
                    </tr>
                </thead>
                <tbody id="missedSnipesTableBody">
                </tbody>
            </table>
        </div>
    </div>
    
    <script>
        const hostnames = ${JSON.stringify(hostnames)};
        const hostnameToColor = ${JSON.stringify(hostnameToColor)};
        const numHostnames = hostnames.length;

        window.scatterTraces = ${JSON.stringify(hostnames.map((h) => ({
          x: timesByHostname[h].map(t => t.toISOString()),
          y: classYByHostname[h],
          mode: 'markers',
          type: 'scatter',
          name: h,
          marker: { size: sizeByHostname[h], color: hostnameToColor[h], symbol: 'circle', opacity: 0.7, line: { width: 1, color: '#fff' } },
          text: hoverTextByHostname[h],
          hovertemplate: '<b>' + h + '</b><br>%{text}<extra></extra>',
          showlegend: true
        })))};

        const timesFullByHostname = ${JSON.stringify(Object.fromEntries(hostnames.map(h => [h, timesByHostname[h].map(t => t.toISOString())])))};
        const classYByHostnameData = ${JSON.stringify(classYByHostname)};
        const sizeByHostnameData = ${JSON.stringify(sizeByHostname)};
        const priceDataByHostname = ${JSON.stringify(priceYByHostname)};
        const markupDataByHostname = ${JSON.stringify(markupYByHostname)};
        const hoverTextFullByHostname = ${JSON.stringify(hoverTextByHostname)};
        const eventsWithIndicesByHostname = ${JSON.stringify(Object.fromEntries(hostnames.map(h => [h, eventsByHostname[h].map((e, i) => ({ ...e, index: i }))])))};

        const distTtfbMsByHostnameClient = ${JSON.stringify(distTtfbMsByHostname)};
        const distBackendMsByHostnameClient = ${JSON.stringify(distBackendMsByHostname)};
        const distTotalMsByHostnameClient = ${JSON.stringify(distTotalMsByHostname)};
        
        // Got vs Missed scatter + rolling % line
        const gotPointsClient = ${JSON.stringify(gotPoints.map((p) => ({ date: p.date.toISOString(), y: p.y, hover: p.hover })))};
        const missedPointsClient = ${JSON.stringify(missedPoints.map((p) => ({ date: p.date.toISOString(), y: p.y, hover: p.hover })))};
        const rollingPctXClient = ${JSON.stringify(rollingPctX.map((d) => d.toISOString()))};
        const smoothedRollingPctYClient = ${JSON.stringify(smoothedRollingPctY)};
        const endpointLineBinTimesClient = ${JSON.stringify(endpointLineBinTimes.map((d) => d.toISOString()))};
        const endpointLineCountsByHostnameClient = ${JSON.stringify(endpointLineCountsByHostname)};
        const stackedBarBinTimesClient = ${JSON.stringify(stackedBarBinTimes.map((d) => d.toISOString()))};
        const stackedBarGotCountsClient = ${JSON.stringify(stackedBarGotCounts)};
        const stackedBarMissedCountsClient = ${JSON.stringify(stackedBarMissedCounts)};
        const stackedBarBinDetailsClient = ${JSON.stringify(stackedBarBinDetails)};
        const stackedBarWinRateClient = ${JSON.stringify(stackedBarWinRate)};
        const volumeBinTimesClient = ${JSON.stringify(volumeBinTimes.map((d) => d.toISOString()))};
        const smoothedVolumeCountsClient = ${JSON.stringify(smoothedVolumeCounts)};
        const smoothedGotCountsClient = ${JSON.stringify(smoothedGotCounts)};
        const allMissedForTable = ${JSON.stringify(missedSnipesDeduped.map((e) => ({
          time: formatTimestamp(e.timestamp),
          timestamp: e.timestamp,
          tradeId: e.tradeId || 'N/A',
          markup: e.markup || (e.markupPercent != null ? e.markupPercent + '%' : 'N/A'),
          value: e.totalValue || 'N/A',
          item: e.itemName || 'N/A',
          raw: (e.rawContent || '').substring(0, 300)
        })))};
        
        // KDE (Kernel Density Estimate) calculation
        window.calculateKDE = function(times, bandwidth) {
            if (times.length === 0) return { x: [], y: [] };
            
            // Convert times to timestamps
            const timestamps = times.map(t => {
                const date = t instanceof Date ? t : new Date(t);
                return date.getTime();
            });
            
            // Sort timestamps
            timestamps.sort((a, b) => a - b);
            
            // Create evaluation points (100 points across the time range)
            const minTime = Math.min(...timestamps);
            const maxTime = Math.max(...timestamps);
            const range = maxTime - minTime;
            const numPoints = 100;
            const step = range / (numPoints - 1);
            
            // Auto-calculate bandwidth if not provided (Silverman's rule of thumb)
            if (!bandwidth) {
                const n = timestamps.length;
                const stdDev = Math.sqrt(timestamps.reduce((sum, t) => sum + Math.pow(t - (timestamps.reduce((a,b) => a+b) / n), 2), 0) / n);
                bandwidth = 1.06 * stdDev * Math.pow(n, -0.2);
            }
            
            const kdeX = [];
            const kdeY = [];
            
            // Gaussian kernel function
            const gaussianKernel = (x) => (1 / Math.sqrt(2 * Math.PI)) * Math.exp(-0.5 * x * x);
            
            // Calculate KDE at each evaluation point
            for (let i = 0; i < numPoints; i++) {
                const evalPoint = minTime + i * step;
                let density = 0;
                
                // Sum contributions from all data points
                for (const timestamp of timestamps) {
                    const u = (evalPoint - timestamp) / bandwidth;
                    density += gaussianKernel(u);
                }
                
                density = (density / (timestamps.length * bandwidth)) * (range / 10); // Scale for visibility
                
                kdeX.push(new Date(evalPoint));
                kdeY.push(density);
            }
            
            return { x: kdeX, y: kdeY };
        };
        
        // KDE for each hostname and total
        window.kdeTraces = hostnames.map((h, i) => {
            const times = timesFullByHostname[h].map(t => new Date(t));
            const kde = window.calculateKDE(times);
            const col = hostnameToColor[h];
            const rgb = col.startsWith('#') ? col.slice(1).match(/.{2}/g).map(x => parseInt(x, 16)) : [0,0,0];
            return {
                x: kde.x,
                y: kde.y,
                mode: 'lines',
                type: 'scatter',
                name: h + ' KDE',
                line: { color: col, width: 3, shape: 'spline' },
                fill: 'tozeroy',
                fillcolor: 'rgba(' + rgb[0] + ',' + rgb[1] + ',' + rgb[2] + ',0.2)',
                yaxis: 'y2',
                showlegend: true,
                visible: false
            };
        });
        const allTimesForKDE = hostnames.flatMap(h => timesFullByHostname[h].map(t => new Date(t)));
        const totalKDE = window.calculateKDE(allTimesForKDE);
        window.totalKDETrace = {
            x: totalKDE.x,
            y: totalKDE.y,
            mode: 'lines',
            type: 'scatter',
            name: 'Total KDE',
            line: { color: '#667eea', width: 4, shape: 'spline' },
            fill: 'tozeroy',
            fillcolor: 'rgba(102, 126, 234, 0.2)',
            yaxis: 'y2',
            showlegend: true,
            visible: false
        };
        const data = [...window.scatterTraces, ...window.kdeTraces, window.totalKDETrace];
        
        const layout = {
            title: {
                text: 'Snipes by Hostname',
                font: { size: 24 }
            },
            xaxis: {
                title: 'Time',
                type: 'date',
                range: ${JSON.stringify(xAxisRange)},
                autorange: false,
                showgrid: true,
                gridcolor: '#e0e0e0'
            },
            yaxis: {
                title: 'Class (Hostname)',
                showgrid: true,
                gridcolor: '#e0e0e0',
                tickvals: hostnames.map((_, i) => i + 0.5),
                ticktext: hostnames,
                range: [-0.5, hostnames.length],
                side: 'left'
            },
            yaxis2: {
                title: 'Density',
                overlaying: 'y',
                side: 'right',
                showgrid: false
            },
            hovermode: 'closest',
            plot_bgcolor: '#fafafa',
            paper_bgcolor: 'white',
            legend: {
                x: 1.02,
                y: 1,
                xanchor: 'left',
                bgcolor: 'rgba(255,255,255,0.95)',
                bordercolor: '#ddd',
                borderwidth: 1
            },
            margin: {
                l: 80,
                r: 220,
                t: 80,
                b: 80
            },
            shapes: hostnames.slice(0, -1).map((_, i) => ({
                type: 'line',
                xref: 'paper',
                yref: 'y',
                x0: 0,
                x1: 1,
                y0: i + 1,
                y1: i + 1,
                line: { color: '#ebebeb', width: 1 }
            }))
        };
        
        const config = {
            responsive: true,
            displayModeBar: true,
            modeBarButtonsToRemove: ['lasso2d'],
            displaylogo: false
        };
        
        let plotDiv = document.getElementById('chart');
        Plotly.newPlot(plotDiv, data, layout, config);

        // Violin + box plots: continuous fields by WS hostname (below main chart)
        function hexToRgbDist(hex) {
            const h = (hex && hex.startsWith('#')) ? hex.slice(1) : '667eea';
            const parts = h.match(/.{2}/g);
            if (!parts || parts.length < 3) return [102, 126, 234];
            return parts.slice(0, 3).map(function (x) { return parseInt(x, 16); });
        }
        function buildViolinTracesDist(dataByHost, scalegroupId) {
            const traces = [];
            hostnames.forEach(function (h) {
                const y = (dataByHost[h] || []).filter(function (v) { return typeof v === 'number' && Number.isFinite(v); });
                if (y.length === 0) return;
                const col = hostnameToColor[h] || '#667eea';
                const rgb = hexToRgbDist(col);
                traces.push({
                    type: 'violin',
                    x: y.map(function () { return h; }),
                    y: y,
                    name: h,
                    scalegroup: scalegroupId,
                    side: 'both',
                    box: { visible: true, width: 0.14 },
                    meanline: { visible: true },
                    fillcolor: 'rgba(' + rgb[0] + ',' + rgb[1] + ',' + rgb[2] + ',0.35)',
                    line: { color: col, width: 1 },
                    opacity: 0.92,
                    points: false,
                    showlegend: false
                });
            });
            return traces;
        }
        function plotDistViolins(divId, dataByHost, title, yTitle, tickformat, scalegroupId) {
            const el = document.getElementById(divId);
            if (!el) return false;
            const wrap = el.closest('.subsection-dist');
            const traces = buildViolinTracesDist(dataByHost, scalegroupId);
            if (traces.length === 0) {
                if (wrap) wrap.style.display = 'none';
                return false;
            }
            if (wrap) wrap.style.display = '';
            const layout = {
                title: { text: title, font: { size: 16 } },
                xaxis: {
                    title: 'WS hostname',
                    type: 'category',
                    categoryorder: 'array',
                    categoryarray: hostnames,
                    tickangle: -25
                },
                yaxis: { title: yTitle, zeroline: true, tickformat: tickformat || '' },
                violinmode: 'group',
                violingap: 0.08,
                violingroupgap: 0.15,
                showlegend: false,
                margin: { l: 64, r: 28, t: 48, b: 100 },
                plot_bgcolor: '#fafafa',
                paper_bgcolor: 'white'
            };
            Plotly.newPlot(el, traces, layout, config);
            return true;
        }
        var anyDistChart = false;
        if (plotDistViolins('chartDistTtfb', distTtfbMsByHostnameClient, 'TTFB by hostname', 'TTFB (ms)', '.1f', 'tf')) anyDistChart = true;
        if (plotDistViolins('chartDistBackend', distBackendMsByHostnameClient, 'Backend time by hostname', 'Backend (ms)', '.1f', 'be')) anyDistChart = true;
        if (plotDistViolins('chartDistTotal', distTotalMsByHostnameClient, 'Total time by hostname', 'Total (ms)', '.1f', 'tot')) anyDistChart = true;
        var distEmptyEl = document.getElementById('distChartsEmpty');
        if (distEmptyEl) distEmptyEl.style.display = anyDistChart ? 'none' : 'block';

        // Snipe count by endpoint over time (one line per hostname)
        const chartEndpointLinesDiv = document.getElementById('chartEndpointLines');
        if (chartEndpointLinesDiv && endpointLineBinTimesClient.length > 0) {
            const endpointTraces = hostnames.map((h) => ({
                x: endpointLineBinTimesClient,
                y: endpointLineCountsByHostnameClient[h] || [],
                mode: 'lines',
                type: 'scatter',
                name: h,
                line: { color: hostnameToColor[h], width: 2, shape: 'spline' },
                showlegend: true
            }));
            const endpointLayout = {
                title: { text: 'Snipe count by endpoint (1h bins)' },
                xaxis: { title: 'Time', type: 'date', range: ${JSON.stringify(xAxisRange)}, autorange: false },
                yaxis: { title: 'Snipe count per hour', rangemode: 'tozero' },
                hovermode: 'x unified',
                showlegend: true,
                legend: { x: 1.02, y: 1, xanchor: 'left' },
                margin: { l: 60, r: 180, t: 50, b: 50 }
            };
            Plotly.newPlot(chartEndpointLinesDiv, endpointTraces, endpointLayout, config);
        }

        // Got vs Missed scatter chart
        const chartGotMissedDiv = document.getElementById('chartGotMissed');
        if (chartGotMissedDiv && gotPointsClient.length + missedPointsClient.length > 0) {
            const gotTrace = {
                x: gotPointsClient.map(p => p.date),
                y: gotPointsClient.map(p => p.y),
                mode: 'markers',
                type: 'scatter',
                name: 'Snipes we got',
                marker: { size: 10, color: '#22c55e', symbol: 'circle', opacity: 0.8, line: { width: 1, color: '#fff' } },
                text: gotPointsClient.map(p => p.hover),
                hovertemplate: '%{text}<extra></extra>',
                showlegend: true
            };
            const missedTrace = {
                x: missedPointsClient.map(p => p.date),
                y: missedPointsClient.map(p => p.y),
                mode: 'markers',
                type: 'scatter',
                name: 'Snipes we missed',
                marker: { size: 10, color: '#ef4444', symbol: 'x', opacity: 0.8, line: { width: 1, color: '#fff' } },
                text: missedPointsClient.map(p => p.hover),
                hovertemplate: '%{text}<extra></extra>',
                showlegend: true
            };
            const lineTrace = {
                x: rollingPctXClient,
                y: smoothedRollingPctYClient,
                mode: 'lines',
                type: 'scatter',
                name: '% snipes we take (1h smoothed)',
                line: { color: '#667eea', width: 3, shape: 'spline' },
                showlegend: true
            };
            const gotMissedLayout = {
                title: { text: 'Got vs Missed • Y: % we got (jittered) • Line: rolling 1h %' },
                xaxis: { title: 'Time of day', type: 'date', range: ${JSON.stringify(xAxisRange)}, autorange: false },
                yaxis: { title: '% of snipes we got', range: [0, 105], tickformat: '.0f', ticksuffix: '%' },
                hovermode: 'closest',
                showlegend: true,
                legend: { x: 0.02, y: 0.98 }
            };
            Plotly.newPlot(chartGotMissedDiv, [gotTrace, missedTrace, lineTrace], gotMissedLayout, config);
        }

        // Counts (15-min bins): grouped bars + line = snipes we got
        const chartStackedDiv = document.getElementById('chartGotMissedStacked');
        if (chartStackedDiv && stackedBarBinTimesClient.length > 0) {
            const gotBarTrace = {
                x: stackedBarBinTimesClient,
                y: stackedBarGotCountsClient,
                type: 'bar',
                name: 'Got',
                marker: { color: '#22c55e' },
                showlegend: true
            };
            const missedBarTrace = {
                x: stackedBarBinTimesClient,
                y: stackedBarMissedCountsClient,
                type: 'bar',
                name: 'Missed',
                marker: { color: '#ef4444' },
                showlegend: true
            };
            const gotLineTrace = {
                x: stackedBarBinTimesClient,
                y: stackedBarWinRateClient,
                mode: 'lines',
                type: 'scatter',
                name: '% got (hour)',
                yaxis: 'y2',
                line: { color: '#667eea', width: 3, shape: 'spline' },
                connectgaps: false,
                showlegend: true
            };
            const stackedLayout = {
                barmode: 'group',
                title: { text: 'Counts per 1 hour (bars) + % got (line, right axis)' },
                xaxis: { title: 'Time', type: 'date', range: ${JSON.stringify(xAxisRange)}, autorange: false },
                yaxis: {
                    title: 'Count per 1 hour',
                    rangemode: 'tozero',
                    side: 'left',
                    showgrid: true
                },
                yaxis2: {
                    title: '% snipes we got',
                    range: [0, 100],
                    tickformat: '.0f',
                    ticksuffix: '%',
                    side: 'right',
                    overlaying: 'y',
                    showgrid: false,
                    zeroline: true
                },
                hovermode: 'x unified',
                showlegend: true,
                legend: { x: 1.02, y: 1, xanchor: 'left' },
                margin: { l: 60, r: 72, t: 50, b: 50 }
            };
            Plotly.newPlot(chartStackedDiv, [gotBarTrace, missedBarTrace, gotLineTrace], stackedLayout, config);
            chartStackedDiv.on('plotly_click', function(event) {
                const pt = event.points[0];
                const binIndex = pt.pointNumber;
                const detail = stackedBarBinDetailsClient[binIndex];
                if (!detail) return;
                document.getElementById('barChartTableBinLabel').textContent = detail.binLabel;
                const rows = [...detail.got, ...detail.missed];
                const tbody = document.getElementById('barChartDetailTableBody');
                const esc = (s) => (s || '').replace(/</g, '&lt;').replace(/"/g, '&quot;');
                tbody.innerHTML = rows.map((r) => '<tr style="border-bottom: 1px solid #eee;"><td style="padding: 8px; font-weight: bold; color: ' + (r.type === 'got' ? '#22c55e' : '#ef4444') + ';">' + (r.type === 'got' ? 'Got' : 'Missed') + '</td><td style="padding: 8px;">' + esc(r.dateTime) + '</td><td style="padding: 8px;">' + esc(r.tradeId) + '</td><td style="padding: 8px;">' + esc(r.markup) + '</td><td style="padding: 8px;">' + esc(r.liquidity) + '</td><td style="padding: 8px;">' + esc(r.buffPrice) + '</td><td style="padding: 8px;">' + esc(r.price) + '</td><td style="padding: 8px;">' + esc(r.externalPrice) + '</td></tr>').join('');
                document.getElementById('barChartTableSection').style.display = 'block';
            });
        }

        // Volume of items ≤3% + snipes we got (1h bins, smoothed)
        const chartVolumeDiv = document.getElementById('chartVolume');
        if (chartVolumeDiv && volumeBinTimesClient.length > 0) {
            const volumeTrace = {
                x: volumeBinTimesClient,
                y: smoothedVolumeCountsClient,
                mode: 'lines',
                type: 'scatter',
                name: 'Items ≤3% (create-trades)',
                line: { color: '#0ea5e9', width: 2, shape: 'spline' },
                fill: 'tozeroy',
                fillcolor: 'rgba(14, 165, 233, 0.15)',
                showlegend: true
            };
            const gotVolumeTrace = {
                x: volumeBinTimesClient,
                y: smoothedGotCountsClient,
                mode: 'lines',
                type: 'scatter',
                name: 'Snipes we got',
                line: { color: '#22c55e', width: 2, shape: 'spline' },
                showlegend: true
            };
            const volumeLayout = {
                title: { text: 'Volume (create-trades ≤3%) + Snipes we got (1h bins, smoothed)' },
                xaxis: { title: 'Time of day', type: 'date' },
                yaxis: { title: 'Count per hour', rangemode: 'tozero' },
                hovermode: 'x unified',
                showlegend: true
            };
            Plotly.newPlot(chartVolumeDiv, [volumeTrace, gotVolumeTrace], volumeLayout, config);
        }

        // Missed snipes table
        let filteredMissed = [...allMissedForTable];
        function renderMissedTable(rows) {
            const tbody = document.getElementById('missedSnipesTableBody');
            if (!tbody) return;
            tbody.innerHTML = rows.map(r => '<tr><td>' + r.time + '</td><td>' + r.tradeId + '</td><td>' + r.markup + '</td><td>' + r.value + '</td><td>' + (r.item || '') + '</td><td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;" title="' + (r.raw || '').replace(/"/g, '&quot;') + '">' + (r.raw || '').replace(/</g, '&lt;') + '</td></tr>').join('');
        }
        const searchMissedBox = document.getElementById('searchMissedBox');
        if (searchMissedBox) {
            searchMissedBox.addEventListener('input', function() {
                const q = this.value.toLowerCase();
                filteredMissed = q ? allMissedForTable.filter(r => [r.tradeId, r.markup, r.value, r.item, r.raw].some(v => String(v || '').toLowerCase().includes(q))) : [...allMissedForTable];
                renderMissedTable(filteredMissed);
            });
        }
        renderMissedTable(filteredMissed);

        // Clustering state
        let clustersEnabled = false;
        let clusterData = null;
        
        // KNN-based clustering algorithm (DBSCAN-like) - Time-based only
        function findClusters(epsTime = 3600000, minPoints = 3) {
            // epsTime: max time difference in ms (default 1 hour)
            // minPoints: minimum points to form a cluster
            const clusters = [];
            const visited = new Set();
            const noise = [];
            
            function distance(p1, p2) {
                // Only consider time difference, ignore price
                const timeDiff = Math.abs(p1.x - p2.x);
                return timeDiff / epsTime; // Normalized time distance (0-1 if within eps)
            }
            
            function getNeighbors(point, points) {
                return points.filter(p => {
                    if (p === point) return false;
                    return distance(point, p) <= 1.0; // Within eps time radius
                });
            }
            
            function expandCluster(point, neighbors, clusterId) {
                clusters[clusterId] = clusters[clusterId] || [];
                clusters[clusterId].push(point);
                visited.add(point);
                
                for (let neighbor of neighbors) {
                    if (!visited.has(neighbor)) {
                        visited.add(neighbor);
                        const neighborNeighbors = getNeighbors(neighbor, allPoints);
                        if (neighborNeighbors.length >= minPoints - 1) {
                            neighbors.push(...neighborNeighbors.filter(n => !neighbors.includes(n)));
                        }
                    }
                    if (!clusters.some(c => c.includes(neighbor))) {
                        clusters[clusterId].push(neighbor);
                    }
                }
            }
            
            const allPoints = [];
            hostnames.forEach((h, traceIdx) => {
                const trace = window.scatterTraces[traceIdx];
                (trace.x || []).forEach((x, i) => {
                    const timestamp = x instanceof Date ? x.getTime() : (typeof x === 'string' ? new Date(x).getTime() : x);
                    allPoints.push({ x: timestamp, y: trace.y[i], trace: traceIdx, traceName: h, index: i });
                });
            });
            
            // Find clusters
            for (let point of allPoints) {
                if (visited.has(point)) continue;
                
                visited.add(point);
                const neighbors = getNeighbors(point, allPoints);
                
                if (neighbors.length < minPoints) {
                    noise.push(point);
                } else {
                    const clusterId = clusters.length;
                    expandCluster(point, neighbors, clusterId);
                }
            }
            
            return { clusters: clusters.filter(c => c.length >= minPoints), noise };
        }
        
        // Create cluster visualization
        function visualizeClusters(clusterResult) {
            if (!clusterResult || clusterResult.clusters.length === 0) {
                return [];
            }
            
            const clusterTraces = [];
            const colors = ['#ff9800', '#9c27b0', '#00bcd4', '#8bc34a', '#ff5722', '#3f51b5', '#e91e63'];
            
            clusterResult.clusters.forEach((cluster, idx) => {
                if (cluster.length < 2) return;
                
                // Calculate cluster center and radius
                const centerX = cluster.reduce((sum, p) => sum + p.x, 0) / cluster.length;
                const centerY = cluster.reduce((sum, p) => sum + p.y, 0) / cluster.length;
                
                // Calculate time span of cluster
                const times = cluster.map(p => p.x);
                const minTime = Math.min(...times);
                const maxTime = Math.max(...times);
                const timeSpan = maxTime - minTime;
                
                // Calculate price range of cluster
                const prices = cluster.map(p => p.y);
                const minPrice = Math.min(...prices);
                const maxPrice = Math.max(...prices);
                const priceRange = maxPrice - minPrice;
                
                // Create rectangle/ellipse that spans the cluster
                const radiusX = (timeSpan / 2) * 1.2; // 20% padding
                const radiusY = Math.max((priceRange / 2) * 1.2, 10); // 20% padding, min 10
                
                // Create ellipse/circle around cluster
                const theta = Array.from({length: 50}, (_, i) => (i / 50) * 2 * Math.PI);
                const ellipseX = theta.map(t => new Date(centerX + radiusX * Math.cos(t)));
                const ellipseY = theta.map(t => centerY + radiusY * Math.sin(t));
                
                clusterTraces.push({
                    x: ellipseX,
                    y: ellipseY,
                    mode: 'lines',
                    type: 'scatter',
                    line: {
                        color: colors[idx % colors.length],
                        width: 2,
                        dash: 'dash'
                    },
                    fill: 'toself',
                    fillcolor: colors[idx % colors.length],
                    opacity: 0.1,
                    showlegend: false,
                    hoverinfo: 'skip',
                    name: 'Cluster ' + (idx + 1) + ' (' + cluster.length + ' snipes)'
                });
            });
            
            return clusterTraces;
        }
        
        // Toggle cluster visualization
        function toggleClusters() {
            clustersEnabled = !clustersEnabled;
            const btn = document.getElementById('clusterBtn');
            const info = document.getElementById('clusterInfo');
            
            if (clustersEnabled) {
                btn.textContent = '🔍 Disable Cluster Finder';
                btn.classList.add('active');
                
                // Find clusters
                clusterData = findClusters();
                const clusterTraces = visualizeClusters(clusterData);
                
                // Update plot with cluster overlays
                const currentData = [...data];
                if (clusterTraces.length > 0) {
                    currentData.push(...clusterTraces);
                }
                
                Plotly.redraw(plotDiv, currentData);
                
                // Show cluster info
                if (clusterData.clusters.length > 0) {
                    const clusterCount = clusterData.clusters.length;
                    const totalSnipes = clusterData.clusters.reduce((sum, c) => sum + c.length, 0);
                    const clusterInfoText = 'Found ' + clusterCount + ' cluster(s) with ' + totalSnipes + ' total snipes';
                    info.textContent = clusterInfoText;
                    info.style.display = 'block';
                } else {
                    info.textContent = 'No significant clusters found';
                    info.style.display = 'block';
                }
            } else {
                btn.textContent = '🔍 Enable Cluster Finder';
                btn.classList.remove('active');
                info.style.display = 'none';
                
                // Remove cluster overlays
                Plotly.redraw(plotDiv, data);
            }
        }
        
        // Add button event listeners
        document.getElementById('clusterBtn').addEventListener('click', toggleClusters);

        // Y-axis is fixed to class (hostname bands); dot size = value. No price/markup toggle.
        let showMarkup = false;

        function toggleYAxis() {
            showMarkup = !showMarkup;
            const btn = document.getElementById('yAxisBtn');
            const infoDiv = document.querySelector('.info');
            // Keep y-axis as class; chart unchanged
            layout.yaxis.title = 'Class (Hostname)';
            layout.yaxis.tickvals = hostnames.map((_, i) => i + 0.5);
            layout.yaxis.ticktext = hostnames;
            layout.yaxis.range = [-0.5, hostnames.length];
            delete layout.yaxis.tickformat;
            const infoText = '<strong>📊 Chart Information:</strong><br>' +
                '• X-axis: Time of day<br>' +
                '• Y-axis: Class (hostname) — each class in its own band<br>' +
                '• Dot size = snipe value (min 30, max 2000; 1000 = 2× min size)<br>' +
                '• Use the filter dropdowns above to filter by Region or Endpoint (WS Host)<br>' +
                '• Hover over data points to see full details<br>' +
                '• Data from the last ${daysBack} days';
            if (infoDiv) infoDiv.innerHTML = infoText;
            btn.textContent = '📊 Show Markup';
            btn.classList.remove('active');
            applyFilters();
        }

        // Add y-axis toggle event listener
        document.getElementById('yAxisBtn').addEventListener('click', toggleYAxis);
        
        // KDE line toggle state
        let kdeEnabled = false;
        
        // Function to toggle KDE lines
        function toggleKDE() {
            kdeEnabled = !kdeEnabled;
            const btn = document.getElementById('kdeBtn');
            const n = numHostnames;
            const scatterVis = Array(n).fill(true);
            const kdeVis = Array(n + 1).fill(kdeEnabled);
            const visible = [...scatterVis, ...kdeVis];
            const indices = Array(2*n + 1).fill(0).map((_, i) => i);
            
            if (kdeEnabled) {
                btn.textContent = '📈 Hide KDE Plot';
                btn.classList.add('active');
            } else {
                btn.textContent = '📈 Show KDE Plot';
                btn.classList.remove('active');
            }
            Plotly.restyle(plotDiv, { visible: visible }, indices);
        }
        
        // Add KDE toggle event listener
        document.getElementById('kdeBtn').addEventListener('click', toggleKDE);

        // Filter functionality
        const allEventsForFiltering = hostnames.flatMap(h => eventsWithIndicesByHostname[h]);
        const uniqueRegions = [...new Set(allEventsForFiltering.map(e => e.region || 'N/A').filter(v => v !== 'N/A'))].sort();
        const uniqueWsHosts = [...new Set(allEventsForFiltering.map(e => e.wsHost || 'N/A').filter(v => v !== 'N/A'))].sort();

        const regionFilter = document.getElementById('regionFilter');
        const wsHostFilter = document.getElementById('wsHostFilter');
        uniqueRegions.forEach(region => {
            const option = document.createElement('option');
            option.value = region;
            option.textContent = region;
            regionFilter.appendChild(option);
        });
        uniqueWsHosts.forEach(host => {
            const option = document.createElement('option');
            option.value = host;
            option.textContent = host;
            wsHostFilter.appendChild(option);
        });

        function applyFilters() {
            const selectedRegion = regionFilter.value;
            const selectedWsHost = wsHostFilter.value;
            const n = numHostnames;

            hostnames.forEach((h, traceIdx) => {
                const evs = eventsWithIndicesByHostname[h];
                const filteredIndices = evs
                    .filter(event => {
                        const regionMatch = selectedRegion === 'all' || (event.region || 'N/A') === selectedRegion;
                        const wsHostMatch = selectedWsHost === 'all' || (event.wsHost || 'N/A') === selectedWsHost;
                        return regionMatch && wsHostMatch;
                    })
                    .map(event => event.index);
                const timesFull = timesFullByHostname[h];
                const classY = classYByHostnameData[h];
                const sizes = sizeByHostnameData[h];
                const hoverTextFull = hoverTextFullByHostname[h];
                window.scatterTraces[traceIdx].x = filteredIndices.map(i => timesFull[i]);
                window.scatterTraces[traceIdx].y = filteredIndices.map(i => classY[i]);
                window.scatterTraces[traceIdx].text = filteredIndices.map(i => hoverTextFull[i]);
                window.scatterTraces[traceIdx].marker.size = filteredIndices.map(i => sizes[i]);
                Plotly.restyle(plotDiv, {
                    x: [window.scatterTraces[traceIdx].x],
                    y: [window.scatterTraces[traceIdx].y],
                    text: [window.scatterTraces[traceIdx].text],
                    'marker.size': [window.scatterTraces[traceIdx].marker.size]
                }, [traceIdx]);
            });

            if (kdeEnabled) {
                hostnames.forEach((h, i) => {
                    const evs = eventsWithIndicesByHostname[h];
                    const filteredIndices = evs
                        .filter(event => {
                            const regionMatch = selectedRegion === 'all' || (event.region || 'N/A') === selectedRegion;
                            const wsHostMatch = selectedWsHost === 'all' || (event.wsHost || 'N/A') === selectedWsHost;
                            return regionMatch && wsHostMatch;
                        })
                        .map(event => event.index);
                    const timesFull = timesFullByHostname[h];
                    const filteredTimes = filteredIndices.map(i => new Date(timesFull[i]));
                    const kde = window.calculateKDE(filteredTimes);
                    Plotly.restyle(plotDiv, { x: [kde.x], y: [kde.y] }, [n + i]);
                });
                const allFilteredTimes = hostnames.flatMap(h => {
                    const evs = eventsWithIndicesByHostname[h];
                    const filteredIndices = evs
                        .filter(event => {
                            const regionMatch = selectedRegion === 'all' || (event.region || 'N/A') === selectedRegion;
                            const wsHostMatch = selectedWsHost === 'all' || (event.wsHost || 'N/A') === selectedWsHost;
                            return regionMatch && wsHostMatch;
                        })
                        .map(event => event.index);
                    return filteredIndices.map(i => new Date(timesFullByHostname[h][i]));
                });
                const filteredTotalKDE = window.calculateKDE(allFilteredTimes);
                Plotly.restyle(plotDiv, { x: [filteredTotalKDE.x], y: [filteredTotalKDE.y] }, [2*n]);
            }
            Plotly.redraw(plotDiv);
        }

        // Add event listeners to filters
        regionFilter.addEventListener('change', applyFilters);
        wsHostFilter.addEventListener('change', applyFilters);
        
        // Format timestamp function for browser
        function formatTimestamp(timestamp) {
          const date = new Date(timestamp);
          return date.toLocaleString('en-US', {
            year: 'numeric',
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: true
          });
        }
        
        // Prepare table data
        const allEvents = ${JSON.stringify(events.map(e => ({
          timestamp: e.timestamp,
          type: e.type,
          bot: e.botName || 'N/A',
          value: e.totalValue || 'N/A',
          markup: e.markup || 'N/A',
          session: e.sessionUsed || 'N/A',
          hostvm: e.hostVM || 'N/A',
          tradeId: e.tradeId || 'N/A',
          wsWinner: e.wsWinner || 'N/A',
          region: e.region || 'N/A',
          zone: e.zone || 'N/A',
          wsHost: e.wsHost || 'N/A',
          postHost: e.postHost || 'N/A'
        })))};
        
        // Format timestamps for display
        allEvents.forEach(event => {
          event.time = formatTimestamp(event.timestamp);
        });
        
        let currentSort = { column: null, direction: 'asc' };
        let filteredEvents = [...allEvents];
        
        function renderTable(eventsToShow) {
          const tbody = document.getElementById('tradesTableBody');
          tbody.innerHTML = eventsToShow.map(event => {
            const typeColor = hostnameToColor[event.type] || '#333';
            return '<tr>' +
              '<td>' + event.time + '</td>' +
              '<td class="type-host" style="color: ' + typeColor + '">' + event.type + '</td>' +
              '<td>' + event.bot + '</td>' +
              '<td>' + event.value + '</td>' +
              '<td>' + event.markup + '</td>' +
              '<td>' + event.session + '</td>' +
              '<td>' + event.hostvm + '</td>' +
              '<td>' + event.tradeId + '</td>' +
              '<td style="max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="' + event.wsWinner + '">' + event.wsWinner + '</td>' +
              '</tr>';
          }).join('');
        }
        
        function sortTable(column) {
          if (currentSort.column === column) {
            currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
          } else {
            currentSort.column = column;
            currentSort.direction = 'asc';
          }
          
          filteredEvents.sort((a, b) => {
            let aVal = a[column];
            let bVal = b[column];
            
            // Handle numeric sorting for timestamp
            if (column === 'time') {
              aVal = a.timestamp;
              bVal = b.timestamp;
            }
            
            // Handle numeric sorting for value
            if (column === 'value') {
              aVal = parseFloat(aVal.replace(/[^0-9.]/g, '')) || 0;
              bVal = parseFloat(bVal.replace(/[^0-9.]/g, '')) || 0;
            }
            
            if (aVal < bVal) return currentSort.direction === 'asc' ? -1 : 1;
            if (aVal > bVal) return currentSort.direction === 'asc' ? 1 : -1;
            return 0;
          });
          
          // Update sort indicators
          document.querySelectorAll('th.sortable').forEach(th => {
            th.classList.remove('sort-asc', 'sort-desc');
            if (th.dataset.sort === column) {
              th.classList.add(currentSort.direction === 'asc' ? 'sort-asc' : 'sort-desc');
            }
          });
          
          renderTable(filteredEvents);
        }
        
        // Add sort handlers
        document.querySelectorAll('th.sortable').forEach(th => {
          th.addEventListener('click', () => sortTable(th.dataset.sort));
        });
        
        // Add search handler
        document.getElementById('searchBox').addEventListener('input', (e) => {
          const searchTerm = e.target.value.toLowerCase();
          filteredEvents = allEvents.filter(event => {
            return Object.values(event).some(val => 
              String(val).toLowerCase().includes(searchTerm)
            );
          });
          renderTable(filteredEvents);
        });
        
        // Initial render
        renderTable(filteredEvents);
    </script>
</body>
</html>`;
  
  return html;
}

/**
 * Build full dashboard HTML for an explicit UTC calendar date range.
 * startDateStr / endDateStr: 'YYYY-MM-DD'
 * supabaseClient: optional Supabase client (from snipesCache.cjs) — pass null to skip caching.
 * Returns { html, cacheHit }
 */
async function getHtmlForDateRange(startDateStr, endDateStr, supabaseClient) {
  if (!DISCORD_BOT_TOKEN) {
    throw new Error('Missing DISCORD_BOT_TOKEN environment variable');
  }

  const {
    dateStringToRange,
    isFullyInPast,
    loadCache,
    upsertCache,
    getChannelWatermark
  } = require('./snipesCache.cjs');

  const { startMs: chartStartMs, endMs: chartEndMs } = dateStringToRange(startDateStr);
  const { endMs: endDayMs }  = dateStringToRange(endDateStr);
  const rangeStartMs = chartStartMs;
  const rangeEndMs   = endDayMs;

  // Days count for generateHTML label
  const [sy, sm, sd] = startDateStr.split('-').map(Number);
  const [ey, em, ed] = endDateStr.split('-').map(Number);
  const daysBack = Math.round((Date.UTC(ey, em-1, ed) - Date.UTC(sy, sm-1, sd)) / 86400000) + 1;

  const client = new Client({
    intents: [
      GatewayIntentBits.Guilds,
      GatewayIntentBits.GuildMessages,
      GatewayIntentBits.MessageContent
    ]
  });

  await client.login(DISCORD_BOT_TOKEN);

  try {
    // --- Cheap watermark check (skip entirely for past-only ranges) ---
    let wmTradeSuccess = '';
    let wmCreateTrades = '';
    const rangeIsPast = isFullyInPast(endDateStr);

    if (!rangeIsPast) {
      // Parallel watermark fetch from both channels
      const guild = await client.guilds.fetch(GUILD_ID);
      const [tsChan, ctChan] = await Promise.all([
        guild.channels.fetch(TRADE_SUCCESS_CHANNEL_ID),
        guild.channels.fetch(CREATE_TRADES_CHANNEL_ID)
      ]);
      [wmTradeSuccess, wmCreateTrades] = await Promise.all([
        getChannelWatermark(tsChan),
        getChannelWatermark(ctChan)
      ]);
    }

    // --- Try cache ---
    if (supabaseClient) {
      const cached = await loadCache(supabaseClient, startDateStr, endDateStr);
      if (cached) {
        const isHit = rangeIsPast || (
          cached.watermarkTradeSuccess === wmTradeSuccess &&
          cached.watermarkCreateTrades === wmCreateTrades
        );
        if (isHit) {
          console.log(`[cache] HIT ${startDateStr}..${endDateStr}`);
          const { events, missedSnipes, marketFeedUnder3, allCreateTrades, chartStartMs: cSt, chartEndMs: cEn } = cached.payload;
          const html = generateHTML(events, daysBack, missedSnipes, {
            startTime: cSt,
            endTime:   cEn,
            channelBUnder3: marketFeedUnder3,
            allCreateTrades: allCreateTrades
          });
          return { html, cacheHit: true };
        }
        console.log(`[cache] STALE ${startDateStr}..${endDateStr} (watermarks changed)`);
      } else {
        console.log(`[cache] MISS ${startDateStr}..${endDateStr}`);
      }
    }

    // --- Full Discord fetch ---
    const [events, allCreateTrades] = await Promise.all([
      fetchMessagesInRange(client, rangeStartMs, rangeEndMs, TRADE_SUCCESS_CHANNEL_ID),
      fetchMessagesInRange(client, rangeStartMs, rangeEndMs, CREATE_TRADES_CHANNEL_ID)
    ]);
    const marketFeedUnder3 = filterCreateTradesMarkupAtMost3(allCreateTrades);
    const gotIds = new Set(events.map((e) => (e.tradeId || '').trim()).filter(Boolean));
    const missedSnipes = marketFeedUnder3.filter((e) => {
      const tid = (e.tradeId || '').trim();
      if (gotIds.has(tid)) return false;
      if (isBlacklisted(e)) return false;
      const price = parsePriceFromMarketFeed(e);
      if (price != null && (price < MISSED_PRICE_MIN || price > MISSED_PRICE_MAX)) return false;
      return true;
    });

    // --- Upsert cache ---
    if (supabaseClient) {
      // Get watermarks for current-day ranges if not fetched yet
      if (rangeIsPast) {
        try {
          const guild = await client.guilds.fetch(GUILD_ID);
          const [tsChan, ctChan] = await Promise.all([
            guild.channels.fetch(TRADE_SUCCESS_CHANNEL_ID),
            guild.channels.fetch(CREATE_TRADES_CHANNEL_ID)
          ]);
          [wmTradeSuccess, wmCreateTrades] = await Promise.all([
            getChannelWatermark(tsChan),
            getChannelWatermark(ctChan)
          ]);
        } catch (_) {}
      }
      await upsertCache(supabaseClient, startDateStr, endDateStr, {
        events, missedSnipes, marketFeedUnder3, allCreateTrades,
        chartStartMs: rangeStartMs,
        chartEndMs:   rangeEndMs
      }, wmTradeSuccess, wmCreateTrades);
    }

    const html = generateHTML(events, daysBack, missedSnipes, {
      startTime: rangeStartMs,
      endTime:   rangeEndMs,
      channelBUnder3: marketFeedUnder3,
      allCreateTrades: allCreateTrades
    });
    return { html, cacheHit: false };
  } finally {
    await client.destroy().catch(() => {});
  }
}

/**
 * Legacy helper: rolling N-day window from now.
 * Converts to a date range and delegates to getHtmlForDateRange.
 */
async function getHtml(daysBack) {
  const days = Math.min(30, Math.max(1, Math.floor(Number(daysBack)) || 1));
  const now   = new Date();
  const endDateStr   = now.toISOString().slice(0, 10);
  const startUTC     = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - (days - 1)));
  const startDateStr = startUTC.toISOString().slice(0, 10);
  const { html } = await getHtmlForDateRange(startDateStr, endDateStr, null);
  return html;
}

/**
 * Main function
 */
async function main() {
  // Parse command line arguments
  const args = process.argv.slice(2);
  const daysBack = args[0] ? parseInt(args[0]) : 7;
  
  if (isNaN(daysBack) || daysBack <= 0) {
    console.error('❌ Invalid number of days. Please provide a positive integer.');
    process.exit(1);
  }

  if (!DISCORD_BOT_TOKEN) {
    console.error('❌ Set DISCORD_BOT_TOKEN in your environment (see .env.example).');
    process.exit(1);
  }
  
  console.log('🚀 Starting Discord message fetcher...');
  console.log(`📅 Analyzing last ${daysBack} days\n`);
  
  // Create Discord client
  const client = new Client({
    intents: [
      GatewayIntentBits.Guilds,
      GatewayIntentBits.GuildMessages,
      GatewayIntentBits.MessageContent
    ]
  });
  
  try {
    // Login to Discord
    console.log('🔑 Logging in to Discord...');
    await client.login(DISCORD_BOT_TOKEN);
    console.log(`✅ Logged in as: ${client.user.tag}\n`);
    
    // Fetch trade-success (our buys)
    const events = await fetchMessages(client, daysBack, TRADE_SUCCESS_CHANNEL_ID);

    // Show distribution of WS Hostname, WS Backend, POST Backend, POST Hostname
    printDistribution(events);

    // Fetch create-trades and keep only markup ≤ 3%
    const { under3: marketFeedUnder3, all: allCreateTrades } = await fetchMarketFeedUnder3(client, daysBack);
    const gotIds = new Set(events.map((e) => (e.tradeId || '').trim()).filter(Boolean));
    // Missed = create-trades ≤3%, not got, not blacklisted, price in range (30–1200). Used for chart and table.
    const missedSnipes = marketFeedUnder3.filter((e) => {
      const tid = (e.tradeId || '').trim();
      if (gotIds.has(tid)) return false;
      if (isBlacklisted(e)) return false;
      const price = parsePriceFromMarketFeed(e);
      if (price != null && (price < MISSED_PRICE_MIN || price > MISSED_PRICE_MAX)) return false;
      return true;
    });
    const blacklistedCount = marketFeedUnder3.filter((e) => !gotIds.has((e.tradeId || '').trim()) && isBlacklisted(e)).length;
    const outOfRangeCount = marketFeedUnder3.filter((e) => {
      if (gotIds.has((e.tradeId || '').trim()) || isBlacklisted(e)) return false;
      const price = parsePriceFromMarketFeed(e);
      return price != null && (price < MISSED_PRICE_MIN || price > MISSED_PRICE_MAX);
    }).length;
    console.log(`\n📉 Missed snipes (create-trades ≤3%, not in trade-success, not blacklisted, price ${MISSED_PRICE_MIN}–${MISSED_PRICE_MAX}): ${missedSnipes.length}`);
    if (blacklistedCount > 0) console.log(`   (Excluded ${blacklistedCount} blacklisted)`);
    if (outOfRangeCount > 0) console.log(`   (Excluded ${outOfRangeCount} outside price range ${MISSED_PRICE_MIN}–${MISSED_PRICE_MAX})`);

    if (events.length === 0) {
      console.log('⚠️  No trade events found. Exiting.');
      await client.destroy();
      return;
    }

    // Generate HTML: chart and table use missedSnipes (price-filtered 30–1200)
    console.log('\n📊 Generating interactive chart...');
    const chartEndTime = Date.now();
    const chartStartTime = chartEndTime - (daysBack * 24 * 60 * 60 * 1000);
    const html = generateHTML(events, daysBack, missedSnipes, {
      startTime: chartStartTime,
      endTime: chartEndTime,
      channelBUnder3: marketFeedUnder3,
      allCreateTrades: allCreateTrades
    });
    
    // Save HTML file
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
    const filename = `snipes-by-hostname-${daysBack}days-${timestamp}.html`;
    const filepath = path.join(__dirname, filename);
    
    fs.writeFileSync(filepath, html);
    console.log(`✅ Chart saved to: ${filepath}`);
    
    // Print summary (each hostname = one class)
    const hostnameCounts = {};
    events.forEach(e => {
      const h = e.type || 'N/A';
      hostnameCounts[h] = (hostnameCounts[h] || 0) + 1;
    });
    console.log('\n📈 Summary by WS Hostname (class):');
    Object.entries(hostnameCounts).sort((a, b) => b[1] - a[1]).forEach(([h, count]) => {
      console.log(`   ${h}: ${count} events`);
    });
    console.log(`   Total: ${events.length} events`);
    
    const senkoMachineCounts = {};
    events.forEach(event => {
      const hostVM = event.hostVM || 'N/A';
      if (hostVM.includes('senko.network')) {
        senkoMachineCounts[hostVM] = (senkoMachineCounts[hostVM] || 0) + 1;
      }
    });
    if (Object.keys(senkoMachineCounts).length > 0) {
      console.log('\n🖥️  Senko Machine Breakdown:');
      Object.entries(senkoMachineCounts)
        .sort((a, b) => b[1] - a[1])
        .forEach(([machine, count]) => {
          console.log(`   ${machine}: ${count} events`);
        });
    }
    
    // Destroy client
    await client.destroy();
    console.log('\n✅ Done!');
    
  } catch (error) {
    console.error('❌ Error:', error);
    await client.destroy();
    process.exit(1);
  }
}

// Run the script
if (require.main === module) {
  main();
}

module.exports = {
  parseTradeMessage,
  parseMarketFeedMessage,
  getMessageContent,
  isBlacklisted,
  fetchMessages,
  fetchMessagesInRange,
  fetchMarketFeedUnder3,
  generateHTML,
  getHtml,
  getHtmlForDateRange
};

