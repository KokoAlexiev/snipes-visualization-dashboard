'use strict';

const { getHtml } = require('./lib/snipesCore.cjs');

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 204,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
      },
      body: ''
    };
  }

  if (event.httpMethod !== 'GET') {
    return { statusCode: 405, body: 'Method Not Allowed' };
  }

  try {
    const raw = event.queryStringParameters?.days ?? '1';
    const parsed = parseInt(raw, 10);
    const days = Number.isFinite(parsed) ? Math.min(30, Math.max(1, parsed)) : 1;
    const html = await getHtml(days);
    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'text/html; charset=utf-8',
        'X-Frame-Options': 'SAMEORIGIN',
        'Cache-Control': 'private, max-age=60'
      },
      body: html
    };
  } catch (err) {
    console.error('snipes-html error:', err);
    const message = err && err.message ? String(err.message) : 'Unknown error';
    return {
      statusCode: 500,
      headers: {
        'Content-Type': 'text/html; charset=utf-8',
        'X-Frame-Options': 'SAMEORIGIN'
      },
      body: `<!DOCTYPE html><html><head><meta charset="utf-8"><title>Error</title></head><body style="font-family:sans-serif;padding:2rem;"><h1>Dashboard error</h1><p>${message.replace(/</g, '&lt;')}</p><p>Check Netlify function logs and environment variables.</p></body></html>`
    };
  }
};
