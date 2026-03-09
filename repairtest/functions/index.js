/*
  Garage Pro One - Cloud Functions
  - Envoi de promotions par email via SendGrid

  Config:
    firebase functions:config:set sendgrid.key="..." sendgrid.from="ton@email.com"

  Deploy:
    cd functions && npm i
    firebase deploy --only functions
*/

const functions = require('firebase-functions');
const admin = require('firebase-admin');

admin.initializeApp();

function assert(condition, message) {
  if (!condition) {
    throw new functions.https.HttpsError('failed-precondition', message);
  }
}

async function isAdmin(uid) {
  const snap = await admin.firestore().doc(`users/${uid}`).get();
  return snap.exists && (snap.data().role === 'admin');
}

function renderTemplate(str, vars) {
  return String(str || '')
    .replace(/\{name\}/g, vars.name || '')
    .replace(/\{phone\}/g, vars.phone || '');
}

exports.sendPromotionEmail = functions.https.onCall(async (data, context) => {
  assert(context.auth && context.auth.uid, 'Non connecté.');
  const uid = context.auth.uid;
  assert(await isAdmin(uid), 'Accès refusé (admin uniquement).');

  const promotionId = String(data.promotionId || '').trim();
  assert(promotionId, 'promotionId manquant.');
  const testEmail = String(data.testEmail || '').trim();

  const cfg = functions.config();
  const apiKey = cfg?.sendgrid?.key;
  const from = cfg?.sendgrid?.from;
  assert(apiKey && from, 'SendGrid non configuré. Configure: sendgrid.key et sendgrid.from');

  const sgMail = require('@sendgrid/mail');
  sgMail.setApiKey(apiKey);

  const promoRef = admin.firestore().doc(`promotions/${promotionId}`);
  const promoSnap = await promoRef.get();
  assert(promoSnap.exists, 'Promotion introuvable.');
  const promo = promoSnap.data() || {};

  const subject = String(promo.subject || '').trim();
  const message = String(promo.message || '').trim();
  assert(subject && message, 'Promotion invalide (subject/message).');

  let recipients = [];

  if (testEmail && testEmail.includes('@')) {
    recipients = [{ email: testEmail, fullName: 'Test', phone: '' }];
  } else {
    // Collecte des clients avec email
    const snap = await admin.firestore().collection('customers').get();
    recipients = snap.docs
      .map(d => ({ id: d.id, ...(d.data() || {}) }))
      .filter(c => String(c.email || '').includes('@'))
      .map(c => ({ email: c.email, fullName: c.fullName || '', phone: c.phone || '' }));
  }

  const total = recipients.length;
  if (total === 0) {
    return { sent: 0, total: 0 };
  }

  // Envoi en petits lots (SendGrid limite selon ton plan)
  let sent = 0;
  const BATCH = 80;

  for (let i = 0; i < recipients.length; i += BATCH) {
    const chunk = recipients.slice(i, i + BATCH);

    // Une requête SendGrid avec plusieurs personalizations
    const personalizations = chunk.map(r => ({
      to: [{ email: r.email }],
      subject,
      dynamic_template_data: {
        name: r.fullName,
        phone: r.phone
      }
    }));

    // Pas besoin de template SendGrid: on render nous-mêmes (texte simple)
    const msgs = chunk.map(r => ({
      to: r.email,
      from,
      subject,
      text: renderTemplate(message, { name: r.fullName, phone: r.phone })
    }));

    // Envoi en parallèle limité
    await Promise.all(msgs.map(m => sgMail.send(m)));
    sent += chunk.length;
  }

  // Log minimal sur la promo
  if (!(testEmail && testEmail.includes('@'))) {
    await promoRef.set({
      lastSentAt: new Date().toISOString(),
      lastSentAtTs: admin.firestore.FieldValue.serverTimestamp(),
      sentCount: admin.firestore.FieldValue.increment(sent)
    }, { merge: true });
  }

  return { sent, total };
});
