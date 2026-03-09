const ADMINS_COLLECTION = 'admins';
const RESERVATIONS_COLLECTION = 'reservations';

let db = null;
let auth = null;
let reservationsCache = [];
let unsubscribeReservations = null;
let currentUser = null;
let currentAdminDoc = null;

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

function formatDateTime(value) {
  if (!value) return '—';
  if (typeof value?.toDate === 'function') {
    return value.toDate().toLocaleString('fr-CA', {
      year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit'
    });
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString('fr-CA', {
    year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit'
  });
}

function isToday(value) {
  let date = null;
  if (value && typeof value?.toDate === 'function') date = value.toDate();
  else if (value) date = new Date(value);
  if (!date || Number.isNaN(date.getTime())) return false;
  const now = new Date();
  return date.getFullYear() === now.getFullYear()
    && date.getMonth() === now.getMonth()
    && date.getDate() === now.getDate();
}

function showInlineMessage(id, text, isError = false) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = text;
  el.classList.remove('hidden');
  el.classList.toggle('error-message', !!isError);
  el.classList.toggle('success-message', !isError);
}

function hideInlineMessage(id) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.add('hidden');
}

function initThemeAndPwa() {
  const selector = document.getElementById('themeSelector');
  if (selector) {
    const saved = localStorage.getItem('theme');
    if (saved) {
      document.body.classList.add('theme-' + saved);
      selector.value = saved;
    }
    selector.addEventListener('change', () => {
      const theme = selector.value;
      document.body.className = '';
      document.body.classList.add('theme-' + theme);
      localStorage.setItem('theme', theme);
    });
  }

  let deferredPrompt;
  const installBtn = document.getElementById('installBtn');
  if (installBtn) {
    window.addEventListener('beforeinstallprompt', (e) => {
      e.preventDefault();
      deferredPrompt = e;
      installBtn.style.display = 'block';
    });

    installBtn.addEventListener('click', async () => {
      installBtn.style.display = 'none';
      if (deferredPrompt) {
        deferredPrompt.prompt();
        deferredPrompt = null;
      }
    });
  }

  const isIos = /iphone|ipad|ipod/i.test(window.navigator.userAgent);
  const isInStandaloneMode = window.matchMedia('(display-mode: standalone)').matches || (('standalone' in window.navigator) && window.navigator.standalone);
  document.body.classList.toggle('standalone-app', !!isInStandaloneMode);
  const iosBanner = document.getElementById('iosBanner');
  if (iosBanner && isIos && !isInStandaloneMode) {
    iosBanner.style.display = 'block';
    const closeBtn = document.getElementById('closeIosBanner');
    if (closeBtn) closeBtn.addEventListener('click', () => iosBanner.style.display = 'none');
  }

  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('./sw.js', { scope: './' }).catch(() => {});
  }
}

function initFirebase() {
  if (!window.firebase || !window.FIREBASE_CONFIG) {
    console.error('Firebase SDK ou config absente.');
    return false;
  }

  if (!firebase.apps.length) {
    firebase.initializeApp(window.FIREBASE_CONFIG);
  }

  db = firebase.firestore();
  auth = firebase.auth();
  return true;
}

async function searchAddress(query, container) {
  if (!container || query.length < 3) {
    if (container) container.style.display = 'none';
    return;
  }

  const url = `https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(query)}&addressdetails=1&limit=5&countrycodes=ca`;
  try {
    const res = await fetch(url, { headers: { 'Accept-Language': 'fr' } });
    const results = await res.json();
    container.innerHTML = '';
    container.style.display = results.length ? 'block' : 'none';

    results.forEach((place) => {
      const div = document.createElement('div');
      div.textContent = place.display_name;
      div.addEventListener('click', () => {
        const input = container.previousElementSibling;
        input.value = place.display_name;
        container.style.display = 'none';
      });
      container.appendChild(div);
    });
  } catch {
    container.style.display = 'none';
  }
}

function setupAutocomplete(inputId, resultsId) {
  const input = document.getElementById(inputId);
  const results = document.getElementById(resultsId);
  if (!input || !results) return;

  input.addEventListener('input', () => searchAddress(input.value, results));
  document.addEventListener('click', (e) => {
    if (!results.contains(e.target) && e.target !== input) {
      results.style.display = 'none';
    }
  });
}

function initReservationPage() {
  const form = document.getElementById('reservationForm');
  if (!form) return;

  setupAutocomplete('depart', 'depart-results');
  setupAutocomplete('arrivee', 'arrivee-results');
  setupAutocomplete('retourDepart', 'retour-depart-results');
  setupAutocomplete('retourArrivee', 'retour-arrivee-results');

  const allerRetourCheckbox = document.getElementById('allerRetour');
  const retourFields = document.getElementById('retourFields');
  const retourRequiredFields = [
    document.getElementById('retourDepart'),
    document.getElementById('retourArrivee'),
    document.getElementById('retourHeure')
  ].filter(Boolean);
  const passagersInput = document.getElementById('passagers');
  const valisesInput = document.getElementById('valises');
  const vehiculeSelect = document.getElementById('vehicule');
  const submitBtn = document.getElementById('submitBtn');

  function toggleRetourFields() {
    const enabled = allerRetourCheckbox?.checked;
    retourFields?.classList.toggle('hidden', !enabled);
    retourRequiredFields.forEach((field) => { field.required = !!enabled; });
  }

  function suggestVehicle() {
    const passagers = Number(passagersInput?.value || 0);
    const valises = Number(valisesInput?.value || 0);
    if (!vehiculeSelect) return;
    vehiculeSelect.value = (passagers > 4 || valises > 3) ? 'van' : 'berline';
  }

  allerRetourCheckbox?.addEventListener('change', toggleRetourFields);
  passagersInput?.addEventListener('input', suggestVehicle);
  valisesInput?.addEventListener('input', suggestVehicle);
  toggleRetourFields();
  suggestVehicle();

  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    hideInlineMessage('confirmation');

    if (!db) {
      showInlineMessage('confirmation', 'Firebase n’est pas configuré.', true);
      return;
    }

    if (submitBtn) {
      submitBtn.disabled = true;
      submitBtn.textContent = 'Enregistrement...';
    }

    const reservation = {
      nom: document.getElementById('nom')?.value?.trim() || '',
      telephone: document.getElementById('telephone')?.value?.trim() || '',
      email: document.getElementById('email')?.value?.trim() || '',
      passagers: Number(document.getElementById('passagers')?.value || 1),
      numeroVol: document.getElementById('numeroVol')?.value?.trim() || '',
      depart: document.getElementById('depart')?.value?.trim() || '',
      arrivee: document.getElementById('arrivee')?.value?.trim() || '',
      heure: document.getElementById('heure')?.value || '',
      vehicule: document.getElementById('vehicule')?.value || 'berline',
      valises: Number(document.getElementById('valises')?.value || 0),
      notes: document.getElementById('notes')?.value?.trim() || '',
      allerRetour: !!document.getElementById('allerRetour')?.checked,
      retourDepart: document.getElementById('retourDepart')?.value?.trim() || '',
      retourArrivee: document.getElementById('retourArrivee')?.value?.trim() || '',
      retourHeure: document.getElementById('retourHeure')?.value || '',
      retourNumeroVol: document.getElementById('retourNumeroVol')?.value?.trim() || '',
      retourDetails: document.getElementById('retourDetails')?.value?.trim() || '',
      status: 'en_attente',
      createdAt: firebase.firestore.FieldValue.serverTimestamp(),
      createdAtClient: new Date().toISOString(),
      source: 'site-web'
    };

    try {
      await db.collection(RESERVATIONS_COLLECTION).add(reservation);
      form.reset();
      toggleRetourFields();
      suggestVehicle();
      showInlineMessage('confirmation', '✅ Réservation envoyée et sauvegardée dans Firebase.', false);
    } catch (error) {
      showInlineMessage('confirmation', 'Erreur Firebase : ' + (error.message || 'enregistrement impossible'), true);
    } finally {
      if (submitBtn) {
        submitBtn.disabled = false;
        submitBtn.textContent = 'Réserver maintenant';
      }
    }
  });
}

function reservationSearchBlob(item) {
  return [
    item.nom, item.telephone, item.email, item.numeroVol, item.depart, item.arrivee,
    item.notes, item.retourNumeroVol, item.retourDepart, item.retourArrivee, item.status
  ].join(' ').toLowerCase();
}

function renderReservations() {
  const list = document.getElementById('reservationsList');
  if (!list) return;

  const searchValue = (document.getElementById('searchReservation')?.value || '').toLowerCase();
  const tripFilter = document.getElementById('tripFilter')?.value || 'all';
  const statusFilter = document.getElementById('statusFilter')?.value || 'all';
  const emptyState = document.getElementById('emptyState');

  const filtered = reservationsCache.filter((item) => {
    const matchesSearch = reservationSearchBlob(item).includes(searchValue);
    const tripType = item.allerRetour ? 'aller-retour' : 'aller-simple';
    const matchesTrip = tripFilter === 'all' || tripFilter === tripType;
    const matchesStatus = statusFilter === 'all' || item.status === statusFilter;
    return matchesSearch && matchesTrip && matchesStatus;
  });

  const statusCounts = {
    en_attente: reservationsCache.filter((item) => item.status === 'en_attente').length,
    confirmee: reservationsCache.filter((item) => item.status === 'confirmee').length,
    terminee: reservationsCache.filter((item) => item.status === 'terminee').length
  };

  const setText = (id, value) => {
    const el = document.getElementById(id);
    if (el) el.textContent = String(value);
  };

  setText('statTotal', reservationsCache.length);
  setText('statRoundTrip', reservationsCache.filter((item) => item.allerRetour).length);
  setText('statToday', reservationsCache.filter((item) => isToday(item.createdAt || item.createdAtClient)).length);
  setText('statPending', statusCounts.en_attente);

  if (!filtered.length) {
    list.innerHTML = '';
    if (emptyState) emptyState.style.display = 'block';
    return;
  }

  if (emptyState) emptyState.style.display = 'none';
  list.innerHTML = filtered.map((item) => `
    <article class="reservation-card">
      <div class="reservation-header">
        <div>
          <h3>${escapeHtml(item.nom || 'Sans nom')}</h3>
          <p>${escapeHtml(item.allerRetour ? 'Aller-retour' : 'Aller simple')} • ${escapeHtml(item.vehicule || 'berline')} • ${escapeHtml(item.statusLabel || item.status || 'en_attente')}</p>
        </div>
        <button class="danger-btn small-btn" data-delete-id="${escapeHtml(item.id)}">Supprimer</button>
      </div>
      <div class="reservation-grid">
        <div><strong>Téléphone :</strong> ${escapeHtml(item.telephone || '—')}</div>
        <div><strong>Email :</strong> ${escapeHtml(item.email || '—')}</div>
        <div><strong>Passagers :</strong> ${escapeHtml(item.passagers || '—')}</div>
        <div><strong>Valises :</strong> ${escapeHtml(item.valises || '—')}</div>
        <div><strong>Vol :</strong> ${escapeHtml(item.numeroVol || '—')}</div>
        <div><strong>Date/heure :</strong> ${escapeHtml(formatDateTime(item.heure))}</div>
        <div><strong>Départ :</strong> ${escapeHtml(item.depart || '—')}</div>
        <div><strong>Arrivée :</strong> ${escapeHtml(item.arrivee || '—')}</div>
        <div><strong>Créée le :</strong> ${escapeHtml(formatDateTime(item.createdAt || item.createdAtClient))}</div>
        <div><strong>Notes :</strong> ${escapeHtml(item.notes || '—')}</div>
        <div>
          <strong>Statut :</strong>
          <select class="status-select" data-status-id="${escapeHtml(item.id)}">
            <option value="en_attente" ${item.status === 'en_attente' ? 'selected' : ''}>En attente</option>
            <option value="confirmee" ${item.status === 'confirmee' ? 'selected' : ''}>Confirmée</option>
            <option value="en_cours" ${item.status === 'en_cours' ? 'selected' : ''}>En cours</option>
            <option value="terminee" ${item.status === 'terminee' ? 'selected' : ''}>Terminée</option>
            <option value="annulee" ${item.status === 'annulee' ? 'selected' : ''}>Annulée</option>
          </select>
        </div>
      </div>
      ${item.allerRetour ? `
        <div class="retour-summary">
          <h4>Retour</h4>
          <p><strong>Départ :</strong> ${escapeHtml(item.retourDepart || '—')}</p>
          <p><strong>Arrivée :</strong> ${escapeHtml(item.retourArrivee || '—')}</p>
          <p><strong>Date/heure :</strong> ${escapeHtml(formatDateTime(item.retourHeure))}</p>
          <p><strong>Vol retour :</strong> ${escapeHtml(item.retourNumeroVol || '—')}</p>
          <p><strong>Détails :</strong> ${escapeHtml(item.retourDetails || '—')}</p>
        </div>
      ` : ''}
    </article>
  `).join('');

  list.querySelectorAll('[data-delete-id]').forEach((button) => {
    button.addEventListener('click', async () => {
      const id = button.getAttribute('data-delete-id');
      if (!id || !db) return;
      if (!confirm('Supprimer cette réservation ?')) return;
      try {
        await db.collection(RESERVATIONS_COLLECTION).doc(id).delete();
      } catch (error) {
        alert('Suppression impossible : ' + (error.message || 'erreur'));
      }
    });
  });

  list.querySelectorAll('[data-status-id]').forEach((select) => {
    select.addEventListener('change', async () => {
      const id = select.getAttribute('data-status-id');
      if (!id || !db) return;
      try {
        await db.collection(RESERVATIONS_COLLECTION).doc(id).update({
          status: select.value,
          updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
          updatedBy: currentUser?.email || ''
        });
      } catch (error) {
        alert('Mise à jour impossible : ' + (error.message || 'erreur'));
      }
    });
  });
}

function mapReservationDoc(doc) {
  const data = doc.data() || {};
  const labels = {
    en_attente: 'En attente',
    confirmee: 'Confirmée',
    en_cours: 'En cours',
    terminee: 'Terminée',
    annulee: 'Annulée'
  };
  return {
    id: doc.id,
    ...data,
    status: data.status || 'en_attente',
    statusLabel: labels[data.status || 'en_attente'] || (data.status || 'En attente')
  };
}

async function verifyAdminAccess(user) {
  const doc = await db.collection(ADMINS_COLLECTION).doc(user.uid).get();
  if (!doc.exists) {
    throw new Error('Ton compte existe dans Firebase Auth, mais pas dans la collection admins.');
  }
  const data = doc.data() || {};
  if (!data.active) {
    throw new Error('Ton compte admin est désactivé.');
  }
  currentAdminDoc = data;
  const badge = document.getElementById('adminBadge');
  if (badge) badge.textContent = `${user.email} • ${data.role || 'admin'}`;
}

function subscribeReservations() {
  if (unsubscribeReservations) unsubscribeReservations();
  unsubscribeReservations = db.collection(RESERVATIONS_COLLECTION)
    .orderBy('createdAt', 'desc')
    .onSnapshot((snapshot) => {
      reservationsCache = snapshot.docs.map(mapReservationDoc);
      renderReservations();
    }, (error) => {
      const syncStatus = document.getElementById('syncStatus');
      if (syncStatus) syncStatus.textContent = 'Erreur de synchronisation : ' + (error.message || 'lecture impossible');
    });
}

function setDashboardVisibility(loggedIn) {
  const loginCard = document.getElementById('loginCard');
  const dashboardCard = document.getElementById('dashboardCard');
  if (loginCard) loginCard.classList.toggle('hidden', loggedIn);
  if (dashboardCard) dashboardCard.classList.toggle('hidden', !loggedIn);
}

function initDashboardPage() {
  const loginCard = document.getElementById('loginCard');
  const dashboardCard = document.getElementById('dashboardCard');
  if (!loginCard || !dashboardCard) return;

  if (!auth || !db) {
    showInlineMessage('loginError', 'Firebase n’est pas configuré.', true);
    return;
  }

  document.getElementById('loginBtn')?.addEventListener('click', async () => {
    hideInlineMessage('loginError');
    const email = document.getElementById('adminUser')?.value?.trim();
    const pass = document.getElementById('adminPass')?.value;
    try {
      await auth.signInWithEmailAndPassword(email, pass);
    } catch (error) {
      showInlineMessage('loginError', error.message || 'Connexion impossible.', true);
    }
  });

  document.getElementById('logoutBtn')?.addEventListener('click', async () => {
    await auth.signOut();
  });

  document.getElementById('searchReservation')?.addEventListener('input', renderReservations);
  document.getElementById('tripFilter')?.addEventListener('change', renderReservations);
  document.getElementById('statusFilter')?.addEventListener('change', renderReservations);

  document.getElementById('clearBtn')?.addEventListener('click', async () => {
    if (!db || !reservationsCache.length) return;
    if (!confirm('Supprimer toutes les réservations visibles ?')) return;
    const batch = db.batch();
    reservationsCache.forEach((item) => {
      batch.delete(db.collection(RESERVATIONS_COLLECTION).doc(item.id));
    });
    try {
      await batch.commit();
    } catch (error) {
      alert('Suppression globale impossible : ' + (error.message || 'erreur'));
    }
  });

  document.getElementById('exportBtn')?.addEventListener('click', () => {
    const data = JSON.stringify(reservationsCache, null, 2);
    const blob = new Blob([data], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'reservations-taxi-etoile-firebase.json';
    a.click();
    URL.revokeObjectURL(url);
  });

  auth.onAuthStateChanged(async (user) => {
    const syncStatus = document.getElementById('syncStatus');
    currentUser = user || null;
    currentAdminDoc = null;
    if (!user) {
      setDashboardVisibility(false);
      reservationsCache = [];
      renderReservations();
      if (unsubscribeReservations) {
        unsubscribeReservations();
        unsubscribeReservations = null;
      }
      if (syncStatus) syncStatus.textContent = 'Non connecté';
      return;
    }

    try {
      await verifyAdminAccess(user);
      setDashboardVisibility(true);
      if (syncStatus) syncStatus.textContent = 'Synchronisé en temps réel avec Firebase';
      subscribeReservations();
    } catch (error) {
      await auth.signOut();
      showInlineMessage('loginError', error.message || 'Accès refusé.', true);
      setDashboardVisibility(false);
    }
  });
}

document.addEventListener('DOMContentLoaded', () => {
  initThemeAndPwa();
  initFirebase();
  initReservationPage();
  initDashboardPage();
});
