
    import { initializeApp } from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js';
    import { getAuth, onAuthStateChanged, signOut } from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js';
    import { getFirestore, doc, getDoc, setDoc, serverTimestamp, collection, getDocs, deleteDoc } from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js';
    import { getStorage, ref as storageRef, uploadBytes, getDownloadURL } from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-storage.js';

    const app = initializeApp(window.FIREBASE_CONFIG);
    const auth = getAuth(app);
    const db = getFirestore(app);
    const storage = getStorage(app);

    let me = null;
    let myStaff = null;
    let garagesCache = [];

    const $ = (id) => document.getElementById(id);
    const slugify = (s='') => String(s).toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g,'').replace(/[^a-z0-9]+/g,'-').replace(/^-+|-+$/g,'').slice(0,60);

    function setMsg(type, text){ const box = $('createMsg'); if(!box) return; box.className = type; box.textContent = text; }
    function setPageStatus(text){ const el=$('pageStatus'); if(el) el.textContent=text||''; }
    function updateSuperLogoPreview(url){ const img = $('gLogoPreview'); if(img) img.src = String(url || '').trim() || 'assets/logo.png'; }

    function formData(){
      return {
        garageId: $('editingGarageId').value.trim(),
        name: $('gName').value.trim(),
        slug: slugify($('gSlug').value.trim() || $('gName').value.trim()),
        phone: $('gPhone').value.trim(),
        email: $('gEmail').value.trim(),
        address: $('gAddress').value.trim(),
        logoUrl: $('gLogo').value.trim(),
        tpsNumber: $('gTpsNo').value.trim(),
        tvqNumber: $('gTvqNo').value.trim(),
        tpsRate: Number($('gTps').value || 0),
        tvqRate: Number($('gTvq').value || 0),
        laborRate: Number($('gLabor').value || 0),
        cardFeeRate: Number($('gCardFee').value || 0),
        plan: $('gPlan').value,
        status: $('gStatus').value,
        active: $('gStatus').value !== 'inactive',
        adminName: $('gAdminName').value.trim(),
        adminEmail: $('gAdminEmail').value.trim(),
        notes: $('gNotes').value.trim(),
      };
    }

    function resetForm(){
      $('editingGarageId').value='';
      ['gName','gSlug','gPhone','gEmail','gAddress','gLogo','gTpsNo','gTvqNo','gAdminName','gAdminEmail','gNotes'].forEach(id=>$(id).value='');
      $('gPlan').value='pro'; $('gStatus').value='active'; $('gTps').value='0.05'; $('gTvq').value='0.09975'; $('gLabor').value='80'; $('gCardFee').value='0.025';
      $('formTitle').textContent='Créer un garage';
      $('editBadge').style.display='none';
      $('btnCancelEdit').style.display='none';
      $('btnSave').textContent='Créer le garage';
      $('gSlug').disabled=false;
      updateSuperLogoPreview('');
      setMsg('', '');
    }

    function enterEditMode(g){
      $('editingGarageId').value = g.id || g.slug || '';
      $('gName').value = g.name || g.garageName || '';
      $('gSlug').value = g.id || g.slug || '';
      $('gPhone').value = g.phone || g.garagePhone || '';
      $('gEmail').value = g.email || g.garageEmail || '';
      $('gAddress').value = g.address || g.garageAddress || '';
      $('gLogo').value = g.logoUrl || g.garageLogoUrl || '';
      $('gTpsNo').value = g.tpsNumber || g.garageTpsNo || '';
      $('gTvqNo').value = g.tvqNumber || g.garageTvqNo || '';
      $('gTps').value = g.tpsRate ?? 0.05;
      $('gTvq').value = g.tvqRate ?? 0.09975;
      $('gLabor').value = g.laborRate ?? 80;
      $('gCardFee').value = g.cardFeeRate ?? 0.025;
      $('gPlan').value = g.plan || 'pro';
      $('gStatus').value = g.status || (g.active === false ? 'inactive' : 'active');
      $('gAdminName').value = g.adminName || '';
      $('gAdminEmail').value = g.adminEmail || '';
      $('gNotes').value = g.notes || '';
      $('formTitle').textContent = `Modifier le garage: ${g.name || g.garageName || g.id}`;
      $('editBadge').style.display='inline-flex';
      $('btnCancelEdit').style.display='inline-block';
      $('btnSave').textContent='Enregistrer les modifications';
      $('gSlug').disabled=true;
      updateSuperLogoPreview(g.logoUrl || g.garageLogoUrl || '');
      window.scrollTo({ top: 0, behavior: 'smooth' });
      setMsg('hint', 'Mode modification activé.');
    }

    async function uploadSuperLogoFile(){
      if(!me) throw new Error('not-authenticated');
      const file = $('gLogoFile')?.files?.[0];
      if(!file) throw new Error("Choisis une image d'abord.");
      const safeName = String(file.name || 'logo').replace(/[^a-zA-Z0-9._-]+/g,'-');
      const data = formData();
      const slug = data.garageId || data.slug || 'garage';
      const path = `garage-logos/${slug}/${Date.now()}-${safeName}`;
      const ref = storageRef(storage, path);
      await uploadBytes(ref, file, { contentType: file.type || 'application/octet-stream' });
      const url = await getDownloadURL(ref);
      $('gLogo').value = url; updateSuperLogoPreview(url); return url;
    }

    async function loadStaff(uid){
      const snap = await getDoc(doc(db,'staff',uid));
      if(!snap.exists()) throw new Error('Document staff introuvable pour cet utilisateur.');
      return snap.data();
    }

    function renderGarages(garages){
      const list = $('garagesList');
      if(!garages.length){ list.innerHTML = '<div class="muted">Aucun garage trouvé.</div>'; return; }
      garages.sort((a,b)=> String(a.name || a.garageName || a.id).localeCompare(String(b.name || b.garageName || b.id), 'fr'));
      list.innerHTML = garages.map(g => `
        <article class="garage">
          <div class="garage-head">
            <div style="display:flex;gap:12px;align-items:center">
              <img src="${g.logoUrl || g.garageLogoUrl || 'assets/logo.png'}" alt="logo" style="height:56px;width:56px;object-fit:cover;border-radius:14px;border:1px solid var(--line);background:#fff">
              <div>
                <div style="font-weight:900;font-size:18px">${g.name || g.garageName || g.id}</div>
                <div class="small">ID: ${g.id}</div>
              </div>
            </div>
            <span class="badge ${g.active === false || g.status === 'inactive' ? 'off' : ''}">${g.status || (g.active === false ? 'inactive' : 'active')}</span>
          </div>
          <div class="mini">
            <div><b>Plan</b><br>${g.plan || '-'}</div>
            <div><b>Slug</b><br>${g.slug || g.id || '-'}</div>
            <div><b>Email</b><br>${g.email || g.garageEmail || '-'}</div>
            <div><b>Téléphone</b><br>${g.phone || g.garagePhone || '-'}</div>
            <div><b>TPS / TVQ</b><br>${g.tpsNumber || g.garageTpsNo || '-'} / ${g.tvqNumber || g.garageTvqNo || '-'}</div>
            <div><b>Main-d'œuvre</b><br>${g.laborRate ?? '-'} $/h</div>
            <div style="grid-column:1/-1"><b>Adresse</b><br>${g.address || g.garageAddress || '-'}</div>
          </div>
          <div class="list-actions">
            <button class="ghost" data-action="edit" data-id="${g.id}">Modifier</button>
            <button class="ghost" data-action="open" data-id="${g.id}">Ouvrir</button>
            <button class="danger" data-action="delete" data-id="${g.id}">Supprimer</button>
          </div>
        </article>`).join('');

      list.querySelectorAll('button[data-action]').forEach(btn => {
        btn.addEventListener('click', async () => {
          const id = btn.dataset.id;
          const action = btn.dataset.action;
          const garage = garagesCache.find(x => x.id === id);
          if(action === 'edit' && garage) return enterEditMode(garage);
          if(action === 'open') return window.location.href = `./index.html?garageId=${encodeURIComponent(id)}`;
          if(action === 'delete') return await deleteGarage(id);
        });
      });
    }

    async function loadGarages(){
      $('garagesList').innerHTML = '<div class="muted">Chargement des garages...</div>';
      setPageStatus('Chargement de la liste des garages...');
      try {
        const qs = await getDocs(collection(db,'garages'));
        garagesCache = await Promise.all(qs.docs.map(async (d) => {
          const root = d.data() || {};
          let main = {};
          try {
            const mainSnap = await getDoc(doc(db,'garages',d.id,'settings','main'));
            if (mainSnap.exists()) main = mainSnap.data() || {};
          } catch(err) {}
          return { id:d.id, ...root, ...main };
        }));
        renderGarages(garagesCache);
        setPageStatus(`Garages chargés: ${garagesCache.length}`);
      } catch(e) {
        console.error('loadGarages', e);
        $('garagesList').innerHTML = `<div class="error">Impossible de charger les garages.<br>Erreur: ${e?.code || ''} ${e?.message || e}<br><br>Vérifie que les règles Firestore sont publiées et que ton compte a bien <b>role: "superadmin"</b> dans <b>staff/{uid}</b>.</div>`;
        setPageStatus('Erreur chargement garages');
      }
    }

    async function saveGarage(){
      try{
        const data = formData();
        if(!data.name) throw new Error('Nom obligatoire.');
        const garageId = data.garageId || data.slug;
        if(!garageId) throw new Error('Slug / ID garage obligatoire.');
        const isEdit = Boolean(data.garageId);
        setMsg('hint', isEdit ? 'Modification en cours...' : 'Création en cours...');
        setPageStatus(isEdit ? 'Modification du garage...' : 'Création du garage...');

        const garageRef = doc(db,'garages',garageId);
        const exists = await getDoc(garageRef);
        if(!isEdit && exists.exists()) throw new Error('Ce garage existe déjà.');
        if(isEdit && !exists.exists()) throw new Error('Garage introuvable.');

        const payload = {
          name: data.name,
          garageName: data.name,
          slug: garageId,
          phone: data.phone,
          garagePhone: data.phone,
          email: data.email,
          garageEmail: data.email,
          address: data.address,
          garageAddress: data.address,
          logoUrl: data.logoUrl,
          garageLogoUrl: data.logoUrl,
          tpsNumber: data.tpsNumber,
          tvqNumber: data.tvqNumber,
          garageTpsNo: data.tpsNumber,
          garageTvqNo: data.tvqNumber,
          tpsRate: data.tpsRate,
          tvqRate: data.tvqRate,
          laborRate: data.laborRate,
          cardFeeRate: data.cardFeeRate,
          plan: data.plan,
          status: data.status,
          active: data.active,
          adminName: data.adminName,
          adminEmail: data.adminEmail,
          notes: data.notes,
          updatedAt: serverTimestamp(),
          updatedBy: me.uid
        };
        if(!isEdit){ payload.createdAt = serverTimestamp(); payload.createdBy = me.uid; }

        await setDoc(garageRef, payload, { merge:true });
        await setDoc(doc(db,'garages',garageId,'settings','main'), {
          garageName: data.name,
          garageId,
          garageAddress: data.address,
          address: data.address,
          garagePhone: data.phone,
          phone: data.phone,
          garageEmail: data.email,
          email: data.email,
          logoUrl: data.logoUrl,
          garageLogoUrl: data.logoUrl,
          garageTpsNo: data.tpsNumber,
          garageTvqNo: data.tvqNumber,
          tpsNumber: data.tpsNumber,
          tvqNumber: data.tvqNumber,
          tpsRate: data.tpsRate,
          tvqRate: data.tvqRate,
          laborRate: data.laborRate,
          cardFeeRate: data.cardFeeRate,
          plan: data.plan,
          status: data.status,
          active: data.active,
          adminName: data.adminName,
          adminEmail: data.adminEmail,
          notes: data.notes,
          updatedAt: serverTimestamp(),
          updatedBy: me.uid
        }, { merge:true });
        await setDoc(doc(db,'garages',garageId,'settings','counters'), {
          garageId,
          invoiceNext: 1,
          updatedAt: serverTimestamp(),
          updatedBy: me.uid
        }, { merge:true });

        setMsg('ok', isEdit ? 'Garage modifié avec succès.' : 'Garage créé avec succès.');
        setPageStatus(isEdit ? 'Garage modifié avec succès' : 'Garage créé avec succès');
        await loadGarages();
        resetForm();
      }catch(e){
        console.error(e);
        setMsg('error', e?.code ? `${e.code} — ${e.message || 'Erreur.'}` : (e.message || 'Erreur.'));
        setPageStatus('Erreur sauvegarde garage');
      }
    }

    async function deleteGarage(garageId){
      if(!confirm(`Supprimer le garage ${garageId} ?

Attention: cela supprime seulement le document principal du garage. Les sous-collections éventuelles devront être supprimées séparément.`)) return;
      try {
        setPageStatus(`Suppression du garage ${garageId}...`);
        await deleteDoc(doc(db,'garages',garageId));
        if($('editingGarageId').value === garageId) resetForm();
        setMsg('ok', `Garage ${garageId} supprimé.`);
        await loadGarages();
        setPageStatus('Garage supprimé');
      } catch(e) {
        console.error(e);
        setMsg('error', e?.message || 'Erreur suppression garage.');
        setPageStatus('Erreur suppression garage');
      }
    }

    $('gLogoFile').addEventListener('change', async (e) => {
      const file = e.target?.files?.[0];
      if (!file) return;
      try { updateSuperLogoPreview(URL.createObjectURL(file)); } catch(_) {}
    });
    $('btnUploadLogo').addEventListener('click', async () => {
      try { setMsg('hint', 'Upload du logo en cours...'); await uploadSuperLogoFile(); setMsg('ok', 'Logo uploadé.'); }
      catch(e) { console.error(e); setMsg('error', e.message || 'Erreur upload logo.'); }
    });

    $('btnSave').addEventListener('click', saveGarage);
    $('btnCancelEdit').addEventListener('click', resetForm);
    $('btnReload').addEventListener('click', loadGarages);
    $('btnReloadTop').addEventListener('click', loadGarages);
    $('btnReloadList').addEventListener('click', loadGarages);
    $('gName').addEventListener('input', () => { if(!$('editingGarageId').value && !$('gSlug').value.trim()) $('gSlug').value = slugify($('gName').value); });
    $('gLogo').addEventListener('input', (e) => updateSuperLogoPreview(e.target.value));
    $('btnLogout').addEventListener('click', async () => {
      try { await signOut(auth); window.location.replace('./index.html'); }
      catch(e){ console.error(e); setMsg('error', 'Erreur déconnexion.'); }
    });

    onAuthStateChanged(auth, async (user) => {
      setPageStatus('Vérification de la session...');
      if(!user){ window.location.replace('./index.html'); return; }
      me = user;
      try{
        myStaff = await loadStaff(user.uid);
        $('who').textContent = `${myStaff.displayName || user.email} — rôle: ${myStaff.role || '-'} — garage: ${myStaff.garageId || '-'}`;
        if(myStaff.role !== 'superadmin'){ setPageStatus('Compte non superadmin'); window.location.replace('./index.html'); return; }
        setPageStatus('Session superadmin validée');
        resetForm();
        await loadGarages();
      }catch(e){
        console.error(e);
        $('garagesList').innerHTML = `<div class="error">${e?.code || ''} ${e.message || 'Erreur chargement staff.'}</div>`;
        setPageStatus('Erreur lecture staff');
      }
    });
  