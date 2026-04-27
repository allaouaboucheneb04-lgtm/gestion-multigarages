import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.5/firebase-app.js";
import { getAuth, onAuthStateChanged, signInWithEmailAndPassword, createUserWithEmailAndPassword, sendPasswordResetEmail, signOut } from "https://www.gstatic.com/firebasejs/10.12.5/firebase-auth.js";
import { getFirestore, doc, getDoc, setDoc, updateDoc, deleteDoc, collection, collectionGroup, query, where, orderBy, limit, getDocs, addDoc, serverTimestamp, onSnapshot, writeBatch, runTransaction } from "https://www.gstatic.com/firebasejs/10.12.5/firebase-firestore.js";

import { getFunctions, httpsCallable } from "https://www.gstatic.com/firebasejs/10.12.5/firebase-functions.js";
import { getStorage, ref as storageRef, uploadBytes, getDownloadURL } from "https://www.gstatic.com/firebasejs/10.12.5/firebase-storage.js";


function debugLogWrapper(){
  try{
    if(window.debugLog){
      const args=[...arguments].map(a=>typeof a==="object"?JSON.stringify(a):String(a)).join(" ");
      window.debugLog(args);
    }
  }catch(e){}
}
["log","warn","error"].forEach(k=>{
 const old=console[k];
 console[k]=function(){
   old.apply(console,arguments);
   debugLogWrapper(k.toUpperCase()+":",...arguments);
 }
});
(()=>{})("App.js chargé ✅");



// ===== Helper: normalizeEmail (added fix) =====
function normalizeEmail(data){
  const v =
    data?.email ??
    data?.Email ??
    data?.courriel ??
    data?.mail ??
    data?.Mail ??
    "";
  return String(v).trim().toLowerCase();
}
// ===== End helper =====


// ===== UI helpers: showToast + showModal alias =====
function showToast(message, duration=3500, type="info"){
  try{
    const msg = String(message ?? "");
    const toast = document.createElement("div");
    toast.textContent = msg;
    toast.style.position="fixed";
    toast.style.left="50%";
    toast.style.bottom="84px";
    toast.style.transform="translateX(-50%)";
    toast.style.background = (type==="error") ? "#b71c1c" : "#1b5e20";
    toast.style.color="#fff";
    toast.style.padding="10px 14px";
    toast.style.borderRadius="10px";
    toast.style.fontSize="14px";
    toast.style.zIndex="999999";
    toast.style.maxWidth="92vw";
    toast.style.textAlign="center";
    toast.style.boxShadow="0 6px 18px rgba(0,0,0,.25)";
    document.body.appendChild(toast);
    setTimeout(()=>{ try{ toast.remove(); }catch(e){} }, Number(duration)||3500);
  }catch(e){
    try{ alert(message); }catch(e2){}
  }
}
// showModal() is used across the codebase; keep it as an alias of openModal()
function showModal(title, html){ openModal(title, html); }
// ===== end UI helpers =====


/* ============
   Firebase init
=========== */
if (!window.FIREBASE_CONFIG) {
  document.getElementById("viewAuth").style.display = "";
  const err = document.getElementById("authError");
  err.style.display = "";
  err.textContent = "Configuration Firebase manquante. Ouvre assets/firebase-config.js et colle la config (voir README).";
  throw new Error("Missing FIREBASE_CONFIG");
}

const app = initializeApp(window.FIREBASE_CONFIG);
const auth = getAuth(app);
const db = getFirestore(app);
const storage = getStorage(app);
const functions = getFunctions(app);

/* ============
   Roles (admin / mechanic)
=========== */
// role is stored in Firestore at /users/{uid}.role
// possible values: "admin" | "mechanic"
let currentRole = "unknown";
let currentUserName = "";
let currentGarageId = "";
let unsubProfile = null;
let mechanics = [];
let unsubStaffLive = null;
let unsubInvitesLive = null;
let staffLiveRows = [];
let invitesLiveRows = [];

let roleNeedsSetup = false;
let roleSetupShown = false;

function normalizeRole(raw) {
  const r = String(raw || "").toLowerCase().trim();
  // tolérance aux fautes fréquentes
  if (r === "mecanic" || r === "mecanicien" || r === "mechanicien") return "mechanic";
  if (r === "administrateur" || r === "administrator") return "admin";
  if (r === "superadmin") return "superadmin";
  if (r === "admin" || r === "mechanic") return r;
  return "";
}

// Normalize customer email field (supports different schemas)
function getCustomerEmail(c){
  const v = ((c && (c.email ?? c.mail ?? c.emailAddress ?? c.email_address ?? c.courriel ?? c.emailClient)) ?? "");
  return String(v).trim();
}

function docUserProfile(uid=currentUid){
  return doc(db, "users", uid);
}

function docStaffProfile(uid=currentUid){
  return doc(db, "staff", uid);
}

async function ensureUserProfile(_user){
  // IMPORTANT: with your Firestore rules, ONLY an admin can create/update /users/{uid}.
  // So we do NOT auto-create anything here.
  return;
}

async function loadRole(){
  if(!currentUid) return;
  try{
    const snap = await getDoc(docStaffProfile());
    if(!snap.exists()){
      currentRole = "unknown";
      currentUserName = "";
      applyRoleUI();
      alert(
        "Compte non autorisé (staff manquant).\n\n"+
        "Demande à l’admin de t’envoyer une invitation, puis crée ton compte via le code.\n\n"+
        "UID: "+currentUid
      );
      await signOut(auth);
      return;
    }
    const d = snap.data() || {};
    if(d.disabled === true){
      alert("Compte désactivé. Contacte l’admin.");
      await signOut(auth);
      return;
    }
    const normalized = normalizeRole(d.role);
    if (!normalized) {
      roleNeedsSetup = true;
      currentRole = "mechanic";
    } else {
      roleNeedsSetup = false;
      currentRole = normalized;
    }
    currentGarageId = String(d.garageId || localStorage.getItem("garageId") || "").trim();
    if(currentGarageId) localStorage.setItem("garageId", currentGarageId);
    window.currentGarageId = currentGarageId;
    currentUserName = d.fullName || d.displayName || d.name || d.email || auth.currentUser.email || "";
    window.currentRole = currentRole;
  }catch(e){

    currentRole = "unknown";
    currentGarageId = String(localStorage.getItem("garageId") || "").trim();
    currentUserName = "";
  }
  applyRoleUI();
}

async function ensureCurrentGarageId(){
  if(currentGarageId) return currentGarageId;
  try{
    if(currentUid){
      const snap = await getDoc(docStaffProfile());
      const d = snap.exists() ? (snap.data() || {}) : {};
      const gid = String(d.garageId || localStorage.getItem("garageId") || "").trim();
      if(gid){
        currentGarageId = gid;
        window.currentGarageId = gid;
        localStorage.setItem("garageId", gid);
        return gid;
      }
    }
  }catch(e){}
  return String(localStorage.getItem("garageId") || "").trim();
}

function applyRoleUI(){
  const isAdmin = (currentRole === "admin" || currentRole === "superadmin");

  // 1) tout ce qui est marqué data-role="admin" (HTML)
  document.querySelectorAll('[data-role="admin"]').forEach(el=>{
    el.style.display = isAdmin ? "" : "none";
  });

  // 2) fallback pour quelques ids (au cas où)
  const ids = ["btnNewClient","btnNewClient2","btnNewRepair","btnNewRepair2"];
  ids.forEach(id=>{ const el = $(id); if(el) el.style.display = isAdmin ? "" : "none"; });

  const subtitle = document.querySelector('.brand .muted');
  if(subtitle){
    subtitle.textContent = (currentRole === "mechanic")
      ? "Mode mécanicien — Mes travaux"
      : "Synchro automatique (Firebase)";
    if (roleNeedsSetup && !roleSetupShown) {
      roleSetupShown = true;
      showModal(
        "Profil incomplet",
        "Ton compte est connecté, mais ton document Firestore /users/" + currentUser.uid + " n'a pas un role valide.\n\n➡️ Mets le champ role = admin ou role = mechanic (exactement)."
      );
    }
  }
}

async function loadMechanics(){
  mechanics = [];
  if(currentRole !== "admin" && currentRole !== "superadmin") return;
  try{
    const snap = await getDocs(currentGarageId ? query(collection(db, "staff"), where("role","==","mechanic"), where("garageId","==", currentGarageId)) : query(collection(db, "staff"), where("role","==","mechanic")));
    mechanics = snap.docs.map(d=>({uid:d.id, ...(d.data()||{})}))
      .map(u=>({uid:u.uid, name:u.fullName || u.name || u.email || u.uid, email:u.email||""}));
  }catch(e){

  }
}

/* ============
   UI helpers
=========== */
const $ = (id)=>document.getElementById(id);
function setGarageLogoPreview(url){
  const img = $("garageLogoPreview");
  if(img) img.src = String(url || "").trim() || "assets/logo.png";
}

async function uploadGarageLogoFile(){
  const fileInput = $("setGarageLogoFile");
  const file = fileInput?.files?.[0];
  if(!file) throw new Error("Choisis une image d'abord.");
  const gid = await ensureCurrentGarageId();
  if(!gid) throw new Error("Garage introuvable.");
  const safeName = String(file.name || "logo").replace(/[^a-zA-Z0-9._-]+/g,"-");
  const fileRef = storageRef(storage, `garage-logos/${gid}/${Date.now()}-${safeName}`);
  await uploadBytes(fileRef, file, { contentType: file.type || "application/octet-stream" });
  const url = await getDownloadURL(fileRef);
  const urlInput = $("setGarageLogoUrl");
  if(urlInput) urlInput.value = url;
  settings.garageLogoUrl = url;
  updateGarageBrand();
  setGarageLogoPreview(url);
  return url;
}

function updateGarageBrand(){
  const garageName = String(settings?.garageName || "Garage Pro One").trim() || "Garage Pro One";
  const garageLogo = String(settings?.garageLogoUrl || "").trim() || "assets/logo.png";
  const topName = $("garageBrandName");
  if(topName) topName.textContent = garageName;
  const sideName = $("sidebarGarageName");
  if(sideName) sideName.textContent = garageName;
  const applyLogo = (el)=>{
    if(!el) return;
    el.alt = garageName;
    el.src = garageLogo;
    el.onerror = ()=>{ el.onerror = null; el.src = "assets/logo.png"; };
  };
  applyLogo($("garageBrandLogo"));
  applyLogo($("sidebarGarageLogo"));
  applyLogo($("print-logo-img"));
  document.querySelectorAll('img[data-garage-logo="true"]').forEach(applyLogo);
}
const views = {
  dashboard: $("viewDashboard"),
  clients: $("viewClients"),
  repairs: $("viewRepairs"),
  promotions: $("viewPromotions"),
  settings: $("viewSettings"),
  revenue: $("viewRevenue"),
  partsExpenses: $("viewPartsExpenses"),
  suppliers: $("viewSuppliers"),
  invoices: $("viewInvoices"),
  fiscal: $("viewFiscal"),
};
const pageTitle = $("pageTitle");

function safe(s){ return String(s??"").replace(/[&<>"]/g, (c)=>({ "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;" }[c])); }
function money(n){
  const x = Number(n||0);
  return x.toLocaleString('fr-CA', {minimumFractionDigits:2, maximumFractionDigits:2}) + " $";
}

// ---- Tax helpers (TPS/TVQ) ----
function splitTaxTotal(taxTotal){
  const tps = Number(settings?.tpsRate ?? 0.05);
  const tvq = Number(settings?.tvqRate ?? 0.09975);
  const totalRate = tps + tvq;
  const tt = Number(taxTotal||0);
  if(!totalRate) return {tps:0, tvq:0};
  const tpsAmt = tt * (tps/totalRate);
  const tvqAmt = tt - tpsAmt;
  return {tps: tpsAmt, tvq: tvqAmt};
}

function monthKey(d){
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`;
}
function drawBarChart(canvas, labels, values){
  if(!canvas) return;
  const ctx = canvas.getContext("2d");
  const w = canvas.width, h = canvas.height;
  ctx.clearRect(0,0,w,h);
  ctx.fillStyle = "#fff";
  ctx.fillRect(0,0,w,h);

  const max = Math.max(1, ...values.map(v=>Math.max(0,v)));
  const pad = 28;
  const bw = (w - pad*2) / Math.max(1, values.length);
  ctx.strokeStyle = "#ddd";
  ctx.beginPath();
  ctx.moveTo(pad, h-pad);
  ctx.lineTo(w-pad, h-pad);
  ctx.stroke();

  for(let i=0;i<values.length;i++){
    const v = Math.max(0, values[i]);
    const bh = (h - pad*2) * (v / max);
    const x = pad + i*bw + 2;
    const y = (h - pad) - bh;
    ctx.fillStyle = "#2563eb";
    ctx.fillRect(x, y, bw-4, bh);

    ctx.fillStyle = "#666";
    ctx.font = "12px system-ui";
    ctx.fillText(labels[i], x, h-10);
  }
  ctx.fillStyle="#666";
  ctx.font="12px system-ui";
  ctx.fillText(money(max), 6, 14);
}
function pct(n){
  return (Number(n)*100).toFixed(3).replace(/\.000$/,'').replace(/0+$/,'').replace(/\.$/,'') + "%";
}
function byCreatedDesc(a,b){
  return (String(b.createdAt||"")).localeCompare(String(a.createdAt||""));
}
function isoNow(){
  const d = new Date();
  const pad = (n)=> String(n).padStart(2,'0');
  return d.getFullYear()+"-"+pad(d.getMonth()+1)+"-"+pad(d.getDate())+" "+pad(d.getHours())+":"+pad(d.getMinutes());

const toastEl = $("toast");
let toastTimer = null;
function showToast(message, ms=3500){
  if(!toastEl) return;
  // compat: ancien code passait true/false
  if(ms === true) ms = 7000;
  if(ms === false) ms = 3500;
  toastEl.textContent = String(message||"");
  toastEl.style.display = "";
  clearTimeout(toastTimer);
  toastTimer = setTimeout(()=>{ toastEl.style.display="none"; }, ms);
}
}

/* ============
   Garage info (modifiable)
=========== */
const GARAGE = {
  name: "Garage Pro One",
  phone: "(514) 727-0522",
  email: "garageproone@gmail.com",
  address1: "7880 Boul PIE-IX",
  address2: "Montréal (QC) H1Z 3T3",
  country: "Canada",
  tps: "73259 0344",
  tvq: "1230268666",
  tagline: "Vérification / Diagnostic / Réparation"
};

// Simple inline logo (SVG) — tu peux le remplacer par une image plus tard
const GARAGE_LOGO_SVG = `
<svg width="44" height="44" viewBox="0 0 64 64" xmlns="http://www.w3.org/2000/svg" aria-label="Garage Pro One">
  <defs>
    <linearGradient id="g" x1="0" x2="1">
      <stop offset="0" stop-color="#2563eb"/>
      <stop offset="1" stop-color="#1d4ed8"/>
    </linearGradient>
  </defs>
  <rect x="6" y="6" width="52" height="52" rx="14" fill="url(#g)" opacity="0.12"/>
  <path d="M22 40l10-10m0 0l6-6m-6 6l6 6" stroke="#1d4ed8" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"/>
  <path d="M40 22c-3 0-5 2-5 5 0 1 .3 2 .9 2.9L24 42c-1.5.5-3.2.2-4.4-1-1.7-1.7-1.8-4.5-.2-6.3l3.2 3.2 4-4-3.2-3.2c1.8-1.6 4.6-1.5 6.3.2 1.2 1.2 1.5 2.9 1 4.4l12-12c-.9-.6-1.9-.9-2.9-.9z" fill="#2563eb"/>
</svg>`;

/* ============
   Modal
=========== */
const modalBackdrop = $("modalBackdrop");
const modalTitle = $("modalTitle");
const modalBody = $("modalBody");
const btnModalClose = $("btnModalClose");
btnModalClose.onclick = closeModal;

// Delegation click (iOS Safari peut ignorer onclick dans du HTML injecté)
modalBody.addEventListener("click", async (e)=>{
  const btn = e.target.closest("button[data-act]");
  if(!btn) return;
  const act = btn.dataset.act;
  try{
    if(act==="printWo"){
      await window.__printWorkorder(btn.dataset.id);
    }else if(act==="setWoStatus"){
      await window.__setWoStatus(btn.dataset.id, btn.dataset.status);
    }else if(act==="deleteWo"){
      await window.__deleteWo(btn.dataset.id);
    }else if(act==="editInvoice"){
      await window.__editInvoiceFromWorkorder(btn.dataset.id);
    }else if(act==="requestAdmin"){
      await window.__requestAdminValidationFromWorkorder(btn.dataset.id);
    }
  }catch(err){

    alert("Erreur: action impossible. Détail: " + (err?.message || err || "inconnu"));
  }
});

function _handleModalAction(e){
  const btn = e.target && e.target.closest ? e.target.closest("button[data-act]") : null;
  if(!btn) return;
  e.preventDefault();
  e.stopPropagation();
  const act = btn.dataset.act;
  (async ()=>{
    try{
      if(act==="printWo"){
        await window.__printWorkorder(btn.dataset.id);
      }else if(act==="setWoStatus"){
        await window.__setWoStatus(btn.dataset.id, btn.dataset.status);
      }else if(act==="deleteWo"){
        await window.__deleteWo(btn.dataset.id);
      }else if(act==="editInvoice"){
        await window.__editInvoiceFromWorkorder(btn.dataset.id);
      }else if(act==="requestAdmin"){
        await window.__requestAdminValidationFromWorkorder(btn.dataset.id);
      }
    }catch(err){

      alert("Erreur: action impossible. Détail: " + (err?.message || err || "inconnu"));
    }
  })();
}
modalBackdrop.addEventListener("click", (e)=>{ if(e.target===modalBackdrop) closeModal(); });

// pour éviter l\'avertissement aria-hidden (focus gardé dans le modal)
let _lastFocusedBeforeModal = null;

function openModal(title, html){
  _lastFocusedBeforeModal = document.activeElement;

  modalTitle.textContent = title;
  modalBody.innerHTML = html;
  modalBackdrop.style.display = "flex";
  modalBackdrop.setAttribute("aria-hidden","false");
  modalBackdrop.removeAttribute("inert");

  document.body.classList.add("modal-open");

  // focus sur le bouton fermer (accessibilité)
  setTimeout(()=>{ try{ btnModalClose && btnModalClose.focus(); }catch(e){} }, 0);
}

function closeModal(){
  // si le focus est dans le modal, on le retire avant de cacher (sinon warning aria-hidden)
  try{
    if(modalBackdrop.contains(document.activeElement)){
      document.activeElement.blur();
    }
  }catch(e){}

  modalBackdrop.style.display = "none";
  modalBackdrop.setAttribute("aria-hidden","true");
  modalBackdrop.setAttribute("inert", "");
  modalBody.innerHTML = "";

  document.body.classList.remove("modal-open");

  // revenir au dernier élément focus
  try{
    if(_lastFocusedBeforeModal && _lastFocusedBeforeModal.focus) _lastFocusedBeforeModal.focus();
  }catch(e){}
  _lastFocusedBeforeModal = null;
}

/* ============
   Navigation
=========== */
// Sidebar helpers (mobile)
function closeSidebar(){
  const sb = document.getElementById("sidebar");
  const ov = document.getElementById("sidebarOverlay");
  if(sb) sb.classList.remove("open");
  if(ov) ov.style.display = "none";
}
function openSidebar(){
  const sb = document.getElementById("sidebar");
  const ov = document.getElementById("sidebarOverlay");
  if(sb) sb.classList.add("open");
  if(ov) ov.style.display = "block";
}

// Attach sidebar listeners once (avoid duplicate handlers)
(function initSidebarToggle(){
  const btn = document.getElementById("btnMenu");
  const ov  = document.getElementById("sidebarOverlay");
  const sb  = document.getElementById("sidebar");
  if(!btn || !ov || !sb) return;
  if(btn.dataset.bound === "1") return; // already bound
  btn.dataset.bound = "1";
  btn.addEventListener("click", ()=>{
    if(sb.classList.contains("open")) closeSidebar(); else openSidebar();
  });
  ov.addEventListener("click", closeSidebar);
  window.addEventListener("resize", ()=>{
    try{ if(!window.matchMedia("(max-width: 980px)").matches) closeSidebar(); }catch(e){}
  });
})();

function resetScrollToTop(){
  // iOS Safari can keep scroll position when toggling display:none blocks.
  // Reset both window and potential scroll containers.
  const main = document.querySelector(".main");
  const container = document.querySelector(".container");
  const scrollingEl = document.scrollingElement || document.documentElement;
  const doReset = ()=>{
    // window
    try{ window.scrollTo(0,0); }catch(e){}

    // standard scrolling element (best for iOS Safari)
    try{ if(scrollingEl) scrollingEl.scrollTop = 0; }catch(e){}

    // legacy fallbacks
    try{ document.documentElement.scrollTop = 0; }catch(e){}
    try{ document.body.scrollTop = 0; }catch(e){}

    // in case a wrapper becomes scrollable
    try{ if(container) container.scrollTop = 0; }catch(e){}
    try{ if(main) main.scrollTop = 0; }catch(e){}
  };
  doReset();
  // Extra passes for iOS (layout/paint timing)
  try{ requestAnimationFrame(()=>{ doReset(); }); }catch(e){}
  try{ setTimeout(()=>{ doReset(); }, 30); }catch(e){}
  try{ setTimeout(()=>{ doReset(); }, 120); }catch(e){}
}

function go(view){
  if(currentRole === "mechanic" && (view==="dashboard" || view==="settings" || view==="revenue" || view==="promotions" || view==="invoices" || view==="fiscal" || view==="partsExpenses" || view==="suppliers" || view==="notifications")){
    view = "repairs";
  }
  // Hide all view sections (robust on mobile/iOS)
document.querySelectorAll('#viewApp > section[id^="view"]').forEach(sec=>{
  sec.style.display = 'none';
});
// Also hide any other view containers if present
for(const k in views){ if(views[k]) views[k].style.display = 'none'; }

// Show requested view
if(views[view]){ views[view].style.display = ''; }
  const titles = {dashboard:"Dashboard", clients:"Clients", repairs:"Réparations", promotions:"Promotions", revenue:"Revenus", fiscal:"Info fiscaux", partsExpenses:"Dépenses pièces", suppliers:"Fournisseurs", invoices:"Factures pièces", notifications:"Notifications", settings:"Paramètres"};
  pageTitle.textContent = titles[view] || "Garage Pro One";
  // highlight active menu
  document.querySelectorAll("[data-go]").forEach(b=>{
    b.classList.toggle("active", b.getAttribute("data-go")===view);
  });

  // Always close sidebar after navigation (mobile UX)
  closeSidebar();

  // Always reset scroll position when changing views
  resetScrollToTop();

  try{ if(view==="notifications") renderNotifications(); }catch(e){}

}
document.querySelectorAll("[data-go]").forEach(btn=>{
  btn.addEventListener("click", ()=>go(btn.getAttribute("data-go")));
});

/* ============
   Firestore paths (garage-scoped)
=========== */
let currentUid = null;
let DATA_MODE = "garage";

function garageBaseRef(garageId=currentGarageId){
  const gid = String(garageId || "").trim();
  if(!gid) throw new Error("missing-garageId");
  return doc(db, "garages", gid);
}
function garageCol(name, garageId=currentGarageId){
  return collection(garageBaseRef(garageId), name);
}
function garageDoc(name, id, garageId=currentGarageId){
  return doc(garageCol(name, garageId), id);
}

function colCustomers(){ return garageCol("customers"); }
function colVehicles(){ return garageCol("vehicles"); }
function colInvoices(){ return garageCol("invoices"); }
function colWorkorders(){ return garageCol("workorders"); }
function colAppointments(){ return garageCol("appointments"); }
function colPromotions(){ return garageCol("promotions"); }
function colPartsExpenses(){ return garageCol("expenses_parts"); }
function colSuppliers(){ return garageCol("suppliers"); }
function colLogs(){ return garageCol("logs"); }
function colInvites(){ return garageCol("invites"); }

function docSettings(){ return garageDoc("settings", "main"); }
function docCounters(){ return garageDoc("settings", "counters"); }

async function _hasAnyDocs(colRef){
  try{
    const snap = await getDocs(query(colRef, limit(1)));
    return snap.docs.length>0;
  }catch(e){ return false; }
}
async function detectDataMode(){
  return "root";
}

/* ============
   Live cache
=========== */
let customers = [];
let vehicles = [];
let workorders = [];
let invoices = [];
let settings = { tpsRate: 0.05, tvqRate: 0.09975 , cardFeeRate: 0.025, laborRate: 80, garageName:"Garage Pro One", garageAddress:"", garagePhone:"", garageEmail:"", garageLogoUrl:"", garageTpsNo:"", garageTvqNo:"", signatureName:"", theme:"light", devMode:"off" };

let promotions = [];
let selectedPromotionId = null;

// Dépenses pièces (achats)
let partsExpenses = [];
let suppliers = [];

let unsubSettings = null;
let unsubCustomers = null;
let unsubVehicles = null;
let unsubWorkorders = null;
let unsubPromotions = null;
let unsubInvoices = null;
let unsubPartsExpenses = null;
let unsubSuppliers = null;

/* ============
   Auth UI
=========== */
$("year").textContent = new Date().getFullYear();

const tabLogin = $("tabLogin");
const tabRegister = $("tabRegister");
const formLogin = $("formLogin");
const formRegister = $("formRegister");
const authError = $("authError");
const authOk = $("authOk");

function showAuthMessage(kind, msg){
  authError.style.display = kind==="error" ? "" : "none";
  authOk.style.display = kind==="ok" ? "" : "none";
  if(kind==="error") authError.textContent = msg;
  if(kind==="ok") authOk.textContent = msg;
}

tabLogin.onclick = ()=>{
  tabLogin.classList.add("active"); tabRegister.classList.remove("active");
  formLogin.style.display = ""; formRegister.style.display = "none";
  showAuthMessage("", "");
};
tabRegister.onclick = ()=>{
  tabRegister.classList.add("active"); tabLogin.classList.remove("active");
  formRegister.style.display = ""; formLogin.style.display = "none";
  showAuthMessage("", "");
};

formLogin.onsubmit = async (e)=>{
  e.preventDefault();
  showAuthMessage("", "");
  const fd = new FormData(formLogin);
  const email = String(fd.get("email")||"").trim();
  const password = String(fd.get("password")||"");
  try{
    await signInWithEmailAndPassword(auth, email, password);
  }catch(err){
    showAuthMessage("error", err?.message || "Connexion impossible.");
  }
};

formRegister.onsubmit = async (e)=>{
  e.preventDefault();
  showAuthMessage("", "");
  const fd = new FormData(formRegister);
  const email = String(fd.get("email")||"").trim();
  const password = String(fd.get("password")||"");
  try{
    await createUserWithEmailAndPassword(auth, email, password);
    showAuthMessage("ok", "Compte créé. Tu es connecté.");
  }catch(err){
    showAuthMessage("error", err?.message || "Création impossible.");
  }
};

$("btnForgot").onclick = async ()=>{
  showAuthMessage("", "");
  const email = prompt("Entre ton email pour recevoir le lien de réinitialisation :");
  if(!email) return;
  try{
    await sendPasswordResetEmail(auth, email.trim());
    showAuthMessage("ok", "Email envoyé. Vérifie ta boîte de réception.");
  }catch(err){
    showAuthMessage("error", err?.message || "Impossible d'envoyer l'email.");
  }
};

$("btnLogout").onclick = async ()=>{ await signOut(auth); };

/* ============
   Snapshot subscriptions
=========== */
async function ensureSettingsDoc(){
  const ref = docSettings();
  const snap = await getDoc(ref);
  if(!snap.exists()){
    const gid = await ensureCurrentGarageId();
    await setDoc(ref, { garageId: gid || "", tpsRate: 0.05, tvqRate: 0.09975, garageName: settings.garageName || "Garage Pro One", garageLogoUrl: settings.garageLogoUrl || "", logoUrl: settings.garageLogoUrl || "", updatedAt: serverTimestamp() }, { merge:true });
  }
  const cRef = docCounters();
  const cSnap = await getDoc(cRef);
  if(!cSnap.exists()){
    const gid2 = await ensureCurrentGarageId();
    await setDoc(cRef, { garageId: gid2 || "", invoiceNext: 1, updatedAt: serverTimestamp() }, { merge:true });
  }
}

async function nextInvoiceNo(){
  const ref = docCounters();
  const n = await runTransaction(db, async (tx)=>{
    const s = await tx.get(ref);
    const cur = s.exists() ? Number(s.data().invoiceNext||1) : 1;
    tx.set(ref, { invoiceNext: cur+1, updatedAt: serverTimestamp() }, { merge:true });
    return cur;
  });
  return "GP-" + String(n).padStart(4,"0");
}

function scopeByGarage(ref, extraConstraints=[]) {
  const constraints = [];
  if (currentGarageId) constraints.push(where("garageId", "==", currentGarageId));
  if (Array.isArray(extraConstraints)) constraints.push(...extraConstraints.filter(Boolean));
  return constraints.length ? query(ref, ...constraints) : query(ref);
}

function subscribeAll(){
  // settings branding/info for all signed-in garage members
  if(currentGarageId){
    unsubSettings = onSnapshot(docSettings(), (snap)=>{
      if(snap.exists()){
        const d = snap.data();
        settings = {
          // keep defaults for missing fields
          tpsRate: Number(d.tpsRate ?? settings.tpsRate ?? 0.05),
          tvqRate: Number(d.tvqRate ?? settings.tvqRate ?? 0.09975),
          cardFeeRate: Number(d.cardFeeRate ?? settings.cardFeeRate ?? 0.025),
          laborRate: Number(d.laborRate ?? settings.laborRate ?? 80),
          garageName: String(d.garageName ?? settings.garageName ?? "Garage Pro One"),
          garageAddress: String(d.garageAddress ?? settings.garageAddress ?? ""),
          garagePhone: String(d.garagePhone ?? settings.garagePhone ?? ""),
          garageEmail: String(d.garageEmail ?? settings.garageEmail ?? ""),
          garageLogoUrl: String(d.garageLogoUrl ?? d.logoUrl ?? settings.garageLogoUrl ?? ""),
          garageTpsNo: String(d.garageTpsNo ?? d.tpsNumber ?? settings.garageTpsNo ?? ""),
          garageTvqNo: String(d.garageTvqNo ?? d.tvqNumber ?? settings.garageTvqNo ?? ""),
          signatureName: String(d.signatureName ?? settings.signatureName ?? ""),
        };
        renderSettings();
        updateGarageBrand();
        renderDashboard();
      } else {
        // doc not created yet -> show defaults
        renderSettings();
        updateGarageBrand();
        renderDashboard();
      }
    });
  }

  // Promotions (admin only)
  if(currentRole === "admin"){
    unsubPromotions = onSnapshot(scopeByGarage(colPromotions()), (snap)=>{
      promotions = snap.docs.map(d=>({id:d.id, ...d.data()})).sort((a,b)=> String(b.createdAt||"").localeCompare(String(a.createdAt||"")));
      renderPromotions();
    });
    // Staff list (admin only) - realtime
    if(unsubStaffLive) try{unsubStaffLive();}catch(e){}
    unsubStaffLive = onSnapshot((currentGarageId ? query(collection(db,"staff"), where("garageId","==", currentGarageId)) : query(collection(db,"staff"))), (snap)=>{
      staffLiveRows = snap.docs.map(d=>({uid:d.id, ...(d.data()||{})})).sort((a,b)=> String(b.createdAt||"").localeCompare(String(a.createdAt||"")));
      renderStaffRows(staffLiveRows);
    });

    // Invites list (admin only) - realtime
    if(unsubInvitesLive) try{unsubInvitesLive();}catch(e){}
    unsubInvitesLive = onSnapshot(query(colInvites()), (snap)=>{
      invitesLiveRows = snap.docs.map(d=>({code:d.id, ...(d.data()||{})})).sort((a,b)=> String(b.createdAt||"").localeCompare(String(a.createdAt||"")));
      renderInviteRows(invitesLiveRows);
    });

  }

  unsubCustomers = onSnapshot(scopeByGarage(colCustomers()), (snap)=>{
    // Normalisation: certains clients ont l'email sous Email/courriel/mail...
    // On force un champ `email` unique utilisé partout (Clients, Promotions, etc.).
    customers = snap.docs.map(d=>{
      const data = d.data() || {};
      return {
        id: d.id,
        ...data,
        fullName: String(data.fullName || data.name || "").trim(),
        phone: String(data.phone || data.tel || data.mobile || "").trim(),
        email: normalizeEmail(data),
        promoSelected: data.promoSelected === true,
      };
    }).sort((a,b)=> String(a.fullName||"").localeCompare(String(b.fullName||"")));
    if(currentRole === "admin" || currentRole === "superadmin") renderDashboard();
    renderClients();
    fillInvoiceCustomers();
    fillInvoiceWorkorders();
    if(currentRole === "admin" || currentRole === "superadmin") renderRevenue();
    if(currentRole === "admin" || currentRole === "superadmin") renderFiscal();
    if(currentRole === "admin" || currentRole === "superadmin") renderPromotions();
  });

  unsubVehicles = onSnapshot(scopeByGarage(colVehicles()), (snap)=>{
    vehicles = snap.docs.map(d=>({id:d.id, ...d.data()})).sort((a,b)=> String(b.createdAt||"").localeCompare(String(a.createdAt||"")));
    if(currentRole === "admin" || currentRole === "superadmin") renderDashboard();
    renderClients();
    fillInvoiceCustomers();
    fillInvoiceWorkorders();
    if(currentRole === "admin" || currentRole === "superadmin") renderRevenue();
    if(currentRole === "admin" || currentRole === "superadmin") renderFiscal();
  });

  const woQ = (currentRole === "mechanic")
    ? scopeByGarage(colWorkorders(), [where("assignedTo","==", currentUid)])
    : scopeByGarage(colWorkorders());

  unsubWorkorders = onSnapshot(
    woQ,
    (snap)=>{
      workorders = snap.docs.map(d=>({id:d.id, ...d.data()})).sort((a,b)=> String(b.createdAt||"").localeCompare(String(a.createdAt||"")));
      if(currentRole === "admin" || currentRole === "superadmin") renderDashboard();
      renderRepairs();
      fillInvoiceWorkorders();
      if(currentRole === "admin" || currentRole === "superadmin") renderRevenue();
    },
    (err)=>{

      showToast("Accès refusé: réparations. Vérifie le champ role dans /users/{uid} (admin ou mechanic).", true);
    }
  );

  // Invoices (admin)
  if(currentRole === "admin"){
    const invQ = scopeByGarage(colInvoices());
    unsubInvoices = onSnapshot(invQ,
      (snap)=>{
        invoices = snap.docs.map(d=>({id:d.id, ...d.data()})).sort((a,b)=> String(b.date||b.createdAt||"").localeCompare(String(a.date||a.createdAt||"")));
        renderInvoices();
        renderRevenue();
        renderFiscal();
        renderFinanceDashboard();
      },
      (err)=>{

        showToast("Accès refusé: factures. Vérifie les rules /invoices et ton rôle admin.", 7000);
      }
    );

    const supQ = query(colSuppliers(), orderBy("name", "asc"), limit(1000));
    unsubSuppliers = onSnapshot(
      supQ,
      (snap)=>{
        suppliers = snap.docs.map(d=>({id:d.id, ...d.data()}));
        renderSuppliers();
        fillInvoiceSuppliers(invSupplierEl?.value||"");
      },
      (err)=>{
        console.error(err);
        showToast("Accès refusé: fournisseurs. Vérifie les règles du garage.", 7000);
      }
    );

    // Parts expenses (admin)
    const pexQ = query(colPartsExpenses(), orderBy("date", "desc"), limit(2000));
    unsubPartsExpenses = onSnapshot(
      pexQ,
      (snap)=>{
        partsExpenses = snap.docs.map(d=>({id:d.id, ...d.data()}));
        renderPartsExpenses();
        renderFiscal();
      },
      (err)=>{
        console.error(err);
        showToast("Accès refusé: dépenses pièces. Vérifie les règles du garage.", 7000);
      }
    );
  }

}

function unsubscribeAll(){
  if(unsubSettings) try{unsubSettings();}catch(e){}
  if(unsubCustomers) try{unsubCustomers();}catch(e){}
  if(unsubVehicles) try{unsubVehicles();}catch(e){}
  if(unsubWorkorders) try{unsubWorkorders();}catch(e){}
  if(unsubPromotions) try{unsubPromotions();}catch(e){}
  if(unsubInvoices) try{unsubInvoices();}catch(e){}
  if(unsubPartsExpenses) try{unsubPartsExpenses();}catch(e){}
  if(unsubSuppliers) try{unsubSuppliers();}catch(e){}
  if(unsubStaffLive) try{unsubStaffLive();}catch(e){}
  if(unsubInvitesLive) try{unsubInvitesLive();}catch(e){}
  unsubSettings = unsubCustomers = unsubVehicles = unsubWorkorders = unsubPromotions = unsubInvoices = unsubPartsExpenses = unsubSuppliers = null;
  unsubStaffLive = unsubInvitesLive = null;
}

/* ============
   Renderers
=========== */
const kpiEl = $("kpi");
const finSalesEl = $("finSales");
const finPartsEl = $("finParts");
const finProfitEl = $("finProfit");
const finCountEl = $("finCount");
const finByPayTbody = $("finByPayTbody");
const finByDayTbody = $("finByDayTbody");
const chartSalesEl = $("chartSales");
const chartNetEl = $("chartNet");
const openRepairsTbody = $("openRepairsTbody");
const openRepairsCards = $("openRepairsCards");
const unpaidRepairsCountEl = $("unpaidRepairsCount");
const unpaidRepairsTbody = $("unpaidRepairsTbody");
const unpaidRepairsCards = $("unpaidRepairsCards");
const finByPayCards = $("finByPayCards");
const finByDayCards = $("finByDayCards");
function getCustomer(id){ return customers.find(c=>c.id===id); }
function getVehicle(id){ return vehicles.find(v=>v.id===id); }

function renderDashboard(){
  if(currentRole === "mechanic"){
    if ($("dashboardCards")) $("dashboardCards").innerHTML = `<div class="note">Accès réservé à l'administrateur.</div>`;
    $("openRepairsTbody").innerHTML = `<tr><td colspan="4" class="muted">—</td></tr>`;
    return;
  }
  const totalCustomers = customers.length;
  const totalVehicles = vehicles.length;
  const openCount = workorders.filter(w=>w.status==="OUVERT").length;
  const monthKey = new Date().toISOString().slice(0,7);
  const monthTotal = workorders
    .filter(w=>String(w.createdAt||"").startsWith(monthKey))
    .reduce((sum,w)=>sum+Number(w.total||0),0);

  kpiEl.innerHTML = `
    <div class="box"><div class="muted">Clients</div><div class="val">${totalCustomers}</div></div>
    <div class="box"><div class="muted">Véhicules</div><div class="val">${totalVehicles}</div></div>
    <div class="box"><div class="muted">Réparations ouvertes</div><div class="val">${openCount}</div></div>
    <div class="box"><div class="muted">Total (${monthKey})</div><div class="val">${money(monthTotal)}</div></div>
  `;

  renderFinanceDashboard();

  const open = [...workorders].filter(w=>w.status==="OUVERT").sort(byCreatedDesc).slice(0,20);
  if(open.length===0){
    openRepairsTbody.innerHTML = '<tr><td colspan="5" class="muted">Aucune réparation ouverte.</td></tr>';
    if(openRepairsCards) openRepairsCards.innerHTML = '<div class="note">Aucune réparation ouverte.</div>';
  }else{
    openRepairsTbody.innerHTML = open.map(w=>{
      const v = getVehicle(w.vehicleId);
      const c = v ? getCustomer(v.customerId) : null;
      const client = c ? c.fullName : "—";
      const veh = v ? [v.year,v.make,v.model].filter(Boolean).join(" ") + (v.plate?` (${v.plate})`:"") : "—";
      const d = String(w.createdAt||"").slice(0,10);
      return `
        <tr>
          <td>${safe(d)}</td>
          <td>${safe(client)}</td>
          <td>${safe(veh)}</td>
          <td>${money(w.total)}</td>
          <td class="nowrap">
            <button class="btn btn-small" onclick="window.__openWorkorderView('${w.id}')">Ouvrir</button>
          </td>
        </tr>
      `;
    }).join("");

    if(openRepairsCards){
      openRepairsCards.innerHTML = open.map(w=>{
        const v = getVehicle(w.vehicleId);
        const c = v ? getCustomer(v.customerId) : null;
        const client = c ? c.fullName : "—";
        const veh = v ? [v.year,v.make,v.model].filter(Boolean).join(" ") + (v.plate?` (${v.plate})`:"") : "—";
        const d = String(w.createdAt||"").slice(0,10);
        return `
          <div class="mcard">
            <div class="top">
              <div>
                <div class="title">${safe(client)}</div>
                <div class="sub">${safe(veh)}</div>
              </div>
              <div class="amount">${money(w.total)}</div>
            </div>
            <div class="meta">
              <span>📅 ${safe(d)}</span>
              <span><span class="pill pill-warn">OUVERT</span></span>
            </div>
            <div class="actions">
              <button class="btn btn-small" onclick="window.__openWorkorderView('${w.id}')">Ouvrir</button>
            </div>
          </div>
        `;
      }).join('');
    }
  }

  // Unpaid repairs dashboard (per réparation)
  renderUnpaidRepairsDashboard();
}

function _workorderIsPaid(w){
  const st = (w && w.paymentStatus) ? String(w.paymentStatus) : "paid";
  return st === "paid";
}
function _workorderAmountDue(w){
  if(!w) return 0;
  if(_workorderIsPaid(w)) return 0;
  const due = (w.amountDue !== undefined && w.amountDue !== null) ? Number(w.amountDue) : Number(w.total||0);
  return isFinite(due) ? due : 0;
}

async function __setWorkorderPaid(id, paid){
  try{
    const w = workorders.find(x=>x.id===id);
    if(!w) return;
    const total = Number(w.total||0);
    await updateDoc(doc(colWorkorders(), id), {
      paymentStatus: paid ? "paid" : "unpaid",
      amountPaid: paid ? total : 0,
      amountDue: paid ? 0 : total,
      paidAt: paid ? serverTimestamp() : null,
      updatedAt: serverTimestamp(),
    });
    showToast(paid ? "Réparation marquée Payée." : "Réparation marquée Non payée.");
  }catch(e){
    showToast("Erreur paiement. Vérifie tes droits Firestore.", true);
  }
}
function __emailWorkorderPayment(id){
  const w = workorders.find(x=>x.id===id);
  if(!w) return;
  const v = getVehicle(w.vehicleId);
  const c = v ? getCustomer(v.customerId) : null;
  const email = (c && c.email) ? String(c.email).trim() : "";
  if(!email){
    showToast("Ce client n'a pas d'email.", true);
    return;
  }
  const veh = v ? [v.year,v.make,v.model].filter(Boolean).join(" ") + (v.plate?` (${v.plate})`:"") : "—";
  const due = _workorderAmountDue(w);
  const subject = `Paiement réparation - Garage Pro One`;
  const body =
`Bonjour ${c?.fullName||""},

Votre réparation (${veh}) est en attente de paiement.

Montant dû: ${money(due)}

Merci,
Garage Pro One`;
  window.location.href = `mailto:${encodeURIComponent(email)}?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`;
}

window.__setWorkorderPaid = __setWorkorderPaid;
window.__emailWorkorderPayment = __emailWorkorderPayment;

function renderUnpaidRepairsDashboard(){
  try{
    if(currentRole !== "admin" && currentRole !== "superadmin") return;
    if(!unpaidRepairsTbody || !unpaidRepairsCountEl) return;

    const list = [...workorders]
      .filter(w=>!_workorderIsPaid(w) && _workorderAmountDue(w) > 0)
      .sort(byCreatedDesc)
      .slice(0, 50);

    unpaidRepairsCountEl.textContent = `${list.length} non payé(s)`;

    if(list.length===0){
      unpaidRepairsTbody.innerHTML = '<tr><td colspan="6" class="muted">Aucune réparation non payée.</td></tr>';
      if(unpaidRepairsCards) unpaidRepairsCards.innerHTML = '<div class="note">Aucune réparation non payée.</div>';
      return;
    }

    unpaidRepairsTbody.innerHTML = list.map(w=>{
      const v = getVehicle(w.vehicleId);
      const c = v ? getCustomer(v.customerId) : null;
      const client = c ? c.fullName : "—";
      const emailOk = c && c.email;
      const veh = v ? [v.year,v.make,v.model].filter(Boolean).join(" ") + (v.plate?` (${v.plate})`:"") : "—";
      const d = String(w.createdAt||"").slice(0,10);
      const due = _workorderAmountDue(w);
      return `
        <tr>
          <td>${safe(d)}</td>
          <td>${safe(client)}</td>
          <td>${safe(veh)}</td>
          <td>${money(due)}</td>
          <td><span class="pill pill-warn">Non payé</span></td>
          <td class="nowrap">
            <button class="btn btn-small" onclick="window.__setWorkorderPaid('${w.id}', true)">Marquer payé</button>
            <button class="btn btn-small btn-ghost" ${emailOk?'':'disabled'} onclick="window.__emailWorkorderPayment('${w.id}')">Email</button>
          </td>
        </tr>
      `;
    }).join("");

    if(unpaidRepairsCards){
      unpaidRepairsCards.innerHTML = list.map(w=>{
        const v = getVehicle(w.vehicleId);
        const c = v ? getCustomer(v.customerId) : null;
        const client = c ? c.fullName : "—";
        const emailOk = c && c.email;
        const veh = v ? [v.year,v.make,v.model].filter(Boolean).join(" ") + (v.plate?` (${v.plate})`:"") : "—";
        const d = String(w.createdAt||"").slice(0,10);
        const due = _workorderAmountDue(w);
        return `
          <div class="mcard">
            <div class="top">
              <div>
                <div class="title">${safe(client)}</div>
                <div class="sub">${safe(veh)}</div>
              </div>
              <div class="amount">${money(due)}</div>
            </div>
            <div class="meta">
              <span>📅 ${safe(d)}</span>
              <span class="pill pill-warn">Non payé</span>
            </div>
            <div class="actions">
              <button class="btn btn-small" onclick="window.__setWorkorderPaid('${w.id}', true)">Marquer payé</button>
              <button class="btn btn-small btn-ghost" ${emailOk?'':'disabled'} onclick="window.__emailWorkorderPayment('${w.id}')">Email</button>
              <button class="btn btn-small" onclick="window.__openWorkorderView('${w.id}')">Ouvrir</button>
            </div>
          </div>
        `;
      }).join('');
    }

  }catch(e){
    // silent
  }
}


function renderFinanceDashboard(){
  try{
    if(currentRole !== "admin" && currentRole !== "superadmin") return;
    if(!finSalesEl) return;

    const now = new Date();
    const monthFrom = isoDate(new Date(now.getFullYear(), now.getMonth(), 1));
    const monthTo = isoDate(new Date(now.getFullYear(), now.getMonth()+1, 0));

    const monthInv = (invoices||[]).filter(inv=>{
      const k = invoiceDateKey(inv);
      return k >= monthFrom && k <= monthTo;
    });

    let sales=0, parts=0, profit=0;
    for(const inv of monthInv){
      const t = getInvoiceTotals(inv);
      sales += t.sell;
      parts += t.cost;
      profit += t.profit;
    }

    finSalesEl.textContent = money(sales);
    if(finPartsEl) finPartsEl.textContent = money(parts);
    if(finProfitEl) finProfitEl.textContent = money(profit);
    if(finCountEl) finCountEl.textContent = String(monthInv.length);

    // Par méthode (mois)
    if(finByPayTbody){
      const map = new Map();
      for(const inv of monthInv){
        const k = String(inv.paymentMethod||"").toLowerCase() || "unknown";
        const cur = map.get(k) || {k, total:0, cost:0, profit:0};
        const t = getInvoiceTotals(inv);
        cur.total += t.sell;
        cur.cost += t.cost;
        cur.profit += t.profit;
        map.set(k, cur);
      }
      const list=[...map.values()].sort((a,b)=>b.profit-a.profit);
      finByPayTbody.innerHTML = list.map(r=>`
        <tr>
          <td>${safe(invPaymentLabel(r.k))}</td>
          <td style="text-align:right">${money(r.total)}</td>
          <td style="text-align:right">${money(r.cost)}</td>
          <td style="text-align:right"><b>${money(r.profit)}</b></td>
        </tr>
      `).join('') || '<tr><td class="muted" colspan="4">Aucune donnée.</td></tr>';

      if(finByPayCards){
        finByPayCards.innerHTML = (list.length ? list : []).map(r=>`
          <div class="mcard">
            <div class="top">
              <div>
                <div class="title">${safe(invPaymentLabel(r.k))}</div>
                <div class="sub">Total: ${money(r.total)}</div>
              </div>
              <div class="amount">${money(r.profit)}</div>
            </div>
            <div class="meta">
              <span>Coût pièces: ${money(r.cost)}</span>
              <span>Bénéfice: <b>${money(r.profit)}</b></span>
            </div>
          </div>
        `).join('') || '<div class="note">Aucune donnée.</div>';
      }
    }

    // Par jour (14 derniers jours)
    if(finByDayTbody){
      const dayFrom = new Date(now.getTime() - 13*24*60*60*1000);
      const map = new Map();
      for(const inv of (invoices||[])){
        const d = invoiceDateAsDate(inv);
        if(d < dayFrom) continue;
        const k = invoiceDateKey(inv);
        const cur = map.get(k) || {k, total:0, cost:0, profit:0};
        const t = getInvoiceTotals(inv);
        cur.total += t.sell;
        cur.cost += t.cost;
        cur.profit += t.profit;
        map.set(k, cur);
      }
      const list=[...map.values()].sort((a,b)=>String(b.k).localeCompare(String(a.k)));
      finByDayTbody.innerHTML = list.map(r=>`
        <tr>
          <td>${safe(r.k)}</td>
          <td style="text-align:right">${money(r.total)}</td>
          <td style="text-align:right">${money(r.cost)}</td>
          <td style="text-align:right"><b>${money(r.profit)}</b></td>
        </tr>
      `).join('') || '<tr><td class="muted" colspan="4">Aucune donnée.</td></tr>';

      if(finByDayCards){
        finByDayCards.innerHTML = (list.length ? list : []).map(r=>`
          <div class="mcard">
            <div class="top">
              <div>
                <div class="title">${safe(r.k)}</div>
                <div class="sub">Total: ${money(r.total)}</div>
              </div>
              <div class="amount">${money(r.profit)}</div>
            </div>
            <div class="meta">
              <span>Coût pièces: ${money(r.cost)}</span>
              <span>Bénéfice: <b>${money(r.profit)}</b></span>
            </div>
          </div>
        `).join('') || '<div class="note">Aucune donnée.</div>';
      }
    }
  

    // Graphiques 12 mois (revenus & profit net)
    if(chartSalesEl || chartNetEl){
      const m = new Map();
      for(const inv of (invoices||[])){
        const d = invoiceDateAsDate(inv);
        const k = monthKey(d);
        const t = getInvoiceTotals(inv);
        const cur = m.get(k) || {sales:0, net:0};
        cur.sales += t.sell;
        cur.net += t.profit;
        m.set(k, cur);
      }
      const keys = [...m.keys()].sort().slice(-12);
      const labels = keys.map(k=>k.slice(5));
      const salesVals = keys.map(k=>m.get(k)?.sales||0);
      const netVals = keys.map(k=>m.get(k)?.net||0);
      drawBarChart(chartSalesEl, labels, salesVals);
      drawBarChart(chartNetEl, labels, netVals);
    }
}catch(e){

  }
}

/* Quick search */
$("btnQuickSearch").onclick = ()=>runQuickSearch();
$("btnClearSearch").onclick = ()=>{ $("quickSearch").value=""; $("searchResults").innerHTML = '<span class="muted">Tape une recherche pour afficher les résultats.</span>'; };
$("quickSearch").addEventListener("keydown", (e)=>{ if(e.key==="Enter") runQuickSearch(); });

// Camera OCR: search by license plate
const btnPlateScan = $("btnPlateScan");
if(btnPlateScan){
  btnPlateScan.onclick = async ()=>{
    try{
      await startPlateScanner();
    }catch(e){

      alert('Impossible d\'ouvrir la caméra pour la plaque. Vérifie les permissions caméra.');
    }
  };
}

function runQuickSearch(){
  const q = ($("quickSearch").value||"").trim().toLowerCase();
  if(!q){
    $("searchResults").innerHTML = '<span class="muted">Tape une recherche pour afficher les résultats.</span>';
    return;
  }
  const matches = [];
  for(const c of customers){
    const cHit = (c.fullName||"").toLowerCase().includes(q) ||
                 (c.phone||"").toLowerCase().includes(q) ||
                 (c.email||"").toLowerCase().includes(q);
    const vs = vehicles.filter(v=>v.customerId===c.id);
    const vHits = vs.filter(v =>
      (v.plate||"").toLowerCase().includes(q) ||
      (v.vin||"").toLowerCase().includes(q) ||
      (v.make||"").toLowerCase().includes(q) ||
      (v.model||"").toLowerCase().includes(q)
    );
    if(cHit || vHits.length){
      matches.push({c, vehicles: vHits.length ? vHits : vs.slice(0,1)});
    }
  }
  if(matches.length===0){
    $("searchResults").innerHTML = '<div class="muted">Aucun résultat.</div>';
    return;
  }
  const rows = matches.slice(0,50).map(m=>{
    const c = m.c;
    const v = (m.vehicles && m.vehicles[0]) ? m.vehicles[0] : null;
    const vehTxt = v ? [v.year,v.make,v.model].filter(Boolean).join(" ") : "";
    const plate = v?.plate || "";
    return `
      <tr>
        <td>${safe(c.fullName)}</td>
        <td>${safe(c.phone||"")}</td>
        <td>${safe(vehTxt)}</td>
        <td>${safe(plate)}</td>
        <td class="nowrap">
          <button class="btn btn-small" onclick="window.__openClientView('${c.id}')">Ouvrir</button>
          ${v ? `<button class="btn btn-small btn-ghost" onclick="window.__openWorkorderForm('${v.id}')">+ Réparation</button>` : ""}
        </td>
      </tr>
    `;
  }).join("");
  $("searchResults").innerHTML = `
    <div class="table-wrap">
      <table>
        <thead><tr><th>Client</th><th>Tél</th><th>Véhicule</th><th>Plaque</th><th></th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

/* Clients view */
const clientsTbody = $("clientsTbody");
const clientsCards = $("clientsCards");
const clientStatsModal = $("clientStatsModal");
const btnCloseClientStats = $("btnCloseClientStats");
const csTitle = $("csTitle");
const csSubtitle = $("csSubtitle");
const csCount = $("csCount");
const csSales = $("csSales");
const csParts = $("csParts");
const csNet = $("csNet");
const csTbody = $("csTbody");
if(btnCloseClientStats) btnCloseClientStats.onclick = ()=>{ if(clientStatsModal) clientStatsModal.style.display="none"; };
if(clientStatsModal) clientStatsModal.addEventListener("click", (e)=>{ if(e.target===clientStatsModal) clientStatsModal.style.display="none"; });

const clientsCount = $("clientsCount");
const promoSelCount = $("promoSelCount");
const btnPromoSelectAll = $("btnPromoSelectAll");
const btnPromoSelectHasEmail = $("btnPromoSelectHasEmail");
const btnPromoSelectNone = $("btnPromoSelectNone");
const btnFilterUnpaid = $("btnFilterUnpaid");
const btnFilterAll = $("btnFilterAll");
const unpaidCountEl = $("unpaidCount");
let clientPayFilter = "all"; // all | unpaid

$("btnClientsSearch").onclick = ()=>renderClients();
$("btnClientsClear").onclick = ()=>{ $("clientsSearch").value=""; renderClients(); };
if(btnFilterUnpaid) btnFilterUnpaid.onclick = ()=>{ clientPayFilter="unpaid"; renderClients(); };
if(btnFilterAll) btnFilterAll.onclick = ()=>{ clientPayFilter="all"; renderClients(); };


if(btnPromoSelectAll) btnPromoSelectAll.onclick = ()=>window.__promoSelectAll(true);
if(btnPromoSelectHasEmail) btnPromoSelectHasEmail.onclick = ()=>window.__promoSelectHasEmail();
if(btnPromoSelectNone) btnPromoSelectNone.onclick = ()=>window.__promoSelectAll(false);

/* ============
   Revenue view
=========== */
const revPresetEl = $("revPreset");
const revPayFilterEl = $("revPayFilter");
const revFromEl = $("revFrom");
const revToEl = $("revTo");
const revTotalEl = $("revTotal");
const revCountEl = $("revCount");
const revAvgEl = $("revAvg");
const revPartsCostEl = $("revPartsCost");
const revProfitEl = $("revProfit");
const revTbody = $("revTbody");
const revByPayTbody = $("revByPayTbody");
const revByDateTbody = $("revByDateTbody");
const revCards = $("revCards");
const revByPayCards = $("revByPayCards");
const revByDateCards = $("revByDateCards");
const btnRevApply = $("btnRevApply");
const btnRevExport = $("btnRevExport");

/* ============
   Parts expenses view
=========== */
const btnNewPartsExpense = $("btnNewPartsExpense");
const btnPartsExpExport = $("btnPartsExpExport");
const pexPresetEl = $("pexPreset");
const pexFromEl = $("pexFrom");
const pexToEl = $("pexTo");
const pexPayFilterEl = $("pexPayFilter");
const pexSubtotalEl = $("pexSubtotal");
const pexTaxEl = $("pexTax");
const pexTotalEl = $("pexTotal");
const pexCountEl = $("pexCount");
const pexTbody = $("pexTbody");

/* ============
   Suppliers view
=========== */
const btnNewSupplier = $("btnNewSupplier");
const suppliersCountEl = $("suppliersCount");
const suppliersActiveCountEl = $("suppliersActiveCount");
const suppliersPhoneCountEl = $("suppliersPhoneCount");
const suppliersEmailCountEl = $("suppliersEmailCount");
const suppliersTbody = $("suppliersTbody");

function renderSuppliers(){
  if(!$('viewSuppliers')) return;
  const rows = Array.isArray(suppliers) ? [...suppliers] : [];
  rows.sort((a,b)=> String(a.name||'').localeCompare(String(b.name||''), 'fr', {sensitivity:'base'}));
  if(suppliersCountEl) suppliersCountEl.textContent = String(rows.length);
  if(suppliersActiveCountEl) suppliersActiveCountEl.textContent = String(rows.filter(x=>x.active !== false).length);
  if(suppliersPhoneCountEl) suppliersPhoneCountEl.textContent = String(rows.filter(x=>String(x.phone||'').trim()).length);
  if(suppliersEmailCountEl) suppliersEmailCountEl.textContent = String(rows.filter(x=>String(x.email||'').trim()).length);
  if(!suppliersTbody) return;
  if(rows.length===0){
    suppliersTbody.innerHTML = '<tr><td class="muted" colspan="7">Aucun fournisseur.</td></tr>';
    return;
  }
  suppliersTbody.innerHTML = rows.map(x=>`<tr>
    <td><b>${safe(x.name||'')}</b></td>
    <td>${safe(x.contact||'')}</td>
    <td>${safe(x.phone||'')}</td>
    <td>${safe(x.email||'')}</td>
    <td>${safe(x.city||'')}</td>
    <td>${safe(x.note||'')}</td>
    <td class="no-print" style="white-space:nowrap">
      <button class="btn btn-ghost btn-small" onclick="window.__editSupplier('${x.id}')">Modifier</button>
      <button class="btn btn-ghost btn-small" onclick="window.__deleteSupplier('${x.id}')">Supprimer</button>
    </td>
  </tr>`).join('');
}

function supplierOptionsHtml(selected=''){
  return (Array.isArray(suppliers)?suppliers:[]).map(s=>{
    const name = String(s.name||'').trim();
    if(!name) return '';
    const sel = name===selected ? 'selected' : '';
    return `<option value="${safe(name)}" ${sel}>${safe(name)}</option>`;
  }).join('');
}

function openSupplierModal(existing){
  if(currentRole !== 'admin' && currentRole !== 'superadmin') return;
  const x = existing || {};
  const html = `
    <form class="form" id="formSupplier">
      <label>Nom du fournisseur</label>
      <input class="input" name="name" required placeholder="Ex: NAPA" value="${safe(x.name||'')}" />

      <div class="row" style="gap:10px; flex-wrap:wrap">
        <div style="flex:1; min-width:160px">
          <label>Contact</label>
          <input class="input" name="contact" placeholder="Nom du contact" value="${safe(x.contact||'')}" />
        </div>
        <div style="flex:1; min-width:160px">
          <label>Téléphone</label>
          <input class="input" name="phone" placeholder="514..." value="${safe(x.phone||'')}" />
        </div>
      </div>

      <div class="row" style="gap:10px; flex-wrap:wrap">
        <div style="flex:1; min-width:160px">
          <label>Email</label>
          <input class="input" name="email" type="email" placeholder="email@exemple.com" value="${safe(x.email||'')}" />
        </div>
        <div style="flex:1; min-width:160px">
          <label>Ville</label>
          <input class="input" name="city" placeholder="Montréal" value="${safe(x.city||'')}" />
        </div>
      </div>

      <label>Adresse</label>
      <input class="input" name="address" placeholder="Adresse" value="${safe(x.address||'')}" />

      <label>Note</label>
      <textarea class="input" name="note" rows="3" placeholder="Notes">${safe(x.note||'')}</textarea>

      <label style="display:flex; align-items:center; gap:8px; margin-top:8px">
        <input type="checkbox" name="active" ${x.active === false ? '' : 'checked'} /> Actif
      </label>

      <div class="row" style="margin-top:12px; gap:10px">
        <button class="btn btn-primary" type="submit">Enregistrer</button>
        <button class="btn btn-ghost" type="button" data-modal-close>Annuler</button>
      </div>
    </form>
  `;
  showModal(existing ? 'Modifier fournisseur' : 'Nouveau fournisseur', html);
  const form = modalBody.querySelector('#formSupplier');
  if(!form) return;
  form.addEventListener('submit', async (e)=>{
    e.preventDefault();
    const fd = new FormData(form);
    const data = {
      name: String(fd.get('name')||'').trim(),
      contact: String(fd.get('contact')||'').trim(),
      phone: String(fd.get('phone')||'').trim(),
      email: String(fd.get('email')||'').trim().toLowerCase(),
      city: String(fd.get('city')||'').trim(),
      address: String(fd.get('address')||'').trim(),
      note: String(fd.get('note')||'').trim(),
      active: fd.get('active') === 'on',
      updatedAt: serverTimestamp(),
    };
    if(!data.name){ showToast('Nom fournisseur obligatoire', true); return; }
    try{
      if(existing && existing.id){
        await updateDoc(garageDoc('suppliers', existing.id), data);
      }else{
        data.createdAt = serverTimestamp();
        await addDoc(colSuppliers(), data);
      }
      closeModal();
      showToast('Fournisseur enregistré ✅');
    }catch(err){
      console.error(err);
      showToast('Erreur fournisseur: '+(err.message||err), true);
    }
  });
}

window.__editSupplier = (id)=>{
  const x = (Array.isArray(suppliers)?suppliers:[]).find(r=>r.id===id);
  if(x) openSupplierModal(x);
};

window.__deleteSupplier = async (id)=>{
  if(!confirm('Supprimer ce fournisseur ?')) return;
  try{
    await deleteDoc(garageDoc('suppliers', id));
    showToast('Fournisseur supprimé');
  }catch(err){
    console.error(err);
    showToast('Erreur suppression: '+(err.message||err), true);
  }
}

/* ============
   Fiscal view
=========== */
const fiscPresetEl = $("fiscPreset");
const fiscFromEl = $("fiscFrom");
const fiscToEl = $("fiscTo");
const fiscPayFilterEl = $("fiscPayFilter");
const fiscPurchasePayFilterEl = $("fiscPurchasePayFilter");
const fiscRevenueEl = $("fiscRevenue");
const fiscExpensesEl = $("fiscExpenses");
const fiscProfitEl = $("fiscProfit");
const fiscTaxCollectedEl = $("fiscTaxCollected");
const fiscTPSCollectedEl = $("fiscTPSCollected");
const fiscTVQCollectedEl = $("fiscTVQCollected");
const fiscTaxPaidEl = $("fiscTaxPaid");
const fiscTPSPaidEl = $("fiscTPSPaid");
const fiscTVQPaidEl = $("fiscTVQPaid");
const fiscTaxPayableEl = $("fiscTaxPayable");
const fiscNetAfterTaxEl = $("fiscNetAfterTax");
const fiscCountEl = $("fiscCount");
const fiscTbody = $("fiscTbody");
const btnFiscExportCsv = $("btnFiscExportCsv");
const btnFiscExportComptable = $("btnFiscExportComptable");
const btnFiscExportComptablePdf = $("btnFiscExportComptablePdf");

/* ============
   Invoices (Parts) / Profit
=========== */
const btnNewInvoice = $("btnNewInvoice");
const invoiceFormBox = $("invoiceFormBox");
const formInvoice = $("formInvoice");
const invCustomerEl = $("invCustomer");
const invEmailEl = $("invEmail");
const invSupplierEl = $("invSupplier");
const invDateEl = $("invDate");
const invPurchaseDateEl = $("invPurchaseDate");
const invInstallDateEl = $("invInstallDate");
const invRefEl = $("invRef");
const invWorkorderEl = $("invWorkorder");
const invPayMethodEl = $("invPayMethod");
const invHoursEl = $("invHours");
const invLaborEl = $("invLabor");
const invSubTotalEl = $("invSubTotal");
const invTaxTotalEl = $("invTaxTotal");
const invTpsTotalEl = $("invTpsTotal");
const invTvqTotalEl = $("invTvqTotal");
const invGrandTotalEl = $("invGrandTotal");
const invCardFeeEl = $("invCardFee");
const invNetProfitEl = $("invNetProfit");
const invItemsTbody = $("invItemsTbody");
const btnInvAddLine = $("btnInvAddLine");
const btnInvCancel = $("btnInvCancel");
const btnInvPrint = $("btnInvPrint");
const btnInvPdf = $("btnInvPdf");
const btnInvEmail = $("btnInvEmail");
const invCostTotalEl = $("invCostTotal");
const invSellTotalEl = $("invSellTotal");
const invProfitTotalEl = $("invProfitTotal");
const inv30CountEl = $("inv30Count");
const inv30ProfitEl = $("inv30Profit");
const inv30MarginEl = $("inv30Margin");
const invListTbody = $("invListTbody");
const invTopClientsTbody = $("invTopClientsTbody");
const invMonthlyTbody = $("invMonthlyTbody");
const invMonthlyChartEl = $("invMonthlyChart");
const invPayTbody = $("invPayTbody");
const invTopRepairsTbody = $("invTopRepairsTbody");
const invFromEl = $("invFrom");
const invToEl = $("invTo");
const btnInvThisMonth = $("btnInvThisMonth");
const btnInvLastMonth = $("btnInvLastMonth");
const btnInvAll = $("btnInvAll");
const btnInvExport = $("btnInvExport");

let editingInvoiceId = null;
let invFilter = { from: null, to: null };

// UI wiring (Invoices)
if(invDateEl) invDateEl.value = todayISO();
if(invHoursEl) invHoursEl.value = "0";
    if(invLaborEl) invLaborEl.value = "0";
if(btnNewInvoice) btnNewInvoice.onclick = ()=>{
  if(currentRole !== "admin"){ showToast("Accès réservé admin."); return; }
  openInvoiceForm(true);
};
if(btnInvAddLine) btnInvAddLine.onclick = ()=>{ ensureInvoiceLine(); recalcInvoiceTotals(); };
if(btnInvCancel) btnInvCancel.onclick = ()=>{ openInvoiceForm(false); };
if(formInvoice) formInvoice.onsubmit = createInvoiceFromForm;
if(invLaborEl) invLaborEl.addEventListener("input", recalcInvoiceTotals);
if(invHoursEl) invHoursEl.addEventListener("input", recalcInvoiceTotals);
if(invPayMethodEl) invPayMethodEl.addEventListener("change", recalcInvoiceTotals);

function invSetMonth(which){
  const now = new Date();
  const d = new Date(now.getFullYear(), now.getMonth() + (which==="last"?-1:0), 1);
  const from = isoDate(firstDayOfMonth(d));
  const to = isoDate(lastDayOfMonth(d));
  setInvFilter(from, to);
}
if(btnInvThisMonth) btnInvThisMonth.onclick = ()=>invSetMonth("this");
if(btnInvLastMonth) btnInvLastMonth.onclick = ()=>invSetMonth("last");
if(btnInvAll) btnInvAll.onclick = ()=>setInvFilter(null, null);
if(invFromEl) invFromEl.onchange = ()=>setInvFilter(invFromEl.value||null, invToEl?.value||null);
if(invToEl) invToEl.onchange = ()=>setInvFilter(invFromEl?.value||null, invToEl.value||null);
if(btnInvExport) btnInvExport.onclick = ()=>exportInvoicesCSV();
 // dates (YYYY-MM-DD)

function todayISO(){
  const d = new Date();
  const tzOff = d.getTimezoneOffset()*60000;
  return new Date(d.getTime()-tzOff).toISOString().slice(0,10);
}

function openInvoiceForm(open=true){
  if(!invoiceFormBox) return;
  invoiceFormBox.style.display = open ? "" : "none";
}

function ensureInvoiceLine(desc="", qty=1, cost=0, price=0){
  const tr = document.createElement('tr');
  tr.innerHTML = `
    <td><input class="input input-mini" data-k="desc" placeholder="ex: Plaquettes de frein" value="${safe(desc)}" /></td>
    <td><input class="input input-mini" data-k="qty" type="number" min="1" value="${Number(qty||1)}" /></td>
    <td style="text-align:right"><input class="input input-mini" data-k="cost" type="number" step="0.01" min="0" value="${Number(cost||0)}" /></td>
    <td style="text-align:right"><input class="input input-mini" data-k="price" type="number" step="0.01" min="0" value="${Number(price||0)}" /></td>
    <td class="no-print" style="text-align:right"><button class="btn btn-ghost btn-icon" type="button" title="Supprimer">✕</button></td>
  `;
  tr.querySelector('button').addEventListener('click', ()=>{ tr.remove(); recalcInvoiceTotals(); });
  tr.querySelectorAll('input').forEach(inp=> inp.addEventListener('input', recalcInvoiceTotals));
  invItemsTbody.appendChild(tr);
}

function readInvoiceItems(){
  const items = [];
  invItemsTbody.querySelectorAll('tr').forEach(tr=>{
    const getv = (k)=> tr.querySelector(`[data-k="${k}"]`)?.value;
    const desc = String(getv('desc')||"").trim();
    const qty = Math.max(1, Number(getv('qty')||1));
    const cost = Math.max(0, Number(getv('cost')||0));
    const price = Math.max(0, Number(getv('price')||0));
    if(desc || cost || price){
      items.push({desc, qty, cost, price});
    }
  });
  return items;
}

function recalcInvoiceTotals(){
  const items = readInvoiceItems();
  const hours = Math.max(0, Number(invHoursEl?.value || 0));
  const laborManual = Math.max(0, Number(invLaborEl?.value || 0));
  const labor = hours>0 ? (hours * Number(settings.laborRate||0)) : laborManual;
  if(invLaborEl) invLaborEl.value = String(labor.toFixed(2));
  let partsCost = 0;
  let partsSell = 0;
  for(const it of items){
    partsCost += Number(it.cost||0) * Number(it.qty||1);
    partsSell += Number(it.price||0) * Number(it.qty||1);
  }
  const subTotal = partsSell + labor;

  const tps = Number(settings.tpsRate||0);
  const tvq = Number(settings.tvqRate||0);
  const tpsAmount = subTotal * tps;
  const tvqAmount = subTotal * tvq;
  const taxTotal = tpsAmount + tvqAmount;
  const grandTotal = subTotal + taxTotal;

  const isCard = (invPayMethodEl?.value || "") === "card";
  const cardFeeRate = Number(settings.cardFeeRate||0);
  const cardFee = isCard ? (grandTotal * cardFeeRate) : 0;

  // Profit net: on exclut les taxes (pas un revenu) et on retire les frais carte
  const netProfit = subTotal - partsCost - cardFee;

  if(invCostTotalEl) invCostTotalEl.textContent = money(partsCost);
  if(invSubTotalEl) invSubTotalEl.textContent = money(subTotal);
  if(invTpsTotalEl) invTpsTotalEl.textContent = money(tpsAmount);
  if(invTvqTotalEl) invTvqTotalEl.textContent = money(tvqAmount);
  if(invTaxTotalEl) invTaxTotalEl.textContent = money(taxTotal);
  if(invGrandTotalEl) invGrandTotalEl.textContent = money(grandTotal);
  if(invCardFeeEl) invCardFeeEl.textContent = money(cardFee);
  if(invNetProfitEl) invNetProfitEl.textContent = money(netProfit);
}

function fillInvoiceCustomers(){
  if(!invCustomerEl) return;
  const list = [...customers].sort((a,b)=> String(a.fullName||"").localeCompare(String(b.fullName||""), 'fr'));
  invCustomerEl.innerHTML = list.map(c=>`<option value="${c.id}">${safe(c.fullName||'(Sans nom)')}</option>`).join('');
}

function fillInvoiceSuppliers(selected=""){
  if(!invSupplierEl) return;
  const list = [...(Array.isArray(suppliers)?suppliers:[])].sort((a,b)=> String(a.name||"").localeCompare(String(b.name||""), 'fr'));
  const opts = ['<option value="">— Aucun —</option>'];
  for(const s of list){
    const val = String(s.name||"");
    opts.push(`<option value="${safe(val)}" ${String(selected)===val?'selected':''}>${safe(val)}</option>`);
  }
  invSupplierEl.innerHTML = opts.join('');
}

function workorderDisplay(wo){
  if(!wo) return "";
  const v = getVehicle(wo.vehicleId) || {};
  const c = v.customerId ? (getCustomer(v.customerId) || {}) : {};
  const client = c.fullName || "";
  const veh = [v.year,v.make,v.model].filter(Boolean).join(" ") + (v.plate?` (${v.plate})`:"");
  const d = String(wo.createdAt||"").slice(0,10);
  const total = money(wo.total||0);
  const parts = [d, client, veh].filter(Boolean).join(" — ");
  return parts ? `${parts} — ${total}` : `${wo.id} — ${total}`;
}

function invPaymentLabel(pm){
  const v = String(pm||"").toLowerCase();
  if(v==="cash") return "Cash";
  if(v==="card") return "Carte";
  if(v==="etransfer") return "Interac";
  if(v==="bank") return "Virement";
  if(v==="cheque") return "Chèque";
  if(v==="other") return "Autre";
  return v || "—";
}

function fillInvoiceWorkorders(){
  if(!invWorkorderEl) return;
  // option vide
  const opts = ['<option value="">— Aucune —</option>'];
  // On ne liste que les réparations (workorders) existantes, triées par date desc
  const list = [...workorders].sort(byCreatedDesc);
  for(const wo of list){
    opts.push(`<option value="${wo.id}">${safe(workorderDisplay(wo))}</option>`);
  }
  invWorkorderEl.innerHTML = opts.join('');
}

async function createInvoiceFromForm(e){
  e.preventDefault();
  const customerId = invCustomerEl.value;
  const customer = customers.find(c=>c.id===customerId);
  const workorderId = (invWorkorderEl && invWorkorderEl.value) ? invWorkorderEl.value : "";

  // Référence auto format GP-0001 si vide
  let refVal = String(invRefEl?.value || "").trim();
  if(!refVal){
    try{
      const cRef = docCounters();
      // runTransaction peut ne pas être importé selon version; on teste
      if(typeof runTransaction === "function"){
        const seq = await runTransaction(db, async (tx)=>{
          const snap = await tx.get(cRef);
          const cur = (snap.exists() && snap.data().invoiceSeq) ? Number(snap.data().invoiceSeq) : 0;
          const next = cur + 1;
          tx.set(cRef, { invoiceSeq: next, updatedAt: serverTimestamp() }, { merge:true });
          return next;
        });
        refVal = "GP-" + String(seq).padStart(4,"0");
      }else{
        // fallback: timestamp
        refVal = "GP-" + String(Date.now()).slice(-6);
      }
      if(invRefEl) invRefEl.value = refVal;
    }catch(e){
      refVal = "GP-" + String(Date.now()).slice(-6);
      if(invRefEl) invRefEl.value = refVal;
    }
  }
  const items = readInvoiceItems();
  if(items.length===0){
    alert("Ajoute au moins une ligne (pièce / service). ");
    return;
  }
  const dateStr = invDateEl.value || todayISO();
  const d = new Date(dateStr+"T12:00:00");
  let costTotal = 0, sellTotal = 0;
  for(const it of items){
    costTotal += Number(it.cost||0) * Number(it.qty||1);
    sellTotal += Number(it.price||0) * Number(it.qty||1);
  }
  const profit = sellTotal - costTotal;
  const ref = String(invRefEl.value||"").trim();

  // Si la référence existe déjà, propose: 1) modifier la facture existante, 2) ajouter au même facture
  if(ref && !editingInvoiceId){
    const existing = invoices.find(x => String(x.ref||"").trim() === ref);
    if(existing){
      const wantEdit = confirm(`La référence "${ref}" existe déjà.\n\nOK = Modifier cette facture\nAnnuler = Autre option`);
      if(wantEdit){
        // Ouvre en mode édition
        editingInvoiceId = existing.id;
        openInvoiceForm(true);
        invCustomerEl.value = existing.customerId || "";
        const dt = existing.date instanceof Date ? existing.date : (existing.date?.toDate ? existing.date.toDate() : new Date(existing.date));
        invDateEl.value = isoDate(dt);
        if(invPurchaseDateEl) invPurchaseDateEl.value = existing.purchaseDate || "";
        if(invInstallDateEl) invInstallDateEl.value = existing.installDate || "";
        invRefEl.value = existing.ref || ref;
        if(invSupplierEl) fillInvoiceSuppliers(existing.supplier || "");
        if(invPayMethodEl) invPayMethodEl.value = existing.paymentMethod || "cash";
        if(invWorkorderEl) invWorkorderEl.value = existing.workorderId || "";
        invItemsTbody.innerHTML = "";
        (existing.items||[]).forEach(it=>ensureInvoiceLine(it.desc,it.qty,it.cost,it.price));
        recalcInvoiceTotals();
        showToast("Facture ouverte en modification.");
        return;
      }

      const wantMerge = confirm(`Ajouter ces lignes à la facture "${ref}" (même facture) ?`);
      if(wantMerge){
        try{
          const mergedItems = [...(existing.items||[]), ...items];
          let cTot=0, sTot=0;
          for(const it of mergedItems){
            cTot += Number(it.cost||0) * Number(it.qty||1);
            sTot += Number(it.price||0) * Number(it.qty||1);
          }
          const pTot = sTot - cTot;

          // On garde le client de la facture existante (référence unique = 1 facture)
          const upd = {
            // garde ref identique
            ref: ref,
            customerId: existing.customerId || customerId,
            customerName: existing.customerName || (customer?.fullName||""),
            customerEmail: existing.customerEmail || String(invEmailEl?.value||"").trim(),
            workorderId: existing.workorderId || workorderId || "",
            workorderLabel: existing.workorderLabel || (workorderId ? workorderDisplay(workorders.find(w=>w.id===workorderId)) : ""),
            supplier: existing.supplier || String(invSupplierEl?.value||"").trim(),
            paymentMethod: (invPayMethodEl?.value || existing.paymentMethod || "cash"),
            date: existing.date || d,
            purchaseDate: existing.purchaseDate || (invPurchaseDateEl?.value||""),
            installDate: existing.installDate || (invInstallDateEl?.value||""),
            items: mergedItems,
            totals: (function(){
  const hours = Number(existing.hours||0);
  const labor = Number(existing.labor||0);
  const subTotal = sTot + labor;
  const tpsRate = Number(settings.tpsRate||0);
  const tvqRate = Number(settings.tvqRate||0);
  const tpsAmount = subTotal * tpsRate;
  const tvqAmount = subTotal * tvqRate;
  const taxTotal = tpsAmount + tvqAmount;
  const grandTotal = subTotal + taxTotal;
  const cardFee = (String(upd.paymentMethod||"").toLowerCase()==="card") ? (grandTotal*Number(settings.cardFeeRate||0)) : 0;
  const netProfit = subTotal - cTot - cardFee;
  return { partsCost: cTot, partsSell: sTot, labor, subTotal, tpsAmount, tvqAmount, taxTotal, tax: taxTotal, grandTotal, cardFee, netProfit };
})(),
            updatedAt: serverTimestamp(),
            updatedBy: currentUid,
          };

          await updateDoc(doc(colInvoices(), existing.id), upd);
          openInvoiceForm(false);
          formInvoice.reset();
          invItemsTbody.innerHTML = "";
          ensureInvoiceLine();
          invDateEl.value = todayISO();
          if(invWorkorderEl) invWorkorderEl.value = "";
          if(invSupplierEl) fillInvoiceSuppliers("");
          if(invPayMethodEl) invPayMethodEl.value = "cash";
    if(invHoursEl) invHoursEl.value = "0";
    if(invLaborEl) invLaborEl.value = "0";
          if(invPurchaseDateEl) invPurchaseDateEl.value = "";
          if(invInstallDateEl) invInstallDateEl.value = "";
          recalcInvoiceTotals();
          showToast("Lignes ajoutées à la facture existante.");
          return;
        }catch(err){

          alert("Erreur ajout au même facture: "+(err?.message||err));
          return;
        }
      }

      // Si l'utilisateur refuse les 2 options, on laisse continuer (créera une 2e facture sans ref, ou changer ref)
      alert("Change la référence si tu veux créer une nouvelle facture.");
      return;
    }
  }

  const hoursCalc = Math.max(0, Number(invHoursEl?.value || 0));
  const laborCalc = hoursCalc>0 ? (hoursCalc*Number(settings.laborRate||0)) : Math.max(0, Number(invLaborEl?.value || 0));
  const subCalc = sellTotal + laborCalc;
  const tpsRate = Number(settings.tpsRate||0);
  const tvqRate = Number(settings.tvqRate||0);
  const tpsCalc = subCalc * tpsRate;
  const tvqCalc = subCalc * tvqRate;
  const taxCalc = tpsCalc + tvqCalc;
  const grandCalc = subCalc + taxCalc;
  const cardCalc = ((invPayMethodEl?.value||"")==="card") ? (grandCalc*Number(settings.cardFeeRate||0)) : 0;
  const netCalc = subCalc - costTotal - cardCalc;

  const payload = {
    ref: ref,

    paymentMethod: (invPayMethodEl?.value || "cash"),
    customerId,
    customerName: customer?.fullName || "",
    customerEmail: String(invEmailEl?.value||"").trim(),
    supplier: String(invSupplierEl?.value||"").trim(),
    workorderId: workorderId || "",
    workorderLabel: workorderId ? workorderDisplay(workorders.find(w=>w.id===workorderId)) : "",
    date: d,
    purchaseDate: invPurchaseDateEl?.value || "",
    installDate: invInstallDateEl?.value || "",
    items,
    hours: hoursCalc,
    labor: laborCalc,
    totals: {
      partsCost: costTotal,
      partsSell: sellTotal,
      labor: laborCalc,
      subTotal: subCalc,
      tpsAmount: tpsCalc,
      tvqAmount: tvqCalc,
      taxTotal: taxCalc,
      tax: taxCalc,
      grandTotal: grandCalc,
      cardFee: cardCalc,
      netProfit: netCalc
    },
    createdAt: serverTimestamp(),
    createdBy: currentUid,
  };
  try{
    if(editingInvoiceId){
      await updateDoc(doc(colInvoices(), editingInvoiceId), payload);
    }else{
      await addDoc(colInvoices(), payload);
    }
    openInvoiceForm(false);
    formInvoice.reset();
    invItemsTbody.innerHTML = "";
    ensureInvoiceLine();
    invDateEl.value = todayISO();
    if(invWorkorderEl) invWorkorderEl.value = "";
    if(invSupplierEl) fillInvoiceSuppliers("");
    if(invPayMethodEl) invPayMethodEl.value = "cash";
    if(invHoursEl) invHoursEl.value = "0";
    if(invLaborEl) invLaborEl.value = "0";
    recalcInvoiceTotals();
    editingInvoiceId = null;
    alert("Facture enregistrée.");
  }catch(err){

    alert("Erreur enregistrement facture: "+(err?.message||err));
  }
}

async function deleteInvoice(id){
  if(!confirm("Supprimer cette facture ?")) return;
  try{
    await deleteDoc(doc(colInvoices(), id));
  }catch(err){

    alert("Erreur suppression: "+(err?.message||err));
  }
}

function setInvFilter(fromISO, toISO){
  invFilter.from = fromISO || null;
  invFilter.to = toISO || null;
  if(invFromEl) invFromEl.value = invFilter.from || "";
  if(invToEl) invToEl.value = invFilter.to || "";
  renderInvoices();
}
function invInRange(inv){
  // inv.date stored as string YYYY-MM-DD or Timestamp/Date
  const d = inv.date instanceof Date ? inv.date : (inv.date?.toDate ? inv.date.toDate() : new Date(inv.date));
  const iso = isoDate(d);
  if(invFilter.from && iso < invFilter.from) return false;
  if(invFilter.to && iso > invFilter.to) return false;
  return true;
}
function getFilteredInvoices(){
  return invoices.filter(invInRange);
}
function downloadText(filename, text){
  const blob = new Blob([text], {type:"text/plain;charset=utf-8"});
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(()=>URL.revokeObjectURL(url), 500);
}
function exportInvoicesCSV(){
  const list = getFilteredInvoices().sort((a,b)=>{
    const da = a.date instanceof Date ? a.date : (a.date?.toDate ? a.date.toDate() : new Date(a.date));
    const db = b.date instanceof Date ? b.date : (b.date?.toDate ? b.date.toDate() : new Date(b.date));
    return db - da;
  });
  const header = ["date","ref","supplier","client","payment_method","labor","sub_total","tax","grand_total","card_fee","parts_cost","net_profit","item_desc","item_qty","item_cost","item_price"];
  const rows = [header.join(",")];
  for(const inv of list){
    const dt = isoDate(inv.date instanceof Date ? inv.date : (inv.date?.toDate ? inv.date.toDate() : new Date(inv.date)));
    const ref = (inv.ref||"").replaceAll('"','""');
    const supplier = String(inv.supplier||"").replaceAll('"','""');
    const client = (inv.customerName||"").replaceAll('"','""');
    const pm = (inv.paymentMethod||"").replaceAll('"','""');
    const labor = Number(inv.labor||0);
    const subT = Number(inv.totals?.subTotal ?? 0);
    const taxT = Number(inv.totals?.tax ?? 0);
    const grandT = Number(inv.totals?.grandTotal ?? inv.totals?.sell ?? 0);
    const cardF = Number(inv.totals?.cardFee ?? 0);
    const costT = Number(inv.totals?.partsCost ?? inv.totals?.cost ?? 0);
    const netP = Number(inv.totals?.netProfit ?? inv.totals?.profit ?? 0);
    const sellT = grandT;
    const profitT = netP;
    const items = Array.isArray(inv.items) ? inv.items : [];
    if(items.length===0){
      rows.push([dt, `"${ref}"`, `"${supplier}"`, `"${client}"`, `"${pm}"`, labor, subT, taxT, grandT, cardF, costT, netP, "", "", "", ""].join(","));
    }else{
      for(const it of items){
        const desc = String(it.desc||"").replaceAll('"','""');
        const qty = Number(it.qty||0);
        const cost = Number(it.cost||0);
        const price = Number(it.price||0);
        rows.push([dt, `"${ref}"`, `"${supplier}"`, `"${client}"`, `"${pm}"`, labor, subT, taxT, grandT, cardF, costT, netP, `"${desc}"`, qty, cost, price].join(","));
      }
    }
  }
  const fname = "factures_pieces.csv";
  downloadText(fname, rows.join("\n"));
}

function renderInvoices(){
  if(!invListTbody) return;
  // 30 derniers jours
  const now = new Date();
  const from = new Date(now.getTime() - 30*24*60*60*1000);
  const inv30 = invoices.filter(inv=>{
    const dt = inv.date instanceof Date ? inv.date : (inv.date?.toDate ? inv.date.toDate() : new Date(inv.date));
    return dt >= from;
  });
  const count = inv30.length;
  let profit = 0, sell=0;
  for(const inv of inv30){
    profit += Number(inv.totals?.netProfit ?? inv.totals?.profit ?? 0);
    sell += Number(inv.totals?.grandTotal ?? inv.totals?.sell ?? 0);
  }
  const margin = sell>0 ? (profit/sell*100) : 0;
  inv30CountEl.textContent = String(count);
  inv30ProfitEl.textContent = money(profit);
  inv30MarginEl.textContent = `${margin.toFixed(1)}%`;

  const list = [...getFilteredInvoices()].sort((a,b)=>{
    const da = a.date instanceof Date ? a.date : (a.date?.toDate ? a.date.toDate() : new Date(a.date));
    const db = b.date instanceof Date ? b.date : (b.date?.toDate ? b.date.toDate() : new Date(b.date));
    return db - da;
  });
  if(list.length===0){
    invListTbody.innerHTML = '<tr><td class="muted" colspan="11">Aucune facture pour ce filtre.</td></tr>';
    return;
  }
  invListTbody.innerHTML = list.map(inv=>{
    const dt = inv.date instanceof Date ? inv.date : (inv.date?.toDate ? inv.date.toDate() : new Date(inv.date));
    const ds = isoDate(dt);
    const ref = safe(inv.ref||"");
    const wo = safe(inv.workorderLabel||"");
    const cust = safe(inv.customerName||"");
    const sub = money(
      (inv.totals?.subTotal ?? inv.totals?.subTotalHT ?? inv.totals?.sub ?? 0) ||
      (Number(inv.totals?.grandTotal ?? inv.totals?.sell ?? 0) - Number(inv.totals?.taxTotal ?? inv.totals?.tax ?? 0))
    );
    const tps = money(inv.totals?.tpsAmount ?? 0);
    const tvq = money(inv.totals?.tvqAmount ?? 0);
    const total = money(inv.totals?.grandTotal ?? inv.totals?.sell ?? 0);
    const cost = money(inv.totals?.partsCost ?? inv.totals?.cost ?? 0);
    const profit = money(inv.totals?.netProfit ?? inv.totals?.profit ?? 0);
    return `
      <tr>
        <td>${ds}</td>
        <td>${ref}</td>
        <td>${safe(inv.supplier||"—")}</td>
        <td>${cust}</td>
        <td>${safe(invPaymentLabel(inv.paymentMethod))}</td>
        <td style="text-align:right">${sub}</td>
        <td style="text-align:right">${tps}</td>
        <td style="text-align:right">${tvq}</td>
        <td style="text-align:right">${total}</td>
        <td style="text-align:right">${cost}</td>
        <td style="text-align:right"><b>${profit}</b></td>
        <td class="no-print" style="text-align:right"><button class="btn btn-ghost" data-edit-inv="${inv.id}">Modifier</button> <button class="btn btn-ghost" data-del-inv="${inv.id}">Supprimer</button></td>
      </tr>
    `;
  }).join('');

  renderInvoicesAnalytics(list);

  
  invListTbody.querySelectorAll('[data-edit-inv]').forEach(btn=>{
    btn.addEventListener('click', ()=>{
      const inv = invoices.find(i=>i.id===btn.getAttribute('data-edit-inv'));
      if(!inv) return;
      editingInvoiceId = inv.id;
      openInvoiceForm(true);
      invCustomerEl.value = inv.customerId || "";
      invDateEl.value = isoDate(inv.date instanceof Date ? inv.date : inv.date.toDate());
      if(invPurchaseDateEl) invPurchaseDateEl.value = inv.purchaseDate || "";
      if(invInstallDateEl) invInstallDateEl.value = inv.installDate || "";
      invRefEl.value = inv.ref || "";
      if(invSupplierEl) fillInvoiceSuppliers(inv.supplier || "");
  if(invEmailEl) invEmailEl.value = inv.customerEmail || "";
      if(invPayMethodEl) invPayMethodEl.value = inv.paymentMethod || "cash";
      if(invHoursEl) invHoursEl.value = String(inv.hours ?? 0);
      if(invLaborEl) invLaborEl.value = String(inv.labor ?? 0);
      recalcInvoiceTotals();
      invItemsTbody.innerHTML = "";
      (inv.items||[]).forEach(it=>ensureInvoiceLine(it.desc,it.qty,it.cost,it.price));
      recalcInvoiceTotals();
    });
  });

  invListTbody.querySelectorAll('[data-del-inv]').forEach(btn=>{
    btn.addEventListener('click', ()=>deleteInvoice(btn.getAttribute('data-del-inv')));
  });

function renderInvoicesAnalytics(list){
  // list = factures filtrées
  if(!invTopClientsTbody || !invMonthlyTbody || !invMonthlyChartEl) return;
  // invPayTbody is optional

  if(!Array.isArray(list) || list.length===0){
    invTopClientsTbody.innerHTML = '<tr><td class="muted" colspan="5">Aucune donnée.</td></tr>';
    invMonthlyTbody.innerHTML = '<tr><td class="muted" colspan="5">Aucune donnée.</td></tr>';
    invMonthlyChartEl.textContent = "—";
    if(invPayTbody) invPayTbody.innerHTML = '<tr><td class="muted" colspan="5">Aucune donnée.</td></tr>';
    return;
  }

  // ---- Top clients ----
  const byClient = new Map();
  for(const inv of list){
    const key = inv.customerId || inv.customerName || "(Sans client)";
    const name = inv.customerName || "(Sans client)";
    const cur = byClient.get(key) || {name, sell:0, cost:0, profit:0};
    const cost = Number(inv.totals?.partsCost ?? inv.totals?.cost ?? 0);
    const sell = Number(inv.totals?.grandTotal ?? inv.totals?.sell ?? 0);
    const profit = Number(inv.totals?.netProfit ?? inv.totals?.profit ?? 0);
    cur.sell += sell; cur.cost += cost; cur.profit += profit;
    byClient.set(key, cur);
  }
  const top = [...byClient.values()].sort((a,b)=>b.profit-a.profit).slice(0,10);
  invTopClientsTbody.innerHTML = top.map(r=>{
    const m = r.sell>0 ? (r.profit/r.sell) : 0;
    return `<tr>
      <td>${safe(r.name)}</td>
      <td style="text-align:right">${money(r.sell)}</td>
      <td style="text-align:right">${money(r.cost)}</td>
      <td style="text-align:right"><b>${money(r.profit)}</b></td>
      <td style="text-align:right">${(m*100).toFixed(1)}%</td>
    </tr>`;
  }).join('') || '<tr><td class="muted" colspan="5">Aucune donnée.</td></tr>';

  // ---- Par mois (12 derniers) ----
  const byMonth = new Map(); // YYYY-MM
  for(const inv of list){
    const dt = inv.date instanceof Date ? inv.date : (inv.date?.toDate ? inv.date.toDate() : new Date(inv.date));
    const ym = dt.getFullYear()+"-"+String(dt.getMonth()+1).padStart(2,'0');
    const cur = byMonth.get(ym) || {ym, sell:0, cost:0, profit:0};
    cur.cost += Number(inv.totals?.cost||0);
    cur.sell += Number(inv.totals?.sell||0);
    cur.profit += Number(inv.totals?.profit||0);
    byMonth.set(ym, cur);
  }
  const months = [...byMonth.values()].sort((a,b)=> String(b.ym).localeCompare(String(a.ym))).slice(0,12);
  invMonthlyTbody.innerHTML = months.map(r=>{
    const m = r.sell>0 ? (r.profit/r.sell) : 0;
    return `<tr>
      <td>${safe(r.ym)}</td>
      <td style="text-align:right">${money(r.sell)}</td>
      <td style="text-align:right">${money(r.cost)}</td>
      <td style="text-align:right"><b>${money(r.profit)}</b></td>
      <td style="text-align:right">${(m*100).toFixed(1)}%</td>
    </tr>`;
  }).join('') || '<tr><td class="muted" colspan="5">Aucune donnée.</td></tr>';

  // ---- Mini graphique texte ----
  const maxP = Math.max(...months.map(m=>m.profit), 0);
  if(maxP<=0){
    invMonthlyChartEl.textContent = "Pas assez de données pour un graphique (bénéfice ≤ 0).";

  // ---- Par méthode de paiement ----
  if(invPayTbody){
    const byPay = new Map();
    for(const inv of list){
      const key = inv.paymentMethod || "unknown";
      const cur = byPay.get(key) || {method:key, sell:0, cost:0, profit:0};
      const cost = Number(inv.totals?.partsCost ?? inv.totals?.cost ?? 0);
      const sell = Number(inv.totals?.grandTotal ?? inv.totals?.sell ?? 0);
      const profit = Number(inv.totals?.netProfit ?? inv.totals?.profit ?? 0);
      cur.sell += sell; cur.cost += cost; cur.profit += profit;
      byPay.set(key, cur);
    }
    const rows = [...byPay.values()].sort((a,b)=>b.profit-a.profit);
    invPayTbody.innerHTML = rows.map(r=>{
      const margin = r.sell>0 ? (r.profit/r.sell*100) : 0;

  // ---- Par réparation (Top 10) ----
  if(invTopRepairsTbody){
    const byWo = new Map();
    for(const inv of list){
      const woId = inv.workorderId || "";
      if(!woId) continue;
      const cur = byWo.get(woId) || { woId, label: inv.workorderLabel || woId, sell:0, cost:0, cardFee:0, net:0 };
      cur.sell += Number(inv.totals?.grandTotal ?? inv.totals?.sell ?? 0);
      cur.cost += Number(inv.totals?.partsCost ?? inv.totals?.cost ?? 0);
      cur.cardFee += Number(inv.totals?.cardFee ?? 0);
      cur.net += Number(inv.totals?.netProfit ?? inv.totals?.profit ?? 0);
      byWo.set(woId, cur);
    }
    const topWo = [...byWo.values()].sort((a,b)=>b.net-a.net).slice(0,10);
    invTopRepairsTbody.innerHTML = topWo.map(r=>`
      <tr>
        <td>${safe(r.label||r.woId)}</td>
        <td style="text-align:right">${money(r.sell)}</td>
        <td style="text-align:right">${money(r.cost)}</td>
        <td style="text-align:right">${money(r.cardFee)}</td>
        <td style="text-align:right"><b>${money(r.net)}</b></td>
      </tr>
    `).join('') || '<tr><td class="muted" colspan="5">Aucune donnée.</td></tr>';
  }
      return `
        <tr>
          <td>${safe(invPaymentLabel(r.method))}</td>
          <td style="text-align:right">${money(r.sell)}</td>
          <td style="text-align:right">${money(r.cost)}</td>
          <td style="text-align:right"><b>${money(r.profit)}</b></td>
          <td style="text-align:right">${margin.toFixed(1)}%</td>
        </tr>
      `;
    }).join('') || '<tr><td class="muted" colspan="5">Aucune donnée.</td></tr>';
  }
  }else{
    const bars = months.slice().reverse().map(r=>{
      const w = Math.round((r.profit/maxP)*16);
      const bar = "▮".repeat(Math.max(1,w));
      return `${r.ym}: ${bar} ${money(r.profit)}`;
    }).join("<br>");
    invMonthlyChartEl.innerHTML = `<div style="font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size:12px; line-height:1.5">${bars}</div>`;
  }
}

}

function isoDate(d){
  // YYYY-MM-DD in local time
  const tzOff = d.getTimezoneOffset()*60000;
  return new Date(d.getTime()-tzOff).toISOString().slice(0,10);
}
function firstDayOfMonth(d){
  return new Date(d.getFullYear(), d.getMonth(), 1);
}
function lastDayOfMonth(d){
  return new Date(d.getFullYear(), d.getMonth()+1, 0);
}
function setRevenuePreset(preset){
  const now = new Date();
  if(preset === "today"){
    const t = isoDate(now);
    revFromEl.value = t;
    revToEl.value = t;
    revFromEl.disabled = true;
    revToEl.disabled = true;
  }else if(preset === "month"){
    revFromEl.value = isoDate(firstDayOfMonth(now));
    revToEl.value = isoDate(lastDayOfMonth(now));
    revFromEl.disabled = true;
    revToEl.disabled = true;
  }else{
    // custom
    if(!revFromEl.value) revFromEl.value = isoDate(firstDayOfMonth(now));
    if(!revToEl.value) revToEl.value = isoDate(now);
    revFromEl.disabled = false;
    revToEl.disabled = false;
  }
}
function revenueRange(){
  const from = (revFromEl && revFromEl.value) ? revFromEl.value : "0000-01-01";
  const to = (revToEl && revToEl.value) ? revToEl.value : "9999-12-31";
  return {from, to};
}
function workorderDateKey(w){
  const s = String(w.invoiceDate || w.createdAt || w.updatedAt || "");
  return s.slice(0,10);
}

function getInvoiceTotals(inv){
  // Supporte plusieurs formats (anciens / nouveaux)
  const t = inv?.totals || inv?.total || {};
  // 1) si totals existe
  let sell = Number(t.grandTotal ?? t.totalClient ?? t.sell ?? t.total ?? 0);
  let cost = Number(t.partsCost ?? t.cost ?? 0);
  let profit = Number(t.netProfit ?? t.profit ?? 0);

  // 2) si totals absent ou à 0 mais items existent -> recalcul
  const items = Array.isArray(inv?.items) ? inv.items : [];
  if((!sell && !cost && !profit) && items.length){
    let s=0, c=0;
    for(const it of items){
      const qty = Number(it.qty ?? 1);
      const price = Number(it.price ?? 0);
      const icost = Number(it.cost ?? 0);
      s += qty * price;
      c += qty * icost;
    }
    sell = s;
    cost = c;
    profit = sell - cost;
  }

  // 3) fallback: si un seul champ existe
  if(!profit && sell) profit = sell - cost;

  return { sell, cost, profit };
}

function invoiceDateAsDate(inv){
  const d = inv?.date instanceof Date ? inv.date : (inv?.date?.toDate ? inv.date.toDate() : (inv?.date ? new Date(inv.date) : new Date(0)));
  return isNaN(d.getTime()) ? new Date(0) : d;
}
function invoiceDateKey(inv){
  return isoDate(invoiceDateAsDate(inv));
}
function filterRevenueWorkorders(){
  const {from, to} = revenueRange();
  return workorders
    .filter(w=>Number(w.total||0) > 0)
    .filter(w=>!!w.invoiceNo) // seulement factures
    .filter(w=>{
      const k = workorderDateKey(w);
      return k && k >= from && k <= to;
    })
    .sort((a,b)=> (workorderDateKey(b).localeCompare(workorderDateKey(a))) || String(b.invoiceNo||"").localeCompare(String(a.invoiceNo||"")));
}

function filterRevenueInvoices(){
  const from = revFromEl?.value || null;
  const to = revToEl?.value || null;
  const pay = String(revPayFilterEl?.value || "").trim().toLowerCase();

  return (invoices||[]).filter(inv=>{
    const k = invoiceDateKey(inv);
    if(from && k < from) return false;
    if(to && k > to) return false;
    if(pay && String(inv.paymentMethod||"").toLowerCase() !== pay) return false;
    return true;
  });
}

function renderRevenue(){
  if(!$("viewRevenue")) return;
  if(currentRole !== "admin"){
    if(revTotalEl) revTotalEl.textContent = money(0);
    if(revCountEl) revCountEl.textContent = "0";
    if(revAvgEl) revAvgEl.textContent = money(0);
    if(revPartsCostEl) revPartsCostEl.textContent = money(0);
    if(revProfitEl) revProfitEl.textContent = money(0);
    if(revTbody) revTbody.innerHTML = '<tr><td colspan="9" class="muted">Accès réservé à l\'administrateur.</td></tr>';
    if(revByPayTbody) revByPayTbody.innerHTML = '<tr><td class="muted" colspan="4">—</td></tr>';
    if(revByDateTbody) revByDateTbody.innerHTML = '<tr><td class="muted" colspan="4">—</td></tr>';
    if(revCards) revCards.innerHTML = '<div class="muted">Accès réservé à l\'administrateur.</div>';
    if(revByPayCards) revByPayCards.innerHTML = '<div class="muted">—</div>';
    if(revByDateCards) revByDateCards.innerHTML = '<div class="muted">—</div>';
    return;
  }

  const rows = filterRevenueInvoices().sort((a,b)=> invoiceDateAsDate(b) - invoiceDateAsDate(a));

  let total=0, parts=0, profit=0;
  for(const inv of rows){
    const t = getInvoiceTotals(inv);
    total += t.sell;
    parts += t.cost;
    profit += t.profit;
  }
  const count = rows.length;
  const avg = count ? total/count : 0;

  if(revTotalEl) revTotalEl.textContent = money(total);
  if(revCountEl) revCountEl.textContent = String(count);
  if(revAvgEl) revAvgEl.textContent = money(avg);
  if(revPartsCostEl) revPartsCostEl.textContent = money(parts);
  if(revProfitEl) revProfitEl.textContent = money(profit);

  if(!revTbody) return;

  while(revTbody.firstChild) revTbody.removeChild(revTbody.firstChild);

  if(count === 0){
    const tr=document.createElement('tr');
    const td=document.createElement('td');
    td.colSpan=9;
    td.className='muted';
    td.textContent='Aucune facture pour cette période.';
    tr.appendChild(td);
    revTbody.appendChild(tr);
  }else{
    for(const inv of rows){
      const ref = String(inv.ref || '—');
      const date = invoiceDateKey(inv) || '—';
      const client = String(inv.customerName || '—');
      const repair = String(inv.workorderLabel || '—');
      const method = invPaymentLabel(inv.paymentMethod);
      const t = getInvoiceTotals(inv);
      const tot = money(t.sell);
      const cst = money(t.cost);
      const prf = money(t.profit);

      const tr=document.createElement('tr');
      const cells=[ref, date, client, repair, method, tot, cst, prf];
      cells.forEach((val, idx)=>{
        const td=document.createElement('td');
        if(idx>=5) td.style.textAlign='right';
        td.textContent=val;
        tr.appendChild(td);
      });

      const tdBtn=document.createElement('td');
      tdBtn.className='no-print';
      tdBtn.style.textAlign='right';
      const btn=document.createElement('button');
      btn.className='btn btn-ghost';
      btn.textContent='Voir';
      btn.addEventListener('click', ()=>{
        try{
          go('invoices');
          const b=document.querySelector(`[data-edit-inv="${inv.id}"]`);
          if(b) b.click();
        }catch(e){}
      });
      tdBtn.appendChild(btn);
      tr.appendChild(tdBtn);

      revTbody.appendChild(tr);
    }
  }

  // ---- Par méthode de paiement ----
  if(revByPayTbody){
    const map=new Map();
    for(const inv of rows){
      const k=String(inv.paymentMethod||'').toLowerCase()||'unknown';
      const cur=map.get(k)||{k,total:0,cost:0,profit:0};
      const t = getInvoiceTotals(inv);
      cur.total += t.sell;
      cur.cost += t.cost;
      cur.profit += t.profit;
      map.set(k,cur);
    }
    const list=[...map.values()].sort((a,b)=>b.profit-a.profit);
    revByPayTbody.innerHTML = list.map(r=>`
      <tr>
        <td>${safe(invPaymentLabel(r.k))}</td>
        <td style="text-align:right">${money(r.total)}</td>
        <td style="text-align:right">${money(r.cost)}</td>
        <td style="text-align:right"><b>${money(r.profit)}</b></td>
      </tr>
    `).join('') || '<tr><td class="muted" colspan="4">Aucune donnée.</td></tr>';

    if(revByPayCards){
      revByPayCards.innerHTML = list.map(r=>`
        <div class="stat-card">
          <div class="stat-k">${safe(invPaymentLabel(r.k))}</div>
          <div class="stat-v">${money(r.total)}
            <span class="stat-sub">Pièces: ${money(r.cost)} · Bénéf.: ${money(r.profit)}</span>
          </div>
        </div>
      `).join('') || '<div class="muted">Aucune donnée.</div>';
    }
  }

  // ---- Par date (par jour) ----
  if(revByDateTbody){
    const map=new Map();
    for(const inv of rows){
      const k=invoiceDateKey(inv);
      const cur=map.get(k)||{k,total:0,cost:0,profit:0};
      const t = getInvoiceTotals(inv);
      cur.total += t.sell;
      cur.cost += t.cost;
      cur.profit += t.profit;
      map.set(k,cur);
    }
    const list=[...map.values()].sort((a,b)=>String(b.k).localeCompare(String(a.k))).slice(0,60);
    revByDateTbody.innerHTML = list.map(r=>`
      <tr>
        <td>${safe(r.k)}</td>
        <td style="text-align:right">${money(r.total)}</td>
        <td style="text-align:right">${money(r.cost)}</td>
        <td style="text-align:right"><b>${money(r.profit)}</b></td>
      </tr>
    `).join('') || '<tr><td class="muted" colspan="4">Aucune donnée.</td></tr>';

    if(revByDateCards){
      revByDateCards.innerHTML = list.map(r=>`
        <div class="stat-card">
          <div class="stat-k">${safe(r.k)}</div>
          <div class="stat-v">${money(r.total)}
            <span class="stat-sub">Pièces: ${money(r.cost)} · Bénéf.: ${money(r.profit)}</span>
          </div>
        </div>
      `).join('') || '<div class="muted">Aucune donnée.</div>';
    }
  }

  // ---- Mobile cards (liste des factures) ----
  if(revCards){
    if(count === 0){
      revCards.innerHTML = '<div class="muted">Aucune facture pour cette période.</div>';
    }else{
      revCards.innerHTML = rows.map(inv=>{
        const ref = String(inv.ref || '—');
        const date = invoiceDateKey(inv) || '—';
        const client = String(inv.customerName || '—');
        const repair = String(inv.workorderLabel || '—');
        const method = invPaymentLabel(inv.paymentMethod);
        const t = getInvoiceTotals(inv);
        const tot = money(t.sell);
        const cst = money(t.cost);
        const prf = money(t.profit);
        return `
          <div class="rev-card">
            <div class="rev-card-top">
              <div>
                <div class="rev-ref"># ${safe(ref)}</div>
                <div class="rev-date">${safe(date)}</div>
              </div>
              <span class="rev-pill">${safe(method)}</span>
            </div>
            <div class="rev-client">${safe(client)}</div>
            <div class="rev-repair">${safe(repair)}</div>
            <div class="rev-bottom">
              <div class="rev-total">${tot}</div>
              <div class="muted" style="font-size:12px">Pièces: ${cst} · Bénéf.: ${prf}</div>
            </div>
            <div style="margin-top:10px; display:flex; justify-content:flex-end">
              <button class="btn btn-ghost" onclick="(function(){ try{ go('invoices'); const b=document.querySelector('[data-edit-inv=\\"${inv.id}\\"]'); if(b) b.click(); }catch(e){} })()">Voir</button>
            </div>
          </div>
        `;
      }).join('');
    }
  }
}

function exportRevenueCSV(){
  try{
    if(currentRole !== "admin" && currentRole !== "superadmin") return;
    const rows = filterRevenueInvoices().sort((a,b)=> invoiceDateAsDate(a) - invoiceDateAsDate(b));
    const lines = [];
    lines.push(["ref","date","client","repair","payment","total","partsCost","profit"].join(","));
    for(const inv of rows){
      const ref = String(inv.ref || "");
      const date = invoiceDateKey(inv) || "";
      const client = String(inv.customerName || "");
      const repair = String(inv.workorderLabel || "");
      const payment = invPaymentLabel(inv.paymentMethod || "");
      const t = getInvoiceTotals(inv);
      const row = [ref,date,client,repair,payment,
        (t.sell||0).toFixed(2),
        (t.cost||0).toFixed(2),
        (t.profit||0).toFixed(2)
      ].map(v=>{
        const s=String(v).replace(/"/g,'""');
        return `"${s}"`;
      }).join(",");
      lines.push(row);
    }
    const csv = lines.join("\n");
    const blob = new Blob([csv], {type:"text/csv;charset=utf-8"});
    const name = "revenus_"+(revFromEl?.value||"")+"_to_"+(revToEl?.value||"")+".csv";
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = name;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(()=>URL.revokeObjectURL(url), 2000);
    showToast("CSV exporté ✅");
  }catch(err){
    console.error(err);
    alert("Impossible d’exporter le CSV: " + (err?.message||err));
  }
}
/* ============
   Fiscal (info fiscaux)
=========== */
function _startOfMonth(d){
  return new Date(d.getFullYear(), d.getMonth(), 1);
}
function _endOfDay(d){
  return new Date(d.getFullYear(), d.getMonth(), d.getDate(), 23,59,59,999);
}

/* ============
   Dépenses pièces (achats)
=========== */
function setPartsExpPreset(preset){
  if(!pexFromEl || !pexToEl) return;
  const now = new Date();
  let from, to;
  if(preset === "custom") return;
  if(preset === "month"){
    from = _startOfMonth(now);
    to = _endOfDay(now);
  }else if(preset === "3m"){
    from = _startOfMonth(new Date(now.getFullYear(), now.getMonth()-2, 1));
    to = _endOfDay(now);
  }else if(preset === "6m"){
    from = _startOfMonth(new Date(now.getFullYear(), now.getMonth()-5, 1));
    to = _endOfDay(now);
  }else if(preset === "12m"){
    from = _startOfMonth(new Date(now.getFullYear(), now.getMonth()-11, 1));
    to = _endOfDay(now);
  }else{
    from = _startOfMonth(now);
    to = _endOfDay(now);
  }
  pexFromEl.value = isoDate(from);
  pexToEl.value = isoDate(to);
}

function _partsExpDateAsDate(x){
  if(!x) return new Date(0);
  if(x.date) return new Date(String(x.date)+"T12:00:00");
  if(x.createdAt && x.createdAt.toDate) return x.createdAt.toDate();
  return new Date(0);
}

function filterPartsExpenses(){
  const list = Array.isArray(partsExpenses) ? partsExpenses : [];
  const preset = (pexPresetEl && pexPresetEl.value) ? String(pexPresetEl.value) : "month";
  let from = pexFromEl && pexFromEl.value ? new Date(pexFromEl.value+"T00:00:00") : null;
  let to = pexToEl && pexToEl.value ? new Date(pexToEl.value+"T23:59:59") : null;
  if(!from || !to){
    setPartsExpPreset(preset);
    from = pexFromEl && pexFromEl.value ? new Date(pexFromEl.value+"T00:00:00") : null;
    to = pexToEl && pexToEl.value ? new Date(pexToEl.value+"T23:59:59") : null;
  }
  const pay = pexPayFilterEl ? String(pexPayFilterEl.value||"").toLowerCase() : "";
  return list.filter(x=>{
    const d = _partsExpDateAsDate(x);
    if(from && d < from) return false;
    if(to && d > to) return false;
    if(pay) return String(x.paymentMethod||"").toLowerCase() === pay;
    return true;
  });
}

function renderPartsExpenses(){
  if(!$("viewPartsExpenses")) return;
  if(currentRole !== "admin"){
    if(pexTbody) pexTbody.innerHTML = '<tr><td class="muted" colspan="8">Accès réservé à l\'administrateur.</td></tr>';
    if(pexSubtotalEl) pexSubtotalEl.textContent = money(0);
    if(pexTaxEl) pexTaxEl.textContent = money(0);
    if(pexTotalEl) pexTotalEl.textContent = money(0);
    if(pexCountEl) pexCountEl.textContent = "0";
    return;
  }

  const rows = filterPartsExpenses().sort((a,b)=> _partsExpDateAsDate(b) - _partsExpDateAsDate(a));

  let subtotal=0, tax=0, total=0;
  for(const x of rows){
    subtotal += Number(x.subtotal||0);
    tax += Number(x.taxTotal||0);
    total += Number(x.total||0);
  }
  if(pexSubtotalEl) pexSubtotalEl.textContent = money(subtotal);
  if(pexTaxEl) pexTaxEl.textContent = money(tax);
  if(pexTotalEl) pexTotalEl.textContent = money(total);
  if(pexCountEl) pexCountEl.textContent = String(rows.length);

  if(!pexTbody) return;
  if(rows.length===0){
    pexTbody.innerHTML = '<tr><td class="muted" colspan="8">Aucune dépense.</td></tr>';
    return;
  }

  pexTbody.innerHTML = rows.map(x=>{
    const d = _partsExpDateAsDate(x);
    const tps = (x.tpsAmount!=null)?Number(x.tpsAmount||0):splitTaxTotal(Number(x.taxTotal||0)).tps;
    const tvq = (x.tvqAmount!=null)?Number(x.tvqAmount||0):splitTaxTotal(Number(x.taxTotal||0)).tvq;
    const taxes = (x.tpsAmount!=null || x.tvqAmount!=null)?(tps+tvq):Number(x.taxTotal||0);
    return `<tr>
      <td>${isoDate(d)}</td>
      <td>${safe(x.supplier||"")}</td>
      <td>${safe(x.description||"")}</td>
      <td>${safe(invPaymentLabel(x.paymentMethod))}</td>
      <td style="text-align:right">${money(x.subtotal||0)}</td>
      <td style="text-align:right">${money(tps)}</td>
      <td style="text-align:right">${money(tvq)}</td>
      <td style="text-align:right">${money(taxes)}</td>
      <td style="text-align:right"><b>${money(x.total||0)}</b></td
      <td class="no-print" style="white-space:nowrap">
        <button class="btn btn-ghost btn-small" onclick="window.__editPartsExpense('${x.id}')">Modifier</button>
        <button class="btn btn-ghost btn-small" onclick="window.__deletePartsExpense('${x.id}')">Supprimer</button>
      </td>
    </tr>`;
  }).join("");
}

function openPartsExpenseModal(existing){
  if(currentRole !== "admin" && currentRole !== "superadmin") return;
  const x = existing || {};
  const today = isoDate(new Date());
  const html = `
    <form class="form" id="formPartsExpense">
      <label>Date</label>
      <input class="input" name="date" type="date" required value="${safe(x.date || today)}" />

      <label>Fournisseur</label>
      <input class="input" name="supplier" list="supplierOptionsList" placeholder="Ex: NAPA" value="${safe(x.supplier||"")}" />
      <datalist id="supplierOptionsList">${(Array.isArray(suppliers)?suppliers:[]).map(s=>`<option value="${safe(s.name||"")}"></option>`).join("")}</datalist>

      <label>Description</label>
      <input class="input" name="description" placeholder="Ex: Plaquettes + disques" value="${safe(x.description||"")}" />

      <div class="row" style="gap:10px; flex-wrap:wrap">
        <div style="flex:1; min-width:160px">
          <label>Montant HT</label>
          <input class="input" name="subtotal" type="number" step="0.01" required value="${Number(x.subtotal||0)}" />
        </div>
        <div style="flex:1; min-width:160px">
          <label>TPS (5%)</label>
          <input class="input" name="tpsAmount" type="number" step="0.01" required value="${Number(x.tpsAmount!=null?x.tpsAmount:splitTaxTotal(Number(x.taxTotal||0)).tps)}" />
        </div>
        <div style="flex:1; min-width:160px">
          <label>TVQ (9.975%)</label>
          <input class="input" name="tvqAmount" type="number" step="0.01" required value="${Number(x.tvqAmount!=null?x.tvqAmount:splitTaxTotal(Number(x.taxTotal||0)).tvq)}" />
        </div>
        <div style="flex:1; min-width:160px">
          <label>Total TTC</label>
          <input class="input" name="total" type="number" step="0.01" required value="${Number(x.total||0)}" />
        </div>
      </div>

      <label>Paiement</label>
      <select class="input" name="paymentMethod">
        ${["cash","card","etransfer","bank","cheque","other"].map(v=>{
          const sel = String(x.paymentMethod||"")===v ? "selected" : "";
          return `<option value="${v}" ${sel}>${invPaymentLabel(v)}</option>`;
        }).join("")}
      </select>

      <div class="row" style="margin-top:12px; gap:10px">
        <button class="btn btn-primary" type="submit">Enregistrer</button>
        <button class="btn btn-ghost" type="button" data-modal-close>Annuler</button>
      </div>
    </form>
    <small class="muted">Astuce: si tu veux, je peux ajouter TPS et TVQ séparés (2 champs) pour des rapports encore plus précis.</small>
  `;

  showModal(existing ? "Modifier dépense" : "Nouvelle dépense", html);

  const form = modalBody.querySelector("#formPartsExpense");
  if(!form) return;
  form.addEventListener("submit", async (e)=>{
    e.preventDefault();
    const fd = new FormData(form);
    const data = {
      date: String(fd.get("date")||"").trim(),
      supplier: String(fd.get("supplier")||"").trim(),
      description: String(fd.get("description")||"").trim(),
      subtotal: Number(fd.get("subtotal")||0),
      tpsAmount: Number(fd.get("tpsAmount")||0),
      tvqAmount: Number(fd.get("tvqAmount")||0),
      taxTotal: (Number(fd.get("tpsAmount")||0) + Number(fd.get("tvqAmount")||0)),
      total: Number(fd.get("total")||0),
      paymentMethod: String(fd.get("paymentMethod")||"other"),
      updatedAt: serverTimestamp(),
    };
    if(!data.total) data.total = data.subtotal + data.taxTotal;
    try{
      if(existing && existing.id){
        await updateDoc(garageDoc("expenses_parts", existing.id), data);
      }else{
        data.createdAt = serverTimestamp();
        await addDoc(colPartsExpenses(), data);
      }
      closeModal();
      showToast("Dépense enregistrée ✅");
    }catch(err){
      console.error(err);
      showToast("Erreur dépense: "+(err.message||err), true);
    }
  });
}

window.__editPartsExpense = (id)=>{
  const x = (Array.isArray(partsExpenses)?partsExpenses:[]).find(r=>r.id===id);
  if(x) openPartsExpenseModal(x);
};

window.__deletePartsExpense = async (id)=>{
  if(currentRole !== "admin" && currentRole !== "superadmin") return;
  if(!confirm("Supprimer cette dépense ?")) return;
  try{
    await deleteDoc(garageDoc("expenses_parts", id));
    showToast("Supprimé ✅");
  }catch(err){
    console.error(err);
    showToast("Erreur suppression: "+(err.message||err), true);
  }
};

function exportPartsExpensesCSV(){
  try{
    if(currentRole !== "admin" && currentRole !== "superadmin") return;
    const rows = filterPartsExpenses().sort((a,b)=> _partsExpDateAsDate(a) - _partsExpDateAsDate(b));
    const lines = [];
    lines.push(["Date","Fournisseur","Description","Paiement","HT","Taxes","TTC"].join(","));
    for(const x of rows){
      const dt = isoDate(_partsExpDateAsDate(x));
      const supplier = String(x.supplier||"").replaceAll('"','""');
      const desc = String(x.description||"").replaceAll('"','""');
      const pm = String(invPaymentLabel(x.paymentMethod)).replaceAll('"','""');
      const tps = (x.tpsAmount!=null)?Number(x.tpsAmount||0):splitTaxTotal(Number(x.taxTotal||0)).tps;
      const tvq = (x.tvqAmount!=null)?Number(x.tvqAmount||0):splitTaxTotal(Number(x.taxTotal||0)).tvq;
      lines.push([dt, `"${supplier}"`, `"${desc}"`, `"${pm}"`, Number(x.subtotal||0), tps, tvq, Number(x.taxTotal||0), Number(x.total||0)].join(","));
    }
    downloadText("depenses_pieces.csv", lines.join("\n"));
    showToast("CSV exporté ✅");
  }catch(err){
    console.error(err);
    showToast("Erreur export CSV dépenses: "+(err.message||err), true);
  }
}

function setFiscalPreset(preset){
  if(!fiscFromEl || !fiscToEl) return;
  const now = new Date();
  let from, to;
  if(preset === "custom"){
    return;
  }
  if(preset === "month"){
    from = _startOfMonth(now);
    to = _endOfDay(now);
  }else if(preset === "3m"){
    from = _startOfMonth(new Date(now.getFullYear(), now.getMonth()-2, 1));
    to = _endOfDay(now);
  }else if(preset === "6m"){
    from = _startOfMonth(new Date(now.getFullYear(), now.getMonth()-5, 1));
    to = _endOfDay(now);
  }else if(preset === "12m"){
    from = _startOfMonth(new Date(now.getFullYear(), now.getMonth()-11, 1));
    to = _endOfDay(now);
  }else{
    from = _startOfMonth(now);
    to = _endOfDay(now);
  }
  fiscFromEl.value = isoDate(from);
  fiscToEl.value = isoDate(to);
}

function _getInvoiceTaxCollected(inv){
  const t = inv?.totals || inv?.total || {};
  return Number(t.taxTotal ?? t.tax ?? t.taxes ?? 0);
}

function _getInvoiceTaxSplit(inv){
  const t = inv?.totals || inv?.total || {};
  const hasSplit = (t.tpsAmount != null) || (t.tvqAmount != null);
  if(hasSplit){
    const tps = Number(t.tpsAmount||0);
    const tvq = Number(t.tvqAmount||0);
    const total = Number(t.taxTotal != null ? t.taxTotal : (tps + tvq));
    return { tps, tvq, total };
  }
  const total = Number(t.taxTotal ?? t.tax ?? t.taxes ?? 0);
  const sp = splitTaxTotal(total);
  return { tps: sp.tps, tvq: sp.tvq, total };
}

function filterFiscalInvoices(){
  const list = Array.isArray(invoices) ? invoices : [];
  const preset = (fiscPresetEl && fiscPresetEl.value) ? String(fiscPresetEl.value) : "month";
  let from = fiscFromEl && fiscFromEl.value ? new Date(fiscFromEl.value+"T00:00:00") : null;
  let to = fiscToEl && fiscToEl.value ? new Date(fiscToEl.value+"T23:59:59") : null;

  if(!from || !to){
    setFiscalPreset(preset);
    from = fiscFromEl && fiscFromEl.value ? new Date(fiscFromEl.value+"T00:00:00") : null;
    to = fiscToEl && fiscToEl.value ? new Date(fiscToEl.value+"T23:59:59") : null;
  }

  const pay = fiscPayFilterEl ? String(fiscPayFilterEl.value||"").toLowerCase() : "";

  return list.filter(inv=>{
    const dt = invoiceDateAsDate(inv);
    if(from && dt < from) return false;
    if(to && dt > to) return false;
    if(pay){
      return String(inv.paymentMethod||"").toLowerCase() === pay;
    }
    return true;
  });
}

function renderFiscal(){
  if(!$("viewFiscal")) return;
  if(currentRole !== "admin"){
    if(fiscTbody) fiscTbody.innerHTML = '<tr><td colspan="8" class="muted">Accès réservé à l\'administrateur.</td></tr>';
    if(fiscRevenueEl) fiscRevenueEl.textContent = money(0);
    if(fiscExpensesEl) fiscExpensesEl.textContent = money(0);
    if(fiscProfitEl) fiscProfitEl.textContent = money(0);
    if(fiscTaxCollectedEl) fiscTaxCollectedEl.textContent = money(0);
    if(fiscTPSCollectedEl) fiscTPSCollectedEl.textContent = money(0);
    if(fiscTVQCollectedEl) fiscTVQCollectedEl.textContent = money(0);
    if(fiscTaxPaidEl) fiscTaxPaidEl.textContent = money(0);
    if(fiscTPSPaidEl) fiscTPSPaidEl.textContent = money(0);
    if(fiscTVQPaidEl) fiscTVQPaidEl.textContent = money(0);
    if(fiscTaxPayableEl) fiscTaxPayableEl.textContent = money(0);
    if(fiscNetAfterTaxEl) fiscNetAfterTaxEl.textContent = money(0);
    if(fiscCountEl) fiscCountEl.textContent = "0";
    return;
  }

  const rows = filterFiscalInvoices().sort((a,b)=> invoiceDateAsDate(b) - invoiceDateAsDate(a));

  // Revenus (factures pièces)
  let revenue = 0;
  let taxCollected = 0;
  let taxCollectedTPS = 0;
  let taxCollectedTVQ = 0;
  for(const inv of rows){
    const t = getInvoiceTotals(inv);
    revenue += Number(t.sell||0);
    const sp = _getInvoiceTaxSplit(inv);
    taxCollected += sp.total;
    taxCollectedTPS += sp.tps;
    taxCollectedTVQ += sp.tvq;
  }

  // Dépenses pièces (achats fournisseurs) — exactes
  const purchasePay = fiscPurchasePayFilterEl ? String(fiscPurchasePayFilterEl.value||"").toLowerCase() : "";
  const from = fiscFromEl && fiscFromEl.value ? new Date(fiscFromEl.value+"T00:00:00") : null;
  const to = fiscToEl && fiscToEl.value ? new Date(fiscToEl.value+"T23:59:59") : null;
  const purchases = (Array.isArray(partsExpenses) ? partsExpenses : []).filter(x=>{
    const d = x?.date ? new Date(String(x.date)+"T12:00:00") : null;
    if(d && from && d < from) return false;
    if(d && to && d > to) return false;
    if(purchasePay) return String(x.paymentMethod||"").toLowerCase() === purchasePay;
    return true;
  });

  let purchasesSubtotal = 0;
  let purchasesTax = 0;
  let purchasesTaxTPS = 0;
  let purchasesTaxTVQ = 0;
  let purchasesTotal = 0;
  for(const p of purchases){
    purchasesSubtotal += Number(p.subtotal||0);
    const pTax = (p.tpsAmount!=null || p.tvqAmount!=null)
      ? (Number(p.tpsAmount||0) + Number(p.tvqAmount||0))
      : Number(p.taxTotal||0);
    purchasesTax += pTax;
    if(p.tpsAmount!=null || p.tvqAmount!=null){
      purchasesTaxTPS += Number(p.tpsAmount||0);
      purchasesTaxTVQ += Number(p.tvqAmount||0);
    } else {
      const sp = splitTaxTotal(pTax);
      purchasesTaxTPS += sp.tps;
      purchasesTaxTVQ += sp.tvq;
    }
    purchasesTotal += Number(p.total||0);
  }

  // Calculs fiscaux
  const revenueHT = revenue - taxCollected;
  const profit = revenueHT - purchasesSubtotal;
  const expenses = purchasesTotal;
  const taxPaid = purchasesTax;
  const taxPayable = taxCollected - taxPaid;
  const netAfterTax = profit - taxPayable;

  if(fiscRevenueEl) fiscRevenueEl.textContent = money(revenue);
  if(fiscExpensesEl) fiscExpensesEl.textContent = money(expenses);
  if(fiscProfitEl) fiscProfitEl.textContent = money(profit);
  if(fiscTaxCollectedEl) fiscTaxCollectedEl.textContent = money(taxCollected);
  if(fiscTPSCollectedEl) fiscTPSCollectedEl.textContent = money(taxCollectedTPS);
  if(fiscTVQCollectedEl) fiscTVQCollectedEl.textContent = money(taxCollectedTVQ);
  if(fiscTaxPaidEl) fiscTaxPaidEl.textContent = money(taxPaid);
  if(fiscTPSPaidEl) fiscTPSPaidEl.textContent = money(purchasesTaxTPS);
  if(fiscTVQPaidEl) fiscTVQPaidEl.textContent = money(purchasesTaxTVQ);
  if(fiscTaxPayableEl) fiscTaxPayableEl.textContent = money(taxPayable);
  if(fiscNetAfterTaxEl) fiscNetAfterTaxEl.textContent = money(netAfterTax);
  if(fiscCountEl) fiscCountEl.textContent = String(rows.length);

  if(!fiscTbody) return;
  if(rows.length===0){
    fiscTbody.innerHTML = '<tr><td colspan="8" class="muted">Aucune facture pour ce filtre.</td></tr>';
    return;
  }

  fiscTbody.innerHTML = rows.map(inv=>{
    const dt = invoiceDateAsDate(inv);
    const t = getInvoiceTotals(inv);
    const tax = _getInvoiceTaxCollected(inv);
    return `<tr>
      <td>${isoDate(dt)}</td>
      <td>${safe(inv.ref||"")}</td>
      <td>${safe(inv.customerName||"")}</td>
      <td>${safe(invPaymentLabel(inv.paymentMethod))}</td>
      <td style="text-align:right">${money(tax)}</td>
      <td style="text-align:right">${money(t.sell)}</td>
      <td style="text-align:right">${money(t.cost)}</td>
      <td style="text-align:right"><b>${money(t.profit)}</b></td>
    </tr>`;
  }).join("");
}

function exportFiscalCSV(){
  try{
    if(currentRole !== "admin" && currentRole !== "superadmin") return;
    const rows = filterFiscalInvoices().sort((a,b)=> invoiceDateAsDate(a) - invoiceDateAsDate(b));
    const lines = [];
    lines.push(["Date","Ref","Client","Paiement","SousTotal_HT","TPS","TVQ","Taxes_Total","Total_TTC","Cout_Pieces","Benefice_Net"].join(","));
    for(const inv of rows){
      const dt = isoDate(invoiceDateAsDate(inv));
      const ref = String(inv.ref||"").replaceAll('"','""');
      const client = String(inv.customerName||"").replaceAll('"','""');
      const pm = String(invPaymentLabel(inv.paymentMethod)).replaceAll('"','""');
      const t = getInvoiceTotals(inv);
      const sp = _getInvoiceTaxSplit(inv);
      const subHT = Number(inv.totals?.subTotal ?? 0);
      lines.push([dt, `"${ref}"`, `"${client}"`, `"${pm}"`, subHT, sp.tps, sp.tvq, sp.total, t.sell, t.cost, t.profit].join(","));
    }
    downloadText("infos_fiscaux.csv", lines.join("\n"));
  }catch(e){
    console.error(e);
    showToast("Erreur export CSV fiscaux: "+(e.message||e), true);
  }
}



function exportComptableCSVs(){
  try{
    if(currentRole !== "admin" && currentRole !== "superadmin") return;

    // période / filtres venant de la page fiscaux
    const rows = filterFiscalInvoices().sort((a,b)=> invoiceDateAsDate(a) - invoiceDateAsDate(b));
    const pay = fiscPayFilterEl ? String(fiscPayFilterEl.value||"").toLowerCase() : "";
    const from = (fiscFromEl && fiscFromEl.value) ? new Date(fiscFromEl.value+"T00:00:00") : null;
    const to = (fiscToEl && fiscToEl.value) ? new Date(fiscToEl.value+"T23:59:59") : null;

    // --- VENTES (Factures)
    const ventes = [];
    ventes.push(["Date","Ref","Client","Paiement","SousTotal_HT","TPS","TVQ","Taxes_Total","Total_TTC","Cout_Pieces","Benefice_Net"].join(","));

    let ventesTotalTTC=0, ventesSubHT=0, ventesTPS=0, ventesTVQ=0, ventesTaxes=0, ventesCout=0, ventesProfit=0;

    for(const inv of rows){
      if(pay && String(inv.paymentMethod||"").toLowerCase() !== pay) continue;

      const dt = isoDate(invoiceDateAsDate(inv));
      const ref = String(inv.ref||"").replaceAll('"','""');
      const client = String(inv.customerName||"").replaceAll('"','""');
      const pm = String(invPaymentLabel(inv.paymentMethod)).replaceAll('"','""');

      const t = getInvoiceTotals(inv);
      const sp = _getInvoiceTaxSplit(inv);
      const subHT = Number(inv.totals?.subTotal ?? 0);

      ventes.push([dt, `"${ref}"`, `"${client}"`, `"${pm}"`, subHT, sp.tps, sp.tvq, sp.total, Number(t.sell||0), Number(t.cost||0), Number(t.profit||0)].join(","));

      ventesTotalTTC += Number(t.sell||0);
      ventesSubHT += subHT;
      ventesTPS += sp.tps;
      ventesTVQ += sp.tvq;
      ventesTaxes += sp.total;
      ventesCout += Number(t.cost||0);
      ventesProfit += Number(t.profit||0);
    }

    // --- ACHATS (Dépenses pièces)
    const achats = [];
    achats.push(["Date","Fournisseur","Description","Paiement","SousTotal_HT","TPS","TVQ","Taxes_Total","Total_TTC"].join(","));

    const purchases = (Array.isArray(partsExpenses) ? partsExpenses : []).filter(x=>{
      const d = x?.date ? new Date(String(x.date)+"T12:00:00") : null;
      if(d && from && d < from) return false;
      if(d && to && d > to) return false;
      if(pay) return String(x.paymentMethod||"").toLowerCase() === pay;
      return true;
    }).sort((a,b)=>{
      const da = a?.date ? new Date(String(a.date)+"T12:00:00") : new Date(0);
      const db = b?.date ? new Date(String(b.date)+"T12:00:00") : new Date(0);
      return da - db;
    });

    let achatsTotalTTC=0, achatsSubHT=0, achatsTPS=0, achatsTVQ=0, achatsTaxes=0;

    for(const x of purchases){
      const dt = x?.date ? String(x.date) : "";
      const supplier = String(x.supplier||"").replaceAll('"','""');
      const desc = String(x.description||"").replaceAll('"','""');
      const pm = String(invPaymentLabel(x.paymentMethod)).replaceAll('"','""');

      const sub = Number(x.subtotal||0);
      const tps = (x.tpsAmount!=null)?Number(x.tpsAmount||0):splitTaxTotal(Number(x.taxTotal||0)).tps;
      const tvq = (x.tvqAmount!=null)?Number(x.tvqAmount||0):splitTaxTotal(Number(x.taxTotal||0)).tvq;
      const taxes = Number(x.taxTotal!=null ? x.taxTotal : (tps+tvq));
      const total = Number(x.total || (sub + taxes));

      achats.push([dt, `"${supplier}"`, `"${desc}"`, `"${pm}"`, sub, tps, tvq, taxes, total].join(","));

      achatsTotalTTC += total;
      achatsSubHT += sub;
      achatsTPS += tps;
      achatsTVQ += tvq;
      achatsTaxes += taxes;
    }

    // --- RÉSUMÉ
    const resume = [];
    const labelFrom = from ? isoDate(from) : "";
    const labelTo = to ? isoDate(to) : "";
    resume.push(["Periode_de","Periode_a","Filtre_paiement","Ventes_TTC","Ventes_HT","TPS_collectee","TVQ_collectee","Taxes_collectees","Achats_TTC","Achats_HT","TPS_payee","TVQ_payee","Taxes_payees","Taxes_a_payer_estimees","Benefice_net_estime"].join(","));
    const taxesPayables = (ventesTaxes - achatsTaxes);
    const beneficeNet = (ventesSubHT - achatsSubHT); // hors taxes
    resume.push([labelFrom, labelTo, `"${(pay||"tous")}"`, ventesTotalTTC, ventesSubHT, ventesTPS, ventesTVQ, ventesTaxes, achatsTotalTTC, achatsSubHT, achatsTPS, achatsTVQ, achatsTaxes, taxesPayables, beneficeNet].join(","));

    // Téléchargements
    downloadText(`rapport_comptable_resume_${labelFrom}_au_${labelTo}.csv`, resume.join("\n"));
    downloadText(`rapport_comptable_ventes_${labelFrom}_au_${labelTo}.csv`, ventes.join("\n"));
    downloadText(`rapport_comptable_achats_pieces_${labelFrom}_au_${labelTo}.csv`, achats.join("\n"));

    showToast("Rapport comptable exporté (3 CSV).");
  }catch(e){
    console.error(e);
    showToast("Erreur rapport comptable: "+(e.message||e), true);
  }
}


function exportComptablePDF(){
  try{
    if(currentRole !== "admin" && currentRole !== "superadmin") return;

    // Vérifier que jsPDF est chargé
    const jsPDF = (window.jspdf && window.jspdf.jsPDF) ? window.jspdf.jsPDF : null;
    if(!jsPDF){
      showToast("PDF indisponible: jsPDF non chargé.", true);
      return;
    }

    // période / filtres venant de la page fiscaux
    const rows = filterFiscalInvoices().sort((a,b)=> invoiceDateAsDate(a) - invoiceDateAsDate(b));
    const pay = fiscPayFilterEl ? String(fiscPayFilterEl.value||"").toLowerCase() : "";
    const from = (fiscFromEl && fiscFromEl.value) ? new Date(fiscFromEl.value+"T00:00:00") : null;
    const to = (fiscToEl && fiscToEl.value) ? new Date(fiscToEl.value+"T23:59:59") : null;

    const labelFrom = from ? isoDate(from) : "";
    const labelTo = to ? isoDate(to) : "";
    const payLabel = pay ? invPaymentLabel(pay) : "Tous";

    // --- Totaux ventes
    let ventesTotalTTC=0, ventesSubHT=0, ventesTPS=0, ventesTVQ=0, ventesTaxes=0, ventesCout=0, ventesProfit=0;
    const ventesRows = [];

    for(const inv of rows){
      if(pay && String(inv.paymentMethod||"").toLowerCase() !== pay) continue;
      const dt = isoDate(invoiceDateAsDate(inv));
      const ref = String(inv.ref||"");
      const client = String(inv.customerName||"");
      const pm = invPaymentLabel(inv.paymentMethod);
      const t = getInvoiceTotals(inv);
      const sp = _getInvoiceTaxSplit(inv);
      const subHT = Number(inv.totals?.subTotal ?? 0);

      ventesRows.push([
        dt, ref, client, pm,
        money(subHT), money(sp.tps), money(sp.tvq), money(sp.total),
        money(Number(t.sell||0)), money(Number(t.cost||0)), money(Number(t.profit||0))
      ]);

      ventesTotalTTC += Number(t.sell||0);
      ventesSubHT += subHT;
      ventesTPS += sp.tps;
      ventesTVQ += sp.tvq;
      ventesTaxes += sp.total;
      ventesCout += Number(t.cost||0);
      ventesProfit += Number(t.profit||0);
    }

    // --- Totaux achats pièces
    const purchases = (Array.isArray(partsExpenses) ? partsExpenses : []).filter(x=>{
      const d = x?.date ? new Date(String(x.date)+"T12:00:00") : null;
      if(d && from && d < from) return false;
      if(d && to && d > to) return false;
      if(pay) return String(x.paymentMethod||"").toLowerCase() === pay;
      return true;
    }).sort((a,b)=>{
      const da = a?.date ? new Date(String(a.date)+"T12:00:00") : new Date(0);
      const db = b?.date ? new Date(String(b.date)+"T12:00:00") : new Date(0);
      return da - db;
    });

    let achatsTotalTTC=0, achatsSubHT=0, achatsTPS=0, achatsTVQ=0, achatsTaxes=0;
    const achatsRows = [];

    for(const x of purchases){
      const dt = x?.date ? String(x.date) : "";
      const supplier = String(x.supplier||"");
      const desc = String(x.description||"");
      const pm = invPaymentLabel(x.paymentMethod);

      const sub = Number(x.subtotal||0);
      const tps = (x.tpsAmount!=null)?Number(x.tpsAmount||0):splitTaxTotal(Number(x.taxTotal||0)).tps;
      const tvq = (x.tvqAmount!=null)?Number(x.tvqAmount||0):splitTaxTotal(Number(x.taxTotal||0)).tvq;
      const taxes = Number(x.taxTotal!=null ? x.taxTotal : (tps+tvq));
      const total = Number(x.total || (sub + taxes));

      achatsRows.push([dt, supplier, desc, pm, money(sub), money(tps), money(tvq), money(taxes), money(total)]);

      achatsTotalTTC += total;
      achatsSubHT += sub;
      achatsTPS += tps;
      achatsTVQ += tvq;
      achatsTaxes += taxes;
    }

    // Calculs principaux pour le comptable
    const ventesHT = ventesTotalTTC - ventesTaxes;
    const achatsHT = achatsTotalTTC - achatsTaxes;
    const profitHT = ventesHT - achatsHT;

    const doc = new jsPDF({ orientation: "portrait", unit: "pt", format: "a4" });
    const pageW = doc.internal.pageSize.getWidth();
    const margin = 40;
    let y = 48;

    // Logo (optionnel)
    try{
      const logoEl = document.getElementById("logoImg");
      if(logoEl && logoEl.complete){
        // no-op: logo in DOM may be cross-origin blocked on github pages; fallback to text
      }
    }catch(e){}

    doc.setFontSize(16);
    doc.text("Rapport fiscal / comptable", margin, y);
    y += 22;

    doc.setFontSize(10);
    doc.text(`Période: ${labelFrom || "-"}  →  ${labelTo || "-"}`, margin, y); y += 14;
    doc.text(`Paiement: ${payLabel}`, margin, y); y += 14;
    doc.text(`Généré: ${new Date().toLocaleString()}`, margin, y); y += 18;

    doc.setFontSize(12);
    doc.text("Résumé", margin, y); y += 10;

    // Tableau résumé
    const resumeBody = [
      ["Ventes (TTC)", money(ventesTotalTTC)],
      ["Ventes (HT)", money(ventesHT)],
      ["TPS collectée", money(ventesTPS)],
      ["TVQ collectée", money(ventesTVQ)],
      ["Taxes collectées (TPS+TVQ)", money(ventesTaxes)],
      ["Achats pièces (TTC)", money(achatsTotalTTC)],
      ["Achats pièces (HT)", money(achatsHT)],
      ["TPS payée (achats)", money(achatsTPS)],
      ["TVQ payée (achats)", money(achatsTVQ)],
      ["Taxes payées (achats)", money(achatsTaxes)],
      ["Profit (HT)", money(profitHT)],
    ];

    doc.autoTable({
      startY: y + 6,
      head: [["Indicateur", "Montant"]],
      body: resumeBody,
      theme: "grid",
      styles: { fontSize: 9, cellPadding: 4 },
      headStyles: { fillColor: [30, 30, 30] },
      margin: { left: margin, right: margin },
    });
    y = doc.lastAutoTable.finalY + 20;

    // Ventes détaillées
    doc.setFontSize(12);
    doc.text("Détails ventes (factures)", margin, y); y += 8;

    doc.autoTable({
      startY: y + 6,
      head: [["Date","Réf","Client","Paiement","HT","TPS","TVQ","Taxes","TTC","Coût pièces","Profit"]],
      body: ventesRows.map(r=>{
        // HT recalculé depuis TTC et Taxes
        const ttc = parseMoney(r[8]);
        const taxes = parseMoney(r[7]);
        const ht = money(ttc - taxes);
        return [r[0], r[1], r[2], r[3], ht, r[5], r[6], r[7], r[8], r[9], r[10]];
      }),
      theme: "grid",
      styles: { fontSize: 7, cellPadding: 3 },
      headStyles: { fillColor: [30, 30, 30] },
      margin: { left: margin, right: margin },
      didDrawPage: function(data){
        doc.setFontSize(8);
        doc.text(`Rapport fiscal / comptable  •  ${labelFrom || "-"} → ${labelTo || "-"}`, margin, 20);
        doc.text(`Page ${doc.getNumberOfPages()}`, pageW - margin, 20, { align: "right" });
      }
    });

    // Achats détaillés
    doc.addPage();
    y = 48;
    doc.setFontSize(12);
    doc.text("Détails achats pièces", margin, y); y += 8;

    doc.autoTable({
      startY: y + 6,
      head: [["Date","Fournisseur","Description","Paiement","HT","TPS","TVQ","Taxes","TTC"]],
      body: achatsRows,
      theme: "grid",
      styles: { fontSize: 8, cellPadding: 3 },
      headStyles: { fillColor: [30, 30, 30] },
      margin: { left: margin, right: margin },
      didDrawPage: function(data){
        doc.setFontSize(8);
        doc.text(`Rapport fiscal / comptable  •  ${labelFrom || "-"} → ${labelTo || "-"}`, margin, 20);
        doc.text(`Page ${doc.getNumberOfPages()}`, pageW - margin, 20, { align: "right" });
      }
    });

    const fname = `rapport_fiscal_comptable_${labelFrom || "debut"}_au_${labelTo || "fin"}.pdf`;
    doc.save(fname);
    showToast("Rapport comptable PDF généré.");
  }catch(e){
    console.error(e);
    showToast("Erreur PDF: "+(e.message||e), true);
  }
}

// Convertit "1 234,56 $" en nombre (fallback)
function parseMoney(v){
  try{
    if(typeof v === "number") return v;
    let s = String(v||"").replace(/\$/g,"").replace(/\s/g,"").replace(/,/g,".");
    const n = Number(s);
    return isNaN(n) ? 0 : n;
  }catch(e){ return 0; }
}





// init revenue controls
if(revPresetEl && revFromEl && revToEl){
  setRevenuePreset(revPresetEl.value || "month");
  revPresetEl.addEventListener("change", ()=>{
    setRevenuePreset(revPresetEl.value);
    renderRevenue();
  });
  if(btnRevApply) btnRevApply.addEventListener("click", ()=>renderRevenue());
  if(btnRevExport) btnRevExport.addEventListener("click", ()=>exportRevenueCSV());
  if(revPayFilterEl) revPayFilterEl.addEventListener("change", ()=>renderRevenue())

  // Fiscal filters
  if(fiscPresetEl && fiscFromEl && fiscToEl){
    setFiscalPreset(fiscPresetEl.value || "month");
    fiscPresetEl.addEventListener("change", ()=>{
      setFiscalPreset(fiscPresetEl.value);
      renderFiscal();
    });
  }
  if(fiscPayFilterEl) fiscPayFilterEl.addEventListener("change", ()=>renderFiscal());
  if(fiscPurchasePayFilterEl) fiscPurchasePayFilterEl.addEventListener("change", ()=>renderFiscal());
  if(fiscFromEl) fiscFromEl.addEventListener("change", ()=>{ if(fiscPresetEl) fiscPresetEl.value="custom"; renderFiscal(); });
  if(fiscToEl) fiscToEl.addEventListener("change", ()=>{ if(fiscPresetEl) fiscPresetEl.value="custom"; renderFiscal(); });
  if(btnFiscExportCsv) btnFiscExportCsv.addEventListener("click", ()=>exportFiscalCSV());
  if(btnFiscExportComptable) btnFiscExportComptable.addEventListener("click", ()=>exportComptableCSVs());
  if(btnFiscExportComptablePdf) btnFiscExportComptablePdf.addEventListener("click", ()=>exportComptablePDF());

  // Parts expenses filters
  if(pexPresetEl && pexFromEl && pexToEl){
    setPartsExpPreset(pexPresetEl.value || "month");
    pexPresetEl.addEventListener("change", ()=>{
      setPartsExpPreset(pexPresetEl.value);
      renderPartsExpenses();
    });
  }
  if(pexPayFilterEl) pexPayFilterEl.addEventListener("change", ()=>renderPartsExpenses());
  if(pexFromEl) pexFromEl.addEventListener("change", ()=>{ if(pexPresetEl) pexPresetEl.value="custom"; renderPartsExpenses(); });
  if(pexToEl) pexToEl.addEventListener("change", ()=>{ if(pexPresetEl) pexPresetEl.value="custom"; renderPartsExpenses(); });
  if(btnNewPartsExpense) btnNewPartsExpense.addEventListener("click", ()=>openPartsExpenseModal(null));
  if(btnNewSupplier) btnNewSupplier.addEventListener("click", ()=>openSupplierModal(null));
  if(btnPartsExpExport) btnPartsExpExport.addEventListener("click", ()=>exportPartsExpensesCSV());
}

// ===== Promo selection (clients) =====
window.__togglePromoSelected = async (customerId, checked)=>{
  if(currentRole !== "admin" && currentRole !== "superadmin") return;
  try{
    await updateDoc(garageDoc("customers", customerId), {
      promoSelected: !!checked,
      promoSelectedAtTs: serverTimestamp()
    });
  }catch(err){

    alert("Impossible de modifier la sélection promo. Vérifie les permissions Firestore (admin).");
  }
};

window.__promoSelectAll = async (checked)=>{
  if(currentRole !== "admin" && currentRole !== "superadmin") return;
  const list = customers.filter(c=>c && c.id);
  if(list.length===0) return;
  const label = checked ? "Tout sélectionner" : "Tout désélectionner";
  if(!confirm(`${label} pour ${list.length} client(s) ?`)) return;

  try{
    // batch updates (max 500 writes per batch)
    for(let i=0; i<list.length; i+=450){
      const chunk = list.slice(i, i+450);
      const batch = writeBatch(db);
      chunk.forEach(c=>{
        batch.update(garageDoc("customers", c.id), {
          promoSelected: !!checked,
          promoSelectedAtTs: serverTimestamp()
        });
      });
      await batch.commit();
    }
  }catch(err){

    alert("Erreur: impossible de mettre à jour la sélection promo.");
  }
};

// Sélectionner uniquement les clients qui ont un email
window.__promoSelectHasEmail = async ()=>{
  if(currentRole !== "admin" && currentRole !== "superadmin") return;
  const list = customers.filter(c=>c && c.id);
  if(list.length===0) return;
  if(!confirm(`Sélectionner uniquement les clients avec email (et désélectionner les autres) ?`)) return;

  try{
    // Mise à jour locale (pour rafraîchir l'UI tout de suite)
    customers = customers.map(c=>{
      const email = String(c?.email||"").trim();
      const hasEmail = email.includes("@") && email.includes(".");
      return { ...c, promoSelected: hasEmail };
    });
    renderClients();
    fillInvoiceCustomers();
    fillInvoiceWorkorders();
    if(typeof renderPromotions === "function") renderPromotions();

    // batch updates (max 500 writes per batch)
    for(let i=0; i<list.length; i+=450){
      const chunk = list.slice(i, i+450);
      const batch = writeBatch(db);
      chunk.forEach(c=>{
        const email = String(c?.email||"").trim();
        const hasEmail = email.includes("@") && email.includes(".");
        batch.update(garageDoc("customers", c.id), {
          promoSelected: !!hasEmail,
          promoSelectedAtTs: serverTimestamp()
        });
      });
      await batch.commit();
    }
  }catch(err){

    alert("Erreur: impossible de sélectionner ceux avec email.");
  }
};


function _isPaidCustomer(c){
  const ps = String(c?.paymentStatus ?? "").trim().toLowerCase();
  if(ps==="paid" || ps==="paye" || ps==="payé") return true;
  if(ps==="unpaid" || ps==="nonpaye" || ps==="non payé" || ps==="nonpayé") return false;
  if(typeof c?.isPaid === "boolean") return c.isPaid;
  if(typeof c?.paid === "boolean") return c.paid;
  // Par défaut: payé (on n'affiche pas comme "non payé" si pas défini)
  return true;
}

window.__setCustomerPaid = async (customerId, isPaid)=>{
  try{
    await updateDoc(doc(colCustomers(), customerId), {
      paymentStatus: isPaid ? "paid" : "unpaid",
      paidAt: isPaid ? serverTimestamp() : null,
      updatedAt: serverTimestamp(),
    });
  }catch(e){

  }
};

window.__emailPaymentRequest = (customerId)=>{
  const c = customers.find(x=>x.id===customerId);
  if(!c) return;
  const email = (c.email||"").trim();
  if(!email){
    alert("Ce client n'a pas d'email.");
    return;
  }
  const subject = `Paiement en attente — Garage Pro One`;
  const body =
`Bonjour ${c.fullName||""},

Votre paiement est en attente chez Garage Pro One.

Merci de nous contacter ou de passer au garage pour régler votre facture.
Téléphone: (ajoute ton numéro ici)

Merci,
Garage Pro One`;
  const url = `mailto:${encodeURIComponent(email)}?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`;
  window.location.href = url;
};

function renderClients(){
  const q = ($("clientsSearch").value||"").trim().toLowerCase();
  let list = [...customers].sort((a,b)=> String(a.fullName||"").localeCompare(String(b.fullName||""), 'fr'));
  if(q){
    list = list.filter(c =>
      (c.fullName||"").toLowerCase().includes(q) ||
      (c.phone||"").toLowerCase().includes(q) ||
      (c.email||"").toLowerCase().includes(q)
    );
  }

  if(clientsCount) clientsCount.textContent = `${list.length} client(s)`;
  if(promoSelCount){
    const sel = customers.filter(c=>c && c.promoSelected===true).length;
    promoSelCount.textContent = `${sel} sélectionné(s)`;
  }

  // Empty state
  if(list.length===0){
    if(clientsTbody) clientsTbody.innerHTML = '<tr><td colspan="5" class="muted">Aucun client.</td></tr>';
    if(clientsCards) clientsCards.innerHTML = '<div class="muted" style="padding:10px 4px">Aucun client.</div>';
    return;
  }

  // Desktop table rows (sans "Payé" — le paiement est lié aux réparations/factures)
  if(clientsTbody){
    clientsTbody.innerHTML = list.map(c=>{
      return `
      <tr>
        <td>${safe(c.fullName)}</td>
        <td>${safe(c.phone||"")}</td>
        <td>${safe(c.email||"")}</td>
        <td class="nowrap">
          <label class="row" style="gap:6px; align-items:center">
            <input type="checkbox" ${c.promoSelected ? "checked" : ""} onchange="window.__togglePromoSelected('${c.id}', this.checked)">
            <span class="muted" style="font-size:12px">Oui</span>
          </label>
        </td>
        <td class="nowrap">
          <button class="btn btn-small" onclick="window.__openClientView('${c.id}')">Ouvrir</button>
          <button class="btn btn-small btn-ghost" onclick="window.__openClientForm('${c.id}')">Modifier</button>
        </td>
      </tr>`;
    }).join("");
  }

  // Mobile cards
  if(clientsCards){
    clientsCards.innerHTML = list.map(c=>{
      const vehiclesCount = (vehicles||[]).filter(v=> v.customerId===c.id).length;
      const openRepairsCount = (workorders||[]).filter(w=> w.customerId===c.id && String(w.status||"").toUpperCase()!=="TERMINE").length;
      const lastRepair = (workorders||[])
        .filter(w=> w.customerId===c.id)
        .sort((a,b)=> new Date(b.updatedAtTs || b.createdAtTs || b.updatedAt || b.createdAt || 0) - new Date(a.updatedAtTs || a.createdAtTs || a.updatedAt || a.createdAt || 0))[0];
      const lastVisit = lastRepair ? safe(String(lastRepair.updatedAt || lastRepair.createdAt || "").slice(0,10) || "—") : "—";
      return `
      <div class="client-card client-card-pro">
        <div class="client-top">
          <div class="client-main">
            <div class="client-name">${safe(c.fullName)}</div>
            <div class="client-meta-row">📞 ${safe(c.phone||"—")}</div>
            <div class="client-meta-row">✉️ ${c.email ? safe(c.email||"") : "—"}</div>
          </div>
          <span class="client-badge">Client actif</span>
        </div>

        <div class="client-stats">
          <span>${vehiclesCount} véhicule${vehiclesCount>1?"s":""}</span>
          <span>${openRepairsCount} réparation${openRepairsCount>1?"s":""} en cours</span>
          <span>Dernière visite : ${lastVisit}</span>
        </div>

        <div class="client-bottom-row">
          <label class="promo-check promo-check-pro">
            <input type="checkbox" ${c.promoSelected ? "checked" : ""} onchange="window.__togglePromoSelected('${c.id}', this.checked)">
            <span>Inclure dans promo</span>
          </label>

          <div class="client-actions">
            <button class="btn btn-small btn-primary" onclick="window.__openClientView('${c.id}')">Ouvrir</button>
            <button class="btn btn-small btn-ghost" onclick="window.__openClientForm('${c.id}')">Modifier</button>
          </div>
        </div>
      </div>`;
    }).join("");
  }
}

function openClientStats(customerId){
  const c = customers.find(x=>x.id===customerId) || {};
  if(csTitle) csTitle.textContent = `Stats: ${c.fullName||"Client"}`;
  if(csSubtitle) csSubtitle.textContent = `${c.phone||""} ${c.email?("• "+c.email):""}`.trim();

  const invs = (invoices||[]).filter(inv=> (inv.customerId===customerId) || (String(inv.customerName||"").toLowerCase()===String(c.fullName||"").toLowerCase()));
  invs.sort((a,b)=> invoiceDateAsDate(b)-invoiceDateAsDate(a));

  let sales=0, parts=0, net=0;
  for(const inv of invs){
    const t = getInvoiceTotals(inv);
    sales += t.sell;
    parts += t.cost;
    net += t.profit;
  }
  if(csCount) csCount.textContent = String(invs.length);
  if(csSales) csSales.textContent = money(sales);
  if(csParts) csParts.textContent = money(parts);
  if(csNet) csNet.textContent = money(net);

  if(csTbody){
    if(invs.length===0){
      csTbody.innerHTML = '<tr><td class="muted" colspan="4">Aucune facture.</td></tr>';
    }else{
      csTbody.innerHTML = invs.slice(0,20).map(inv=>{
        const t = getInvoiceTotals(inv);
        return `
          <tr>
            <td>${safe(inv.ref||inv.id||"—")}</td>
            <td>${safe(invoiceDateKey(inv)||"—")}</td>
            <td style="text-align:right">${money(t.sell)}</td>
            <td style="text-align:right"><b>${money(t.profit)}</b></td>
          </tr>
        `;
      }).join("");
    }
  }

  if(clientStatsModal) clientStatsModal.style.display="flex";
}
window.__openClientStats = openClientStats;

/* Repairs view */
const repairsTbody = $("repairsTbody");
const repairsCards = $("repairsCards");
const repairsCount = $("repairsCount");
$("btnRepairsFilter").onclick = ()=>renderRepairs();
      if(currentRole === "admin" || currentRole === "superadmin") renderRevenue();
$("btnRepairsClear").onclick = ()=>{ $("repairsSearch").value=""; $("repairsStatus").value=""; renderRepairs();
      if(currentRole === "admin" || currentRole === "superadmin") renderRevenue(); };

function renderRepairs(){
  const q = ($("repairsSearch").value||"").trim().toLowerCase();
  const st = $("repairsStatus").value;
  let list = [...workorders].sort(byCreatedDesc);
  if(st) list = list.filter(w=>w.status===st);
  if(q){
    list = list.filter(w=>{
      const v = getVehicle(w.vehicleId) || {};
      const c = v.customerId ? (getCustomer(v.customerId) || {}) : {};
      return (c.fullName||"").toLowerCase().includes(q) ||
             (c.phone||"").toLowerCase().includes(q) ||
             (v.plate||"").toLowerCase().includes(q);
    });
  }
  repairsCount.textContent = `${list.length} réparation(s)`;
  if(list.length===0){
    if(repairsTbody) repairsTbody.innerHTML = '<tr><td colspan="6" class="muted">Aucune réparation.</td></tr>';
    if(repairsCards) repairsCards.innerHTML = '<div class="muted">Aucune réparation.</div>';
    return;
  }
  if(repairsTbody){
    repairsTbody.innerHTML = list.map(w=>{
      const v = getVehicle(w.vehicleId);
      const c = v ? getCustomer(v.customerId) : null;
      const client = c ? c.fullName : "—";
      const veh = v ? [v.year,v.make,v.model].filter(Boolean).join(" ") + (v.plate?` (${v.plate})`:"") : "—";
      const d = String(w.createdAt||"").slice(0,10);
      const pill = w.status==="TERMINE" ? "pill-ok" : (w.status==="EN_COURS" ? "pill-blue" : "pill-warn");
      return `
        <tr>
          <td>${safe(d)}</td>
          <td>${safe(client)}</td>
          <td>${safe(veh)}</td>
          <td><span class="pill ${pill}">${safe(w.status)}</span></td>
          <td>${money(w.total)}</td>
          <td class="nowrap">
            <button class="btn btn-small" onclick="window.__openWorkorderView('${w.id}')">Ouvrir</button>
          </td>
        </tr>
      `;
    }).join("");
  }

  if(repairsCards){
    repairsCards.innerHTML = list.map(w=>{
      const v = getVehicle(w.vehicleId);
      const c = v ? getCustomer(v.customerId) : null;
      const client = c ? c.fullName : "—";
      const veh = v ? [v.year,v.make,v.model].filter(Boolean).join(" ") + (v.plate?` (${v.plate})`:"") : "—";
      const d = String(w.createdAt||"").slice(0,10);
      const pill = w.status==="TERMINE" ? "pill-ok" : (w.status==="EN_COURS" ? "pill-blue" : "pill-warn");
      return `
        <div class="repair-card">
          <div class="repair-top">
            <div>
              <div class="repair-title">${safe(client)}</div>
              <div class="repair-sub">${safe(veh)}</div>
              <div class="repair-sub">${safe(d)}</div>
            </div>
            <span class="pill ${pill}">${safe(w.status)}</span>
          </div>
          <div class="repair-meta">
            <div class="repair-amount">${money(w.total)}</div>
          </div>
          <div class="repair-actions">
            <button class="btn btn-small" onclick="window.__openWorkorderView('${w.id}')">Ouvrir</button>
          </div>
        </div>
      `;
    }).join("");
  }
}

/* Promotions */
const formPromo = $("formPromo");
const promosTbody = $("promosTbody");
const promoSaved = $("promoSaved");
const promoTestEmail = $("promoTestEmail");
const btnPromoSend = $("btnPromoSend");
const promoSendError = $("promoSendError");
const promoSendOk = $("promoSendOk");
const promoAudienceInfo = $("promoAudienceInfo");

function _selectedPromoCustomers(){
  return customers.filter(c=>c && c.promoSelected === true);
}
function _countPromoSelected(){
  return _selectedPromoCustomers().length;
}
function _countPromoSelectedWithEmail(){
  return _selectedPromoCustomers().filter(c=>String(c.email||"").includes("@")).length;
}

function renderPromotions(){
  if(!promosTbody) return;
  if(currentRole !== "admin" && currentRole !== "superadmin"){
    promosTbody.innerHTML = `<tr><td class="muted" colspan="5">Accès réservé à l'administrateur.</td></tr>`;
    return;
  }

  // Audience info
  if(promoAudienceInfo){
    promoAudienceInfo.textContent = `Sélectionnés: ${_countPromoSelected()} (avec email: ${_countPromoSelectedWithEmail()})`;
  }

  if(!promotions.length){
    promosTbody.innerHTML = `<tr><td class="muted" colspan="5">Aucune promotion.</td></tr>`;
    selectedPromotionId = null;
    if(btnPromoSend) btnPromoSend.disabled = true;
    return;
  }

  promosTbody.innerHTML = promotions.map(p=>{
    const d = String(p.createdAt||"").slice(0,10) || "—";
    const valid = p.validUntil ? String(p.validUntil).slice(0,10) : "—";
    const sent = p.lastSentAt ? `Oui (${String(p.lastSentAt).slice(0,10)})` : "Non";
    const isSel = p.id === selectedPromotionId;
    return `
      <tr class="${isSel ? 'row-selected' : ''}">
        <td>${safe(d)}</td>
        <td>${safe(p.subject||'')}</td>
        <td>${safe(valid)}</td>
        <td>${safe(sent)}</td>
        <td class="nowrap"><button class="btn btn-small" data-promo-id="${p.id}">Sélectionner</button></td>
      </tr>
    `;
  }).join("");

  promosTbody.querySelectorAll("[data-promo-id]").forEach(btn=>{
    btn.addEventListener("click", ()=>{
      selectedPromotionId = btn.getAttribute("data-promo-id");
      if(btnPromoSend) btnPromoSend.disabled = false;
      renderPromotions();
    });
  });
}

if(formPromo){
  formPromo.onsubmit = async (e)=>{
    e.preventDefault();
    if(currentRole !== "admin" && currentRole !== "superadmin") return;
    promoSaved.style.display = "none";
    const fd = new FormData(formPromo);
    const subject = String(fd.get("subject")||"").trim();
    const message = String(fd.get("message")||"").trim();
    const code = String(fd.get("code")||"").trim();
    const validUntil = String(fd.get("validUntil")||"").trim();
    if(!subject || !message){
      alert("Objet et message obligatoires.");
      return;
    }
    const docRef = await addDoc(colPromotions(), {
      garageId: currentGarageId || "garage-demo",
      subject,
      message,
      code: code || "",
      validUntil: validUntil || "",
      createdAt: isoNow(),
      createdAtTs: serverTimestamp(),
      createdBy: currentUid,
      lastSentAt: "",
      lastSentAtTs: null,
      sentCount: 0,
    });
    selectedPromotionId = docRef.id;
    if(btnPromoSend) btnPromoSend.disabled = false;
    promoSaved.textContent = "Promotion enregistrée. Sélectionnée pour l’envoi.";
    promoSaved.style.display = "";
    formPromo.reset();
    renderPromotions();
  };
}

// ======= ENVOI PROMO via Firebase Extension (collection "mail") =======
function escHtml(s){
  return String(s||"")
    .replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;")
    .replace(/"/g,"&quot;").replace(/'/g,"&#039;");
}
function applyTemplate(str, vars, promo){
  const data = {
    name: vars?.name || "",
    fullName: vars?.fullName || vars?.name || "",
    phone: vars?.phone || "",
    garageName: vars?.garageName || "",
    garagePhone: vars?.garagePhone || "",
    garageEmail: vars?.garageEmail || "",
    garageAddress: vars?.garageAddress || "",
    promoTitle: promo?.subject || promo?.title || "",
    promoCode: promo?.code || "",
    promoValidUntil: promo?.validUntil || ""
  };

  let out = String(str || "");
  Object.entries(data).forEach(([key, value]) => {
    const safeValue = String(value || "");
    out = out.split(`{${key}}`).join(safeValue);
    out = out.split(`\${${key}}`).join(safeValue);
  });

  out = out
    .split('{promo.title}').join(data.promoTitle)
    .split('${promo.title}').join(data.promoTitle)
    .split('{promo.code}').join(data.promoCode)
    .split('${promo.code}').join(data.promoCode)
    .split('{promo.validUntil}').join(data.promoValidUntil)
    .split('${promo.validUntil}').join(data.promoValidUntil)
    .split('`').join('');

  return out;
}
function buildPromoHtml(promo, vars){
  const finalSubject = applyTemplate(`${vars.garageName || "Garage Pro One"} | ${promo.subject || promo.title || "Promotion"}`, vars, promo)
    .replace(/\s+\|\s+\|/g, " | ")
    .replace(/^\s*\|\s*/, "")
    .trim();
  const subject = escHtml(finalSubject);
  const msg = applyTemplate(promo.message, vars, promo);

  const garageName = escHtml(vars.garageName || settings?.garageName || "Garage Pro One");
  const garagePhone = escHtml(vars.garagePhone || settings?.garagePhone || "");
  const garageEmail = escHtml(vars.garageEmail || settings?.garageEmail || "");
  const garageAddress = escHtml(vars.garageAddress || settings?.garageAddress || "");
  const garageLogoUrl = String(vars.garageLogoUrl || settings?.garageLogoUrl || settings?.logoUrl || "").trim();

  const msgHtml = escHtml(msg).replace(/\n/g,"<br>");
  const codeHtml = promo.code ? `<p><b>Code promo :</b> ${escHtml(promo.code)}</p>` : "";
  const validHtml = promo.validUntil ? `<p><b>Valable jusqu’au :</b> ${escHtml(String(promo.validUntil).slice(0,10))}</p>` : "";
  const logoHtml = garageLogoUrl ? `<div style="margin:0 0 16px 0"><img src="${escHtml(garageLogoUrl)}" alt="logo" style="max-height:72px;max-width:180px"></div>` : "";

  return `
    <div style="font-family:Arial,Helvetica,sans-serif;line-height:1.5;color:#111">
      ${logoHtml}
      <h2 style="margin:0 0 12px 0">${subject}</h2>
      <p>${msgHtml}</p>
      ${codeHtml}
      ${validHtml}
      <hr style="margin:20px 0">
      <p style="color:#666;font-size:12px;margin:0 0 6px 0"><b>${garageName}</b></p>
      ${garagePhone ? `<p style="color:#666;font-size:12px;margin:0 0 6px 0">${garagePhone}</p>` : ""}
      ${garageEmail ? `<p style="color:#666;font-size:12px;margin:0 0 6px 0">${garageEmail}</p>` : ""}
      ${garageAddress ? `<p style="color:#666;font-size:12px;margin:0">${garageAddress}</p>` : ""}
    </div>
  `;
}

if(btnPromoSend){
  btnPromoSend.addEventListener("click", async ()=>{
    promoSendError.style.display = "none";
    promoSendOk.style.display = "none";
    if(currentRole !== "admin" && currentRole !== "superadmin") return;

    if(!selectedPromotionId){
      alert("Sélectionne une promotion.");
      return;
    }

    const promo = promotions.find(p=>p.id === selectedPromotionId);
    if(!promo){
      alert("Promotion introuvable. Recharge la page.");
      return;
    }

    const testEmail = String(promoTestEmail?.value||"").trim();
    const isTest = testEmail.includes("@");
    const replyTo = String(settings?.garageEmail || "").trim();
    const garageName = String(settings?.garageName || "Garage Pro One").trim();
    const garagePhone = String(settings?.garagePhone || "").trim();
    const garageAddress = String(settings?.garageAddress || "").trim();
    const garageEmail = String(settings?.garageEmail || "").trim();
    const garageLogoUrl = String(settings?.garageLogoUrl || settings?.logoUrl || "").trim();

    // liste destinataires
    const recipients = isTest
      ? [{ email: testEmail, name: "Test", phone: "" }]
      : customers
          .filter(c=>c && c.promoSelected === true)
          .filter(c=>String(c.email||"").includes("@"))
          .map(c=>({ email: String(c.email).trim(), name: String(c.name||"").trim(), phone: String(c.phone||"").trim() }));

    const msgConfirm = isTest
      ? `Envoyer un TEST à: ${testEmail} ?`
      : `Envoyer cette promotion aux clients sélectionnés avec email (${recipients.length}) ?`;

    if(!confirm(msgConfirm)) return;

    btnPromoSend.disabled = true;

    try{
      // ⚠️ Extension attend une collection ROOT nommée "mail"
      // On fait des batches (max 500 écritures par batch)
      let total = recipients.length;
      let sent = 0;

      for(let i=0; i<recipients.length; i+=400){
        const chunk = recipients.slice(i, i+400);
        const batch = writeBatch(db);

        chunk.forEach(r=>{
          const vars = {
            name: r.name,
            fullName: r.name,
            phone: r.phone,
            garageName,
            garagePhone,
            garageEmail,
            garageAddress,
            garageLogoUrl
          };
          const finalSubject = applyTemplate(`${garageName || "Garage Pro One"} | ${promo.subject || promo.title || "Promotion"}`, vars, promo)
            .replace(/\s+\|\s+\|/g, " | ")
            .replace(/^\s*\|\s*/, "")
            .trim();
          const text = applyTemplate(promo.message, vars, promo);
          const html = buildPromoHtml(promo, vars);

          const mailRef = doc(collection(db, "mail")); // ROOT "mail"
          batch.set(mailRef, {
            to: [r.email],
            replyTo: replyTo || null,
            garageId: currentGarageId || "garage-demo",
            garageName: garageName || "Garage Pro One",
            message: {
              subject: finalSubject,
              text,
              html
            },
            createdAt: isoNow(),
            createdAtTs: serverTimestamp(),
            promotionId: selectedPromotionId
          });
        });

        await batch.commit();
        sent += chunk.length;
      }

      // marque la promo comme envoyée (date + compteur)
      await updateDoc(doc(colPromotions(), selectedPromotionId), {
        lastSentAt: isoNow(),
        lastSentAtTs: serverTimestamp(),
        sentCount: (promo.sentCount || 0) + (isTest ? 0 : sent)
      });

      promoSendOk.textContent = `Envoi déclenché: ${sent} / ${total}` + (isTest ? " (test)" : "");
      promoSendOk.style.display = "";
    }catch(err){

      promoSendError.textContent =
        (err?.message || "Erreur envoi. Vérifie Firestore rules + extension + collection 'mail'.");
      promoSendError.style.display = "";
    }finally{
      btnPromoSend.disabled = false;
    }
  });
}

/* Settings */
$("btnSaveSettings").onclick = async ()=>{
  const tps = parseFloat(String($("setTps").value).replace(',','.'))/100;
  const tvq = parseFloat(String($("setTvq").value).replace(',','.'))/100;
  const cardFee = parseFloat(String($("setCardFee").value||"0").replace(',','.'))/100;
  const laborRate = parseFloat(String($("setLaborRate")?.value||"0").replace(',','.'));
  const garageName = String($("setGarageName")?.value||"").trim();
  const garageAddress = String($("setGarageAddress")?.value||"").trim();
  const garagePhone = String($("setGaragePhone")?.value||"").trim();
  const garageEmail = String($("setGarageEmail")?.value||"").trim();
  const garageLogoUrl = String($("setGarageLogoUrl")?.value||"").trim();
  const garageTpsNo = String($("setGarageTpsNo")?.value||"").trim();
  const garageTvqNo = String($("setGarageTvqNo")?.value||"").trim();
  const signatureName = String($("setSignatureName")?.value||"").trim();
  const selectedTheme = String($("themeSelect")?.value || "light");
  const selectedDevMode = String($("devModeSelect")?.value || "off");
  if(!isFinite(tps) || !isFinite(tvq) || !isFinite(cardFee) || !isFinite(laborRate) || tps<0 || tvq<0 || cardFee<0 || laborRate<0){
    alert("TPS/TVQ invalides.");
    return;
  }
  applyTheme(selectedTheme);
  applyDevMode(selectedDevMode);
  const gid = await ensureCurrentGarageId();
  if(!gid) throw new Error("missing-garageId");
  await setDoc(docSettings(), { garageId: gid, tpsRate: tps, tvqRate: tvq, cardFeeRate: cardFee, laborRate: laborRate, garageName, garageAddress, garagePhone, garageEmail, garageLogoUrl, logoUrl: garageLogoUrl, garageTpsNo, garageTvqNo, tpsNumber: garageTpsNo, tvqNumber: garageTvqNo, signatureName, theme: selectedTheme, devMode: selectedDevMode, updatedAt: serverTimestamp() }, { merge:true });
  alert("Paramètres enregistrés.");
};
const themeSelectEl = $("themeSelect");
if(themeSelectEl){
  themeSelectEl.onchange = (e)=> applyTheme(e.target.value);
}
const devModeSelectEl = $("devModeSelect");
if(devModeSelectEl){
  devModeSelectEl.onchange = (e)=> applyDevMode(e.target.value);
}
const garageLogoUrlInput = $("setGarageLogoUrl");
if(garageLogoUrlInput){
  garageLogoUrlInput.addEventListener("input", (e)=>{
    const value = String(e.target?.value || "").trim();
    settings.garageLogoUrl = value;
    setGarageLogoPreview(value);
    updateGarageBrand();
  });
}
const garageLogoFileInput = $("setGarageLogoFile");
if(garageLogoFileInput){
  garageLogoFileInput.addEventListener("change", ()=>{
    const file = garageLogoFileInput.files?.[0];
    if(!file){
      setGarageLogoPreview($("setGarageLogoUrl")?.value || settings.garageLogoUrl || "");
      return;
    }
    const tmp = URL.createObjectURL(file);
    setGarageLogoPreview(tmp);
  });
}
const btnUploadGarageLogo = $("btnUploadGarageLogo");
if(btnUploadGarageLogo){
  btnUploadGarageLogo.onclick = async ()=>{
    try{
      btnUploadGarageLogo.disabled = true;
      btnUploadGarageLogo.textContent = "Upload...";
      const url = await uploadGarageLogoFile();
      alert("Logo envoyé avec succès.");
      setGarageLogoPreview(url);
    }catch(err){
      alert(err?.message || "Erreur upload logo.");
    }finally{
      btnUploadGarageLogo.disabled = false;
      btnUploadGarageLogo.textContent = "📤 Uploader le logo";
    }
  };
}


function buildRegisterLink(){
  try{
    const gid = String(currentGarageId || garageId || localStorage.getItem("garageId") || "").trim();
    if(!gid) return "";
    const baseUrl = new URL(window.location.href);
    let pathname = baseUrl.pathname || "/";
    pathname = pathname.replace(/index\.html?$/i, "register.html");
    if(pathname === "/") pathname = "/register.html";
    baseUrl.pathname = pathname;
    baseUrl.search = `?garageId=${encodeURIComponent(gid)}`;
    baseUrl.hash = "";
    return baseUrl.toString();
  }catch(e){
    try{
      const gid = String(currentGarageId || garageId || "").trim();
      return gid ? `register.html?garageId=${encodeURIComponent(gid)}` : "";
    }catch(_){ return ""; }
  }
}

async function copyRegisterLink(){
  const link = buildRegisterLink();
  if(!link){ toast("Lien register indisponible"); return; }
  try{
    if(navigator.clipboard?.writeText){
      await navigator.clipboard.writeText(link);
    }else{
      const el = $("registerLinkFull");
      if(el){ el.focus(); el.select(); document.execCommand("copy"); }
    }
    toast("Lien register copié");
  }catch(e){
    try{
      const el = $("registerLinkFull");
      if(el){ el.focus(); el.select(); document.execCommand("copy"); toast("Lien register copié"); return; }
    }catch(_){}
    toast("Impossible de copier le lien");
  }
}

function renderSettings(){
  $("setTps").value = (settings.tpsRate*100).toFixed(3).replace(/\.000$/,'').replace(/0+$/,'').replace(/\.$/,'');
  $("setTvq").value = (settings.tvqRate*100).toFixed(3).replace(/\.000$/,'').replace(/0+$/,'').replace(/\.$/,'');
  $("setCardFee").value = (Number(settings.cardFeeRate||0)*100).toFixed(3).replace(/\.000$/,'').replace(/0+$/,'').replace(/\.$/,'');
  // Tarif main-d'œuvre
  const laborEl = $("setLaborRate");
  if(laborEl) laborEl.value = String(Number(settings.laborRate||0));
  // Infos garage (facture)
  const gn = $("setGarageName"); if(gn) gn.value = String(settings.garageName||"");
  const ga = $("setGarageAddress"); if(ga) ga.value = String(settings.garageAddress||"");
  const gp = $("setGaragePhone"); if(gp) gp.value = String(settings.garagePhone||"");
  const ge = $("setGarageEmail"); if(ge) ge.value = String(settings.garageEmail||"");
  const gl = $("setGarageLogoUrl"); if(gl) gl.value = String(settings.garageLogoUrl||"");
  const gt = $("setGarageTpsNo"); if(gt) gt.value = String(settings.garageTpsNo||"");
  const gv = $("setGarageTvqNo"); if(gv) gv.value = String(settings.garageTvqNo||"");
  setGarageLogoPreview(String(settings.garageLogoUrl||""));
  const sn = $("setSignatureName"); if(sn) sn.value = String(settings.signatureName||"");
  const regLink = $("registerLinkFull"); if(regLink) regLink.value = buildRegisterLink();
  const copyBtn = $("btnCopyRegisterLink"); if(copyBtn && !copyBtn.dataset.bound){ copyBtn.dataset.bound = "1"; copyBtn.onclick = copyRegisterLink; }
  updateGarageBrand();
  applyTheme(String(settings.theme || (function(){ try{return localStorage.getItem("gpo_theme")||"light";}catch(e){return "light";} })()));
  applyDevMode(String(settings.devMode || (function(){ try{return localStorage.getItem("gpo_dev_mode")||"off";}catch(e){return "off";} })()));
}


/* Theme */
function applyTheme(theme){
  const value = theme === "dark" ? "dark" : "light";
  document.body.classList.remove("light","dark");
  document.body.classList.add(value);
  try{ localStorage.setItem("gpo_theme", value); }catch(e){}
  const sel = $("themeSelect");
  if(sel) sel.value = value;
}
function initTheme(){
  let saved = "light";
  try{ saved = localStorage.getItem("gpo_theme") || "light"; }catch(e){}
  applyTheme(saved);
}
initTheme();
updateGarageBrand();

/* Dev mode / console */
function isDevMode(){
  return String(settings?.devMode || (function(){ try{return localStorage.getItem("gpo_dev_mode")||"off";}catch(e){return "off";} })()) === "on";
}
function applyDevMode(mode){
  const value = mode === "on" ? "on" : "off";
  try{ localStorage.setItem("gpo_dev_mode", value); }catch(e){}
  const sel = $("devModeSelect");
  if(sel) sel.value = value;
  const box = $("devConsole");
  if(box) box.style.display = value === "on" ? "block" : "none";
  if(value === "on") document.body.classList.add("dev-console-open");
  else document.body.classList.remove("dev-console-open");
}
function devLog(){
  try{ console.log.apply(console, arguments); }catch(e){}
  if(!isDevMode()) return;
  const body = $("devConsoleBody");
  if(!body) return;
  const line = document.createElement("div");
  line.className = "dev-console-line";
  line.textContent = Array.from(arguments).map(v=>{
    if(v instanceof Error) return `${v.name}: ${v.message}`;
    if(typeof v === "object"){ try{return JSON.stringify(v, null, 2);}catch(e){ return String(v); } }
    return String(v);
  }).join(" ");
  body.appendChild(line);
  body.scrollTop = body.scrollHeight;
}
window.__devLog = devLog;
window.onerror = function(message, source, lineno, colno, error){
  devLog("JS", message, source || "", `L${lineno}:${colno}`, error && error.stack ? error.stack : (error || ""));
};
window.addEventListener("unhandledrejection", (ev)=>{
  const r = ev.reason;
  devLog("PROMISE", r && r.code ? r.code : "", r && r.message ? r.message : String(r), r && r.stack ? r.stack : "");
});
if($("btnClearDevConsole")) $("btnClearDevConsole").onclick = ()=>{ if($("devConsoleBody")) $("devConsoleBody").innerHTML = ""; };
if($("btnToggleDevConsole")) $("btnToggleDevConsole").onclick = ()=>{
  const box = $("devConsole");
  const btn = $("btnToggleDevConsole");
  if(!box || !btn) return;
  const minimized = box.classList.toggle("minimized");
  btn.textContent = minimized ? "Agrandir" : "Réduire";
};
applyDevMode((function(){ try{return localStorage.getItem("gpo_dev_mode")||"off";}catch(e){return "off";} })());

/* Export / Import */
$("btnExport").onclick = ()=>{
  const hoursCalc = Math.max(0, Number(invHoursEl?.value || 0));
  const laborCalc = hoursCalc>0 ? (hoursCalc*Number(settings.laborRate||0)) : Math.max(0, Number(invLaborEl?.value || 0));
  const subCalc = sellTotal + laborCalc;
  const taxCalc = subCalc * (Number(settings.tpsRate||0) + Number(settings.tvqRate||0));
  const grandCalc = subCalc + taxCalc;
  const cardCalc = ((invPayMethodEl?.value||"")==="card") ? (grandCalc*Number(settings.cardFeeRate||0)) : 0;
  const netCalc = subCalc - costTotal - cardCalc;

  const payload = { settings, customers, vehicles, workorders };
  const blob = new Blob([JSON.stringify(payload,null,2)], {type:"application/json"});
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "garage-pro-one-export.json";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
};

$("importFile").addEventListener("change", async (e)=>{
  const file = e.target.files?.[0];
  if(!file) return;
  try{
    const txt = await file.text();
    const obj = JSON.parse(txt);
    if(!obj || typeof obj!=="object") throw new Error("format");
    if(!confirm("Importer ce JSON dans le cloud ? (écrase tout)")) return;
    await wipeCloudData();
    const batch = writeBatch(db);

    const tpsRate = Number(obj.settings?.tpsRate ?? 0.05);
    const tvqRate = Number(obj.settings?.tvqRate ?? 0.09975);
    const cardFeeRate = Number(obj.settings?.cardFeeRate ?? 0.025);
    batch.set(docSettings(), { tpsRate, tvqRate, cardFeeRate, updatedAt: serverTimestamp() }, { merge:true });

    for(const c of (obj.customers||[])){
      batch.set(doc(colCustomers()), { fullName:c.fullName||"", phone:c.phone||"", email:c.email||"", notes:c.notes||"", createdAt:c.createdAt||isoNow(), createdAtTs: serverTimestamp() });
    }
    for(const v of (obj.vehicles||[])){
      batch.set(doc(colVehicles()), {
        customerId: v.customerId||"",
        make:v.make||"", model:v.model||"", year:v.year||"",
        plate:v.plate||"", vin:v.vin||"", currentKm:v.currentKm||"",
        notes:v.notes||"", createdAt:v.createdAt||isoNow(),
        createdAtTs: serverTimestamp()
      });
    }
    for(const w of (obj.workorders||[])){
      batch.set(doc(colWorkorders()), {
        vehicleId: w.vehicleId||"",
        status: w.status||"OUVERT",
        km: w.km||"",
        reportedIssue: w.reportedIssue||"",
        diagnostic: w.diagnostic||"",
        workDone: w.workDone||"",
        notes: w.notes||"",
        items: Array.isArray(w.items)?w.items:[],
        subtotal: Number(w.subtotal||0),
        tpsRate: Number(w.tpsRate||tpsRate),
        tvqRate: Number(w.tvqRate||tvqRate),
        tpsAmount: Number(w.tpsAmount||0),
        tvqAmount: Number(w.tvqAmount||0),
        total: Number(w.total||0),
        createdAt: w.createdAt||isoNow(),
        createdAtTs: serverTimestamp()
      });
    }
    await batch.commit();
    alert("Import terminé.");
    go("dashboard");
  }catch(err){
    alert("Import impossible. Vérifie le JSON.");
  }finally{
    e.target.value="";
  }
});

$("btnResetCloud").onclick = async ()=>{
  if(!confirm("Tout supprimer dans le cloud ? (clients, véhicules, réparations)")) return;
  await wipeCloudData();
  alert("Cloud vidé.");
};

async function wipeCloudData(){
  const deletions = [];
  for(const c of (await getDocs(query(colCustomers(), limit(500)))).docs) deletions.push(deleteDoc(c.ref));
  for(const v of (await getDocs(query(colVehicles(), limit(1000)))).docs) deletions.push(deleteDoc(v.ref));
  for(const w of (await getDocs(query(colWorkorders(), limit(2000)))).docs) deletions.push(deleteDoc(w.ref));
  await Promise.all(deletions);
}

/* ============
   Entities & forms
=========== */
$("btnNewClient").onclick = ()=>openClientForm();
$("btnNewClient2").onclick = ()=>openClientForm();
$("btnNewRepair").onclick = ()=>openNewRepairChooser();
$("btnNewRepair2").onclick = ()=>openNewRepairChooser();

window.__openClientForm = openClientForm;
window.__openClientView = openClientView;
window.__openWorkorderForm = openWorkorderForm;
window.__openWorkorderView = openWorkorderView;
window.__openVehicleForm = openVehicleForm;
window.__openVehicleView = openVehicleView;

async function createCustomer(data){
  const gid = await ensureCurrentGarageId();
  if(!gid) throw new Error("missing-garageId");
  await addDoc(colCustomers(), { garageId: gid, name: data.fullName || data.name || "", ...data, createdAt: isoNow(), createdAtTs: serverTimestamp() });
}
async function updateCustomer(id, data){
  await updateDoc(doc(colCustomers(), id), { ...data, updatedAt: serverTimestamp() });
}
async function deleteCustomer(id){
  const vdocs = (await getDocs(query(colVehicles(), where("customerId","==", id), limit(2000)))).docs;
  const batch = writeBatch(db);
  for(const v of vdocs){
    const wdocs = (await getDocs(query(colWorkorders(), where("vehicleId","==", v.id), limit(2000)))).docs;
    wdocs.forEach(w=>batch.delete(w.ref));
    batch.delete(v.ref);
  }
  batch.delete(doc(colCustomers(), id));
  await batch.commit();
}

function openClientForm(customerId=null){
  const editing = !!customerId;
  const c = editing ? customers.find(x=>x.id===customerId) : {fullName:"",phone:"",email:"",notes:""};
  if(editing && !c){ alert("Client introuvable."); return; }

  openModal(editing ? "Modifier client" : "Nouveau client", `
    <form class="form" id="clientForm">
      <div id="clientError" class="alert" style="display:none"></div>
      <label>Nom complet *</label>
      <input name="fullName" value="${safe(c.fullName||"")}" required />
      <label>Téléphone *</label>
      <input name="phone" value="${safe(c.phone||"")}" required />
      <label>Email</label>
      <input name="email" type="email" value="${safe(c.email||"")}" />
      <label>Notes</label>
      <textarea name="notes" rows="4">${safe(c.notes||"")}</textarea>
      <div class="row-between">
        <button class="btn btn-primary" type="submit">Enregistrer</button>
        <button class="btn btn-ghost" type="button" onclick="window.__closeModal()">Annuler</button>
      </div>
    </form>
  `);
  window.__closeModal = closeModal;

  if ($("clientForm")) $("clientForm").onsubmit = async (e)=>{
    e.preventDefault();
    if(!(currentRole === "admin" || currentRole === "superadmin")){
      alert("Accès refusé : seul l’admin peut créer/modifier des clients.");
      return;
    }
    const fd = new FormData(e.target);
    const fullName = String(fd.get("fullName")||"").trim();
    const phone = String(fd.get("phone")||"").trim();
    const email = String(fd.get("email")||"").trim();
    const notes = String(fd.get("notes")||"").trim();
    const err = $("clientError");
    if(!fullName || !phone){
      err.style.display="";
      err.textContent = "Nom et téléphone sont obligatoires.";
      return;
    }
    try{
      if(editing) await updateCustomer(customerId, {fullName, phone, email, notes});
      else await createCustomer({fullName, phone, email, notes});
      closeModal();
    }catch(ex){
      const msg = (ex && ex.code === "permission-denied")
        ? "Accès refusé (règles Firestore). Vérifie le rôle admin et le garageId du compte."
        : ((ex && String(ex.message||"").includes("missing-garageId")) ? "Garage introuvable pour ce compte. Vérifie staff/{uid}.garageId." : "Erreur sauvegarde client.");
      alert(msg);
    }
  };
}

function openClientView(customerId){
  const c = customers.find(x=>x.id===customerId);
  if(!c){ alert("Client introuvable."); return; }
  const vs = vehicles.filter(v=>v.customerId===c.id).sort(byCreatedDesc);
  const wos = workorders.filter(w=>{
    const v = getVehicle(w.vehicleId);
    return v && v.customerId===c.id;
  }).sort(byCreatedDesc);

  const vehRows = vs.length ? vs.map(v=>{
    const veh = [v.year,v.make,v.model].filter(Boolean).join(" ");
    return `
      <tr>
        <td>${safe(veh)}</td>
        <td>${safe(v.plate||"")}</td>
        <td class="muted">${safe(v.vin||"")}</td>
        <td class="nowrap">
          <button class="btn btn-small" onclick="window.__openVehicleView('${v.id}')">Ouvrir</button>
          ${(currentRole === 'admin' || currentRole === 'superadmin') ? `<button class="btn btn-small btn-ghost" onclick="window.__openWorkorderForm('${v.id}')">+ Réparation</button>` : ``}
        </td>
      </tr>
    `;
  }).join("") : `<tr><td colspan="4" class="muted">Aucun véhicule.</td></tr>`;

  const woRows = wos.length ? wos.map(w=>{
    const v = getVehicle(w.vehicleId);
    const veh = v ? [v.make,v.model].filter(Boolean).join(" ") + (v.plate?` (${v.plate})`:"") : "—";
    const pill = w.status==="TERMINE" ? "pill-ok" : (w.status==="EN_COURS" ? "pill-blue" : "pill-warn");
    return `
      <tr>
        <td>${safe(String(w.createdAt||"").slice(0,10))}</td>
        <td>${safe(veh)}</td>
        <td>${safe(w.km||"")}</td>
        <td><span class="pill ${pill}">${safe(w.status)}</span></td>
        <td>${money(w.total)}</td>
        <td class="nowrap"><button class="btn btn-small" onclick="window.__openWorkorderView('${w.id}')">Ouvrir</button></td>
      </tr>
    `;
  }).join("") : `<tr><td colspan="6" class="muted">Aucune réparation.</td></tr>`;

  openModal("Fiche client", `
    <div class="row-between">
      <div>
        <h2 style="margin:0">${safe(c.fullName)}</h2>
        <div class="muted" style="margin-top:6px">
          <strong>Tél:</strong> ${safe(c.phone||"")} &nbsp; • &nbsp;
          <strong>Email:</strong> ${safe(c.email||"")}
        </div>
      </div>
      ${currentRole === 'admin' ? `
      <div class="row">
        <button class="btn btn-small" onclick="window.__openClientForm('${c.id}')">Modifier</button>
        <button class="btn btn-small btn-ghost" onclick="window.__openVehicleForm(null, '${c.id}')">+ Véhicule</button>
        <button class="btn btn-small btn-danger" onclick="window.__deleteCustomer('${c.id}')">Supprimer</button>
      </div>
      ` : ``}
    </div>
    ${c.notes ? `<div class="note" style="margin-top:12px">${safe(c.notes).replace(/\n/g,'<br>')}</div>` : ""}
    <div class="divider"></div>
    <h3>Véhicules</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Véhicule</th><th>Plaque</th><th>VIN</th><th></th></tr></thead>
        <tbody>${vehRows}</tbody>
      </table>
    </div>
    <div class="divider"></div>
    <h3>Historique des réparations</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Date</th><th>Véhicule</th><th>KM</th><th>Statut</th><th>Total</th><th></th></tr></thead>
        <tbody>${woRows}</tbody>
      </table>
    </div>
  `);
}
window.__deleteCustomer = async (id)=>{
  if(!confirm("Supprimer ce client (et ses véhicules/réparations) ?")) return;
  await deleteCustomer(id);
  closeModal();
};

/* Vehicles */
async function createVehicle(customerId, data){
  const gid = await ensureCurrentGarageId();
  if(!gid) throw new Error("missing-garageId");
  await addDoc(colVehicles(), { garageId: gid, customerId, ...data, createdAt: isoNow(), createdAtTs: serverTimestamp() });
}
async function updateVehicle(id, data){
  await updateDoc(doc(colVehicles(), id), { ...data, updatedAt: serverTimestamp() });
}
async function deleteVehicle(id){
  const wdocs = (await getDocs(query(colWorkorders(), where("vehicleId","==", id), limit(2000)))).docs;
  const batch = writeBatch(db);
  wdocs.forEach(w=>batch.delete(w.ref));
  batch.delete(doc(colVehicles(), id));
  await batch.commit();
}

// ===== Vehicle make/model helper (vPIC) =====
const VPIC_BASE = "https://vpic.nhtsa.dot.gov/api";
const MAKE_CACHE_KEY = "gpo_vpic_makes_v1";
const MAKE_CACHE_TTL_MS = 1000 * 60 * 60 * 24 * 30; // 30 days

async function loadMakesIntoDatalist(datalistEl){
  try{
    if(!datalistEl) return;
    datalistEl.innerHTML = "";
    const now = Date.now();
    let cached = null;
    try{
      cached = JSON.parse(localStorage.getItem(MAKE_CACHE_KEY) || "null");
    }catch(e){ cached = null; }
    let makes = (cached && cached.makes && (now - cached.ts) < MAKE_CACHE_TTL_MS) ? cached.makes : null;

    if(!makes){
      const res = await fetch(`${VPIC_BASE}/vehicles/getallmakes?format=json`, { headers: { "Accept": "application/json" } });
      const data = await res.json();
      makes = (data && data.Results) ? data.Results.map(x=>x.Make_Name).filter(Boolean) : [];
      makes = [...new Set(makes)].sort((a,b)=>a.localeCompare(b));
      try{ localStorage.setItem(MAKE_CACHE_KEY, JSON.stringify({ts: now, makes})); }catch(e){}
    }
    datalistEl.innerHTML = makes.map(m=>`<option value="${safe(m)}"></option>`).join("");
  }catch(e){
    // silent (offline, blocked, etc.)
  }
}

async function loadModelsIntoDatalist(makeName, datalistEl, hintEl){
  try{
    if(!datalistEl) return;
    datalistEl.innerHTML = "";
    if(hintEl) hintEl.textContent = makeName ? "Chargement des modèles..." : "Choisis d’abord une marque pour voir les modèles.";
    if(!makeName){ return; }

    // cache per make (1 month)
    const key = `gpo_vpic_models_${String(makeName||"").toLowerCase()}`;
    const ttl = MAKE_CACHE_TTL_MS;
    const now = Date.now();
    let cached = null;
    try{ cached = JSON.parse(localStorage.getItem(key) || "null"); }catch(e){ cached = null; }
    let models = (cached && cached.models && (now - cached.ts) < ttl) ? cached.models : null;

    if(!models){
      const url = `${VPIC_BASE}/vehicles/getmodelsformake/${encodeURIComponent(makeName)}?format=json`;
      const res = await fetch(url, { headers: { "Accept": "application/json" } });
      const data = await res.json();
      models = (data && data.Results) ? data.Results.map(x=>x.Model_Name).filter(Boolean) : [];
      models = [...new Set(models)].sort((a,b)=>a.localeCompare(b));
      try{ localStorage.setItem(key, JSON.stringify({ts: now, models})); }catch(e){}
    }
    datalistEl.innerHTML = models.map(m=>`<option value="${safe(m)}"></option>`).join("");
    if(hintEl) hintEl.textContent = models.length ? "Sélectionne un modèle (liste automatique)." : "Aucun modèle trouvé pour cette marque.";
  }catch(e){
    if(hintEl) hintEl.textContent = "Impossible de charger les modèles (hors ligne ?).";
  }
}

function openVehicleForm(vehicleId=null, customerId=null){
  const editing = !!vehicleId;
  const v = editing ? vehicles.find(x=>x.id===vehicleId) : {
    make:"",model:"",year:"",plate:"",vin:"",currentKm:"",notes:"",
    cylinders:"", engineType:"", bodyType:"", seats:"",
    customerId
  };
  if(editing && !v){ alert("Véhicule introuvable."); return; }
  const c = customers.find(x=>x.id===customerId);
  if(!c){ alert("Client introuvable."); return; }

  openModal(editing ? "Modifier véhicule" : "Nouveau véhicule", `
    <div class="muted">Client: <strong>${safe(c.fullName)}</strong></div>
    <div class="divider"></div>
    <form class="form" id="vehicleForm">
      <div id="vehicleError" class="alert" style="display:none"></div>
      <label>Marque *</label>
      <input name="make" list="makeList" autocomplete="off" value="${safe(v.make||"")}" required />
      <datalist id="makeList"></datalist>
      <div class="muted" style="margin-top:4px">Commence à taper (liste automatique des marques).</div>
      <label>Modèle *</label>
      <input name="model" list="modelList" autocomplete="off" value="${safe(v.model||"")}" required />
      <datalist id="modelList"></datalist>
      <div id="modelHint" class="muted" style="margin-top:4px">Choisis d’abord une marque pour voir les modèles.</div>
      <label>Année</label>
      <input name="year" inputmode="numeric" value="${safe(v.year||"")}" / list="yearListVehicle">
      
      <div class="vehicle-scan-head">
        <div class="muted">Scan rapide</div>
        <div class="vehicle-scan-actions">
          <button type="button" class="btn secondary" data-vin-scan>Scanner VIN</button>
          <button type="button" class="btn secondary" data-plate-scan>Scanner plaque</button>
          <button type="button" class="btn secondary" data-vin-decode>Remplir via VIN</button>
        </div>
        <small class="muted" style="display:block;margin-top:6px">
          VIN : remplit marque, modèle, année, type, moteur, cylindres (vPIC). Plaque : remplit la plaque (OCR).
        </small>
      </div>

      <div class="grid-2">
        <div>
          <label>Plaque</label>
          <div class="input-with-btn">
            <input name="plate" value="${safe(v.plate||"")}" placeholder="ex: ABC 123" />
            <button type="button" class="btn icon" title="Scanner plaque" data-plate-scan>📷</button>
          </div>
        </div>
        <div>
          <label>VIN</label>
          <div class="input-with-btn">
            <input name="vin" value="${safe(v.vin||"")}" placeholder="17 caractères" />
            <button type="button" class="btn icon" title="Scanner VIN" data-vin-scan>📷</button>
          </div>
        </div>
      </div>

      <label>Kilométrage actuel</label>
      <input name="currentKm" inputmode="numeric" value="${safe(v.currentKm||"")}" />

      <div class="divider"></div>
      <h3 style="margin:0">Infos véhicule</h3>
      <div class="muted" style="margin-top:6px">Ces infos aident pour le suivi et les rapports.</div>

      <label>Type de moteur</label>
      <select name="engineType">
        ${[
          {v:"",t:"—"},
          {v:"Essence",t:"Essence"},
          {v:"Diesel",t:"Diesel"},
          {v:"Hybride",t:"Hybride"},
          {v:"Électrique",t:"Électrique"},
          {v:"Autre",t:"Autre"},
        ].map(o=>`<option value="${o.v}" ${String(v.engineType||"")===o.v?"selected":""}>${o.t}</option>`).join("")}
      </select>

      <label>Nombre de cylindres</label>
      <select name="cylinders">
        ${["", "3", "4", "5", "6", "8", "10", "12"].map(n=>`<option value="${n}" ${String(v.cylinders||"")===n?"selected":""}>${n? n+" cylindres":"—"}</option>`).join("")}
      </select>

      <label>Type de véhicule</label>
      <select name="bodyType" id="bodyType">
        ${[
          {v:"",t:"—"},
          // Voitures
          {v:"Coupe",t:"Coupé"},
          {v:"Berline",t:"Berline"},
          {v:"Hatchback",t:"Hatchback"},
          {v:"Cabriolet",t:"Cabriolet"},
          {v:"Wagon",t:"Familiale / Wagon"},
          // Camions / utilitaires
          {v:"VUS",t:"VUS / SUV"},
          {v:"Pickup",t:"Pick-up"},
          {v:"Van",t:"Van / Minivan"},
          {v:"Fourgon",t:"Fourgon / Cargo"},
          {v:"Camion",t:"Camion (commercial)"},
          {v:"ChassisCabine",t:"Châssis-cabine"},
          {v:"Bus",t:"Bus / Minibus"},
          {v:"Remorque",t:"Remorque / Trailer"},
          {v:"Autre",t:"Autre"},
        ].map(o=>`<option value="${o.v}" ${String(v.bodyType||"")===o.v?"selected":""}>${o.t}</option>`).join("")}
      </select>

      <div id="seatsWrap" style="display:none">
        <label>Nombre de places (si van / bus)</label>
        <input name="seats" inputmode="numeric" value="${safe(v.seats||"")}" placeholder="ex: 7" />
      </div>

      <label>Notes</label>
      <textarea name="notes" rows="4">${safe(v.notes||"")}</textarea>
      <div class="row-between">
        <button class="btn btn-primary" type="submit">Enregistrer</button>
        <button class="btn btn-ghost" type="button" onclick="window.__closeModal()">Annuler</button>
      </div>
    </form>
  `);

  // show/hide seats when Van / Bus
  const toggleSeats = ()=>{
    const bt = $("bodyType")?.value || "";
    const wrap = $("seatsWrap");
    if(!wrap) return;
    const seatTypes = ["Van","Bus"]; 
    wrap.style.display = (seatTypes.includes(bt)) ? "" : "none";
    if(!seatTypes.includes(bt)){
      const inp = wrap.querySelector('input[name="seats"]');
      if(inp) inp.value = "";
    }
  };
  if ($("bodyType")) $("bodyType").addEventListener("change", toggleSeats);

  // Make/Model lists (vPIC)
  const vf = $("vehicleForm");
  const makeInput = vf.querySelector('input[name="make"]');
  const modelInput = vf.querySelector('input[name="model"]');
  const makeList = $("makeList");
  const modelList = $("modelList");
  const modelHint = $("modelHint");
  loadMakesIntoDatalist(makeList);
  // preload models if editing
  loadModelsIntoDatalist(makeInput.value, modelList, modelHint);

  let lastMake = (makeInput.value||"").trim();
  const onMakeChanged = async ()=>{
    const mk = (makeInput.value||"").trim();
    if(mk && mk !== lastMake){
      // reset model when make changes
      if(modelInput) modelInput.value = "";
    }
    lastMake = mk;
    await loadModelsIntoDatalist(mk, modelList, modelHint);
  };
  makeInput.addEventListener("change", onMakeChanged);
  makeInput.addEventListener("blur", onMakeChanged);

  toggleSeats();

  if ($("vehicleForm")) $("vehicleForm").onsubmit = async (e)=>{
    e.preventDefault();
    const fd = new FormData(e.target);
    const make = String(fd.get("make")||"").trim();
    const model = String(fd.get("model")||"").trim();
    const year = String(fd.get("year")||"").trim();
    const plate = String(fd.get("plate")||"").trim();
    const vin = String(fd.get("vin")||"").trim();
    const currentKm = String(fd.get("currentKm")||"").trim();
    const engineType = String(fd.get("engineType")||"").trim();
    const cylinders = String(fd.get("cylinders")||"").trim();
    const bodyType = String(fd.get("bodyType")||"").trim();
    const seats = String(fd.get("seats")||"").trim();
    const notes = String(fd.get("notes")||"").trim();
    const err = $("vehicleError");
    if(!make || !model){
      err.style.display="";
      err.textContent = "Marque et modèle sont obligatoires.";
      return;
    }
    if(bodyType === "Van" && seats && !String(seats).match(/^\d+$/)){
      err.style.display="";
      err.textContent = "Nombre de places doit être un nombre (ex: 7).";
      return;
    }
    try{
      const payload = {make,model,year,plate,vin,currentKm,engineType,cylinders,bodyType,seats:(bodyType==="Van"?seats:""),notes};
      if(editing) await updateVehicle(vehicleId, payload);
      else await createVehicle(customerId, payload);
      closeModal();
    }catch(ex){
      alert("Erreur sauvegarde véhicule.");
    }
  };
}

function openVehicleView(vehicleId){
  const v = vehicles.find(x=>x.id===vehicleId);
  if(!v){ alert("Véhicule introuvable."); return; }
  const c = customers.find(x=>x.id===v.customerId);
  const wos = workorders.filter(w=>w.vehicleId===v.id).sort(byCreatedDesc);

  const woRows = wos.length ? wos.map(w=>{
    const pill = w.status==="TERMINE" ? "pill-ok" : (w.status==="EN_COURS" ? "pill-blue" : "pill-warn");
    return `
      <tr>
        <td>${safe(String(w.createdAt||"").slice(0,10))}</td>
        <td>${safe(w.km||"")}</td>
        <td><span class="pill ${pill}">${safe(w.status)}</span></td>
        <td>${money(w.total)}</td>
        <td class="nowrap"><button class="btn btn-small" onclick="window.__openWorkorderView('${w.id}')">Ouvrir</button></td>
      </tr>
    `;
  }).join("") : `<tr><td colspan="5" class="muted">Aucune réparation.</td></tr>`;

  const vehTxt = [v.year,v.make,v.model].filter(Boolean).join(" ");
  const extraLines = [
    v.engineType ? `Moteur: <strong>${safe(v.engineType)}</strong>` : null,
    v.cylinders ? `Cylindres: <strong>${safe(v.cylinders)}</strong>` : null,
    v.bodyType ? `Type: <strong>${safe(v.bodyType)}</strong>${(v.bodyType==="Van" && v.seats)?` &nbsp;•&nbsp; Places: <strong>${safe(v.seats)}</strong>`:""}` : null,
  ].filter(Boolean);
  openModal("Fiche véhicule", `
    <div class="row-between">
      <div>
        <h2 style="margin:0">${safe(vehTxt)}</h2>
        <div class="muted" style="margin-top:6px">
          Client: <a href="#" onclick="window.__openClientView('${v.customerId}'); return false;">${safe(c?.fullName||c?.name||"—")}</a><br/>
          Plaque: <strong>${safe(v.plate||"")}</strong> &nbsp; • &nbsp; VIN: ${safe(v.vin||"")}<br/>
          KM: ${safe(v.currentKm||"")}
          ${extraLines.length ? `<br/>${extraLines.join(" &nbsp;•&nbsp; ")}` : ""}
        </div>
      </div>
      <div class="row">
        <button class="btn btn-small" onclick="window.__openVehicleForm('${v.id}', '${v.customerId}')">Modifier</button>
        <button class="btn btn-small btn-ghost" onclick="window.__openWorkorderForm('${v.id}')">+ Réparation</button>
        <button class="btn btn-small btn-danger" onclick="window.__deleteVehicle('${v.id}')">Supprimer</button>
      </div>
    </div>
    ${v.notes ? `<div class="note" style="margin-top:12px">${safe(v.notes).replace(/\n/g,'<br>')}</div>` : ""}
    <div class="divider"></div>
    <h3>Historique des réparations</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Date</th><th>KM</th><th>Statut</th><th>Total</th><th></th></tr></thead>
        <tbody>${woRows}</tbody>
      </table>
    </div>
  `);
}
window.__deleteVehicle = async (id)=>{
  if(!confirm("Supprimer ce véhicule (et ses réparations) ?")) return;
  await deleteVehicle(id);
  closeModal();
};

/* Workorders */
function calcTotals(items, tpsRate, tvqRate){
  let subtotal = 0;
  const clean = [];
  for(const it of items){
    const desc = String(it.desc||"").trim();
    if(!desc) continue;
    const type = (it.type==="MO") ? "MO" : "PIECE";
    const qty  = Math.max(0.000001, Number(String(it.qty||1).replace(',','.')) || 1);
    const unit = Math.max(0, Number(String(it.unit||0).replace(',','.')) || 0);
    const line = qty * unit;
    subtotal += line;
    clean.push({type, desc, qty, unit, line});
  }
  const tpsAmount = subtotal * tpsRate;
  const tvqAmount = subtotal * tvqRate;
  const total = subtotal + tpsAmount + tvqAmount;
  return {items: clean, subtotal, tpsAmount, tvqAmount, total};
}

function openNewRepairChooser(){
  if(customers.length===0){
    alert("Ajoute d'abord un client.");
    openClientForm();
    return;
  }
  openModal("Nouvelle réparation", `
    <p class="muted">Choisis un véhicule (recherche par nom client / plaque / VIN), puis crée la réparation.</p>
    <form class="form form-inline" onsubmit="return false;">
      <input id="chooseVehQ" placeholder="Nom / Téléphone / Plaque / VIN" />
      <button class="btn btn-primary" id="btnChooseVeh">Rechercher</button>
    </form>
    <div class="divider"></div>
    <div id="chooseVehRes" class="muted">Tape une recherche.</div>
  `);
  const qEl = $("chooseVehQ");
  const resEl = $("chooseVehRes");
  if ($("btnChooseVeh")) $("btnChooseVeh").onclick = ()=>{
    const q = (qEl.value||"").trim().toLowerCase();
    if(!q){ resEl.innerHTML = '<span class="muted">Tape une recherche.</span>'; return; }
    const rows = [];
    for(const v of vehicles){
      const c = getCustomer(v.customerId);
      const hit = (c?.fullName||"").toLowerCase().includes(q) ||
                  (c?.phone||"").toLowerCase().includes(q) ||
                  (v.plate||"").toLowerCase().includes(q) ||
                  (v.vin||"").toLowerCase().includes(q);
      if(hit) rows.push({v,c});
    }
    if(rows.length===0){ resEl.innerHTML = '<div class="muted">Aucun véhicule.</div>'; return; }
    resEl.innerHTML = `
      <div class="table-wrap">
        <table>
          <thead><tr><th>Client</th><th>Véhicule</th><th>Plaque</th><th></th></tr></thead>
          <tbody>
            ${rows.slice(0,50).map(r=>{
              const veh = [r.v.year,r.v.make,r.v.model].filter(Boolean).join(" ");
              return `
                <tr>
                  <td>${safe(r.c?.fullName||"—")}</td>
                  <td>${safe(veh)}</td>
                  <td>${safe(r.v.plate||"")}</td>
                  <td class="nowrap"><button class="btn btn-small" onclick="window.__openWorkorderForm('${r.v.id}')">Choisir</button></td>
                </tr>
              `;
            }).join("")}
          </tbody>
        </table>
      </div>
    `;
  };
  if (qEl) qEl.addEventListener("keydown", (e)=>{ if(e.key==="Enter" && $("btnChooseVeh")) $("btnChooseVeh").click(); });
}

async function createWorkorder(data){
  if(!data.invoiceNo){
    data.invoiceNo = await nextInvoiceNo();
  }
  const gid = await ensureCurrentGarageId();
  if(!gid) throw new Error("missing-garageId");
  await addDoc(colWorkorders(), { garageId: gid, ...data, createdAt: isoNow(), createdAtTs: serverTimestamp(), createdBy: currentUid, updatedAt: isoNow(), updatedAtTs: serverTimestamp(), updatedBy: currentUid });
  if(data.km){
    await updateDoc(doc(colVehicles(), data.vehicleId), { currentKm: data.km, updatedAt: isoNow(), updatedAtTs: serverTimestamp() });
  }
}

function openWorkorderForm(vehicleId){
  const v = getVehicle(vehicleId);
  if(!v){ alert("Véhicule introuvable."); return; }
  const c = getCustomer(v.customerId);
  const vehTxt = [v.year,v.make,v.model].filter(Boolean).join(" ");

  openModal("Nouvelle réparation", `
    <div class="muted">
      Client: <strong>${safe(c?.fullName||c?.name||"—")}</strong><br/>
      Véhicule: <strong>${safe(vehTxt)}</strong> ${v.plate?`— Plaque: <strong>${safe(v.plate)}</strong>`:""}
    </div>
    <div class="divider"></div>
    <form class="form" id="woForm">
      <div id="woError" class="alert" style="display:none"></div>
      <div class="row" style="gap:12px">
        <div style="flex:1; min-width:220px">
          <label>Statut</label>
          <select name="status">
            <option value="OUVERT">Ouvert</option>
            <option value="EN_COURS">En cours</option>
            <option value="TERMINE">Terminé</option>
          </select>
        </div>
        <div style="flex:1; min-width:220px">
          <label>KM (visite)</label>
          <input name="km" inputmode="numeric" placeholder="ex: 123456" />
        </div>
      </div>

      ${currentRole==="admin" ? `
      <div class="row" style="gap:12px">
        <div style="flex:1; min-width:220px">
          <label>Assigné à (mécanicien)</label>
          <select name="assignedTo">
            <option value="">— Non assigné —</option>
            ${mechanics.map(m=>`<option value="${m.uid}">${safe(m.name)}</option>`).join("")}
          </select>
        </div>
      </div>

      ` : ``}

      <div class="row" style="gap:12px">
        <div style="flex:1; min-width:220px">
          <label>Paiement</label>
          <select name="paymentMethod">
            <option value="">Non défini</option>
            <option value="CASH">Cash</option>
            <option value="CARTE">Carte</option>
            <option value="VIREMENT">Virement</option>
            <option value="AUTRE">Autre</option>
          </select>
        </div>
        <div style="flex:1; min-width:220px">
          <label>Statut paiement</label>
          <select name="paymentStatus">
            <option value="NON_PAYE">Non payé</option>
            <option value="PAYE">Payé</option>
          </select>
        </div>
      </div>

      <label>Problème rapporté (client)</label>
      <textarea name="reportedIssue" rows="3" placeholder="ex: bruit avant gauche..."></textarea>
      <label>Diagnostic</label>
      <textarea name="diagnostic" rows="3"></textarea>
      <label>Travaux effectués</label>
      <textarea name="workDone" rows="3"></textarea>

      <h3>Lignes (pièces / main d’œuvre)</h3>
      <div class="table-wrap">
        <table id="itemsTable">
          <thead><tr><th>Type</th><th>Description</th><th>Qté</th><th>Prix</th><th></th></tr></thead>
          <tbody></tbody>
        </table>
      </div>
      <div class="row">
        <button class="btn btn-ghost" type="button" id="btnAddLine">+ Ajouter une ligne</button>
        <span class="muted">Total TTC calculé automatiquement.</span>
      </div>
      <div class="note" id="totalsBox"></div>

      <label>Notes</label>
      <textarea name="notes" rows="3"></textarea>

      <div class="row-between">
        <button class="btn btn-primary" type="submit">Enregistrer</button>
        <button class="btn btn-ghost" type="button" onclick="window.__closeModal()">Annuler</button>
      </div>
    </form>
  `);

  const tbody = modalBody.querySelector("#itemsTable tbody");
  const totalsBox = modalBody.querySelector("#totalsBox");

  function addLine(def={}){
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>
        <select class="itType">
          <option value="PIECE">Pièce</option>
          <option value="MO">Main d’œuvre</option>
        </select>
      </td>
      <td><input class="itDesc" placeholder="ex: Plaquettes de frein" /></td>
      <td><input class="itQty" inputmode="decimal" value="1" /></td>
      <td><input class="itUnit" inputmode="decimal" placeholder="0.00" /></td>
      <td class="nowrap"><button class="btn btn-small btn-ghost" type="button">-</button></td>
    `;
    tbody.appendChild(tr);
    tr.querySelector(".itType").value = def.type || "PIECE";
    tr.querySelector(".itDesc").value = def.desc || "";
    tr.querySelector(".itQty").value  = (def.qty ?? 1);
    tr.querySelector(".itUnit").value = (def.unit ?? "");
    tr.querySelector("button").onclick = ()=>{ tr.remove(); recalc(); };
    ["input","change"].forEach(evt=>{
      tr.querySelector(".itType").addEventListener(evt, recalc);
      tr.querySelector(".itDesc").addEventListener(evt, recalc);
      tr.querySelector(".itQty").addEventListener(evt, recalc);
      tr.querySelector(".itUnit").addEventListener(evt, recalc);
    });
    recalc();
  }

  function collectItems(){
    const rows = [...tbody.querySelectorAll("tr")];
    return rows.map(r=>({
      type: r.querySelector(".itType").value,
      desc: r.querySelector(".itDesc").value,
      qty:  r.querySelector(".itQty").value,
      unit: r.querySelector(".itUnit").value
    }));
  }

  function recalc(){
    const items = collectItems();
    const t = calcTotals(items, settings.tpsRate, settings.tvqRate);
    totalsBox.innerHTML = `
      <div class="row-between"><span>Sous-total</span><strong>${money(t.subtotal)}</strong></div>
      <div class="row-between"><span>TPS (${pct(settings.tpsRate)})</span><strong>${money(t.tpsAmount)}</strong></div>
      <div class="row-between"><span>TVQ (${pct(settings.tvqRate)})</span><strong>${money(t.tvqAmount)}</strong></div>
      <div class="divider"></div>
      <div class="row-between" style="font-size:16px"><span><strong>Total TTC</strong></span><strong>${money(t.total)}</strong></div>
    `;
  }

  if ($("btnAddLine")) $("btnAddLine").onclick = ()=>addLine({});
  for(let i=0;i<5;i++) addLine({type:"PIECE", qty:1});

  if ($("woForm")) $("woForm").onsubmit = async (e)=>{
    e.preventDefault();
    const fd = new FormData(e.target);
    const status = String(fd.get("status")||"OUVERT");
    const km = String(fd.get("km")||"").trim();
    const reportedIssue = String(fd.get("reportedIssue")||"").trim();
    const diagnostic = String(fd.get("diagnostic")||"").trim();
    const workDone = String(fd.get("workDone")||"").trim();
    const notes = String(fd.get("notes")||"").trim();
    const paymentMethod = String(fd.get("paymentMethod")||"").trim();
    const paymentStatus = String(fd.get("paymentStatus")||"NON_PAYE").trim();
    const assignedTo = String(fd.get("assignedTo")||"").trim();
    const assignedName = mechanics.find(m=>m.uid===assignedTo)?.name || "";

    const items = collectItems();
    const t = calcTotals(items, settings.tpsRate, settings.tvqRate);

    const err = $("woError");
    if(!reportedIssue && !workDone && t.items.length===0){
      err.style.display="";
      err.textContent = "Ajoute au moins un problème, un travail, ou une ligne de facture.";
      return;
    }
    try{
      await createWorkorder({
        vehicleId,
        status: (status==="TERMINE"?"TERMINE":(status==="EN_COURS"?"EN_COURS":"OUVERT")),
        km, reportedIssue, diagnostic, workDone, notes, paymentMethod, paymentStatus,
        assignedTo: (currentRole==="admin" ? assignedTo : currentUid),
        assignedName: (currentRole==="admin" ? assignedName : (currentUserName || "")),
        
        items: t.items,
        subtotal: t.subtotal,
        tpsRate: settings.tpsRate,
        tvqRate: settings.tvqRate,
        tpsAmount: t.tpsAmount,
        tvqAmount: t.tvqAmount,
        total: t.total
      });
      closeModal();
    }catch(ex){
      alert("Erreur sauvegarde réparation.");
    }
  };
}

async function setWorkorderStatus(id, status){
  const ref = doc(colWorkorders(), id);
  const payload = { status, updatedAt: isoNow(), updatedAtTs: serverTimestamp(), updatedBy: currentUid, updatedByName: (currentUserName || auth?.currentUser?.email || "") };
  if(currentRole==="mechanic"){
    payload.needsAdminReview = true;
    payload.lastEditedBy = currentUid;
    payload.lastEditedByName = payload.updatedByName;
  }
  await updateDoc(ref, payload);
  if(currentRole==="mechanic"){
    await addLog("WORKORDER_STATUS", { workorderId: id, message: `Statut changé à ${status} par un mécanicien (${payload.updatedByName}).` });
  }else{
    await addLog("WORKORDER_STATUS", { workorderId: id, message: `Statut changé à ${status} par l'admin (${payload.updatedByName}).` });
  }

  // Update local cache immediately (meilleure UX + évite impression "ça marche pas")
  const wo = workorders.find(w=>w.id===id);
  if(wo){
    wo.status = status;
    wo.updatedAt = isoNow();
    wo.updatedBy = currentUid;
  }
  try{ renderRepairs(); }catch(e){}
  try{ renderDashboard(); }catch(e){}
}

async function toggleWorkorderStatus(id, next){
  await updateDoc(doc(colWorkorders(), id), { status: next, updatedAt: serverTimestamp() });
}
async function deleteWorkorder(id){
  await deleteDoc(doc(colWorkorders(), id));
}

function openWorkorderView(workorderId){
  const wo = workorders.find(w=>w.id===workorderId);
  if(!wo){ alert("Réparation introuvable."); return; }
  const v = getVehicle(wo.vehicleId);
  const c = v ? getCustomer(v.customerId) : null;
  const vehTxt = v ? [v.year,v.make,v.model].filter(Boolean).join(" ") : "—";
  const printPlate = String(v?.plate || v?.plateNumber || "");
  const printVin = String(v?.vin || v?.vinNumber || "");
  const printKm = String(wo.km || v?.currentKm || v?.mileage || v?.km || "");
  const pill = wo.status==="TERMINE" ? "pill-ok" : (wo.status==="EN_COURS" ? "pill-blue" : "pill-warn");

  const itemsRows = (wo.items && wo.items.length) ? wo.items.map(it=>`
    <tr>
      <td>${it.type==="MO" ? "Main d’œuvre" : "Pièce"}</td>
      <td>${safe(it.desc)}</td>
      <td>${safe(it.qty)}</td>
      <td>${money(it.unit)}</td>
      <td>${money(it.line)}</td>
    </tr>
  `).join("") : `<tr><td colspan="5" class="muted">Aucune ligne.</td></tr>`;

  openModal("Réparation", `
    <div class="row-between">
      <div>
        <h2 style="margin:0">Réparation</h2>
        <div class="muted" style="margin-top:6px">
          Date: ${safe(String(wo.createdAt||"").slice(0,16))} —
          Statut: <span class="pill ${pill}">${safe(wo.status)}</span> — Assigné: <strong>${safe(wo.assignedName || "—")}</strong>
        </div>
      </div>
      <div class="row">
        <!-- data-act (delegation) + onclick (fallback iOS pour éviter boutons "qui ne répondent pas") -->
        <button type="button" class="btn btn-small" data-act="printWo" data-id="${wo.id}" onclick="window.__printWorkorder && window.__printWorkorder('${wo.id}')">Imprimer / PDF</button>
        ${currentRole==="admin" ?
          `<button type="button" class="btn btn-small btn-ghost" data-act="editInvoice" data-id="${wo.id}" onclick="window.__editInvoiceFromWorkorder && window.__editInvoiceFromWorkorder('${wo.id}')">Modifier facture</button>`
          :
          `<button type="button" class="btn btn-small btn-ghost" data-act="requestAdmin" data-id="${wo.id}" onclick="window.__requestAdminValidationFromWorkorder && window.__requestAdminValidationFromWorkorder('${wo.id}')">Demander validation admin</button>`
        }
        ${wo.status!=="EN_COURS" ? `<button type="button" class="btn btn-small btn-ghost" data-act="setWoStatus" data-id="${wo.id}" data-status="EN_COURS" onclick="window.__setWoStatus && window.__setWoStatus('${wo.id}','EN_COURS')">Démarrer</button>` : ``}
        ${wo.status!=="TERMINE" ? `<button type="button" class="btn btn-small btn-ghost" data-act="setWoStatus" data-id="${wo.id}" data-status="TERMINE" onclick="window.__setWoStatus && window.__setWoStatus('${wo.id}','TERMINE')">Terminer</button>` : `<button type="button" class="btn btn-small btn-ghost" data-act="setWoStatus" data-id="${wo.id}" data-status="OUVERT" onclick="window.__setWoStatus && window.__setWoStatus('${wo.id}','OUVERT')">Rouvrir</button>`}
        ${currentRole==="admin" ? `<button type="button" class="btn btn-small btn-danger" data-act="deleteWo" data-id="${wo.id}" onclick="window.__deleteWo && window.__deleteWo('${wo.id}')">Supprimer</button>` : ``}
      </div>
    </div>
    <div class="divider"></div>
    <div class="grid" style="grid-template-columns:1fr; gap:12px">
      <div class="note">
        <strong>Client</strong><br/>
        ${safe(c?.fullName||c?.name||"—")}<br/>
        ${safe(c?.phone||"")}<br/>
        ${safe(c?.email||"")}
      </div>
      <div class="note">
        <strong>Véhicule</strong><br/>
        ${safe(vehTxt)}<br/>
        Plaque: ${safe(v?.plate||"")}<br/>
        VIN: ${safe(v?.vin||"")}<br/>
        KM (visite): ${safe(wo.km||"")}
      </div>
    </div>
    ${wo.reportedIssue ? `<h3>Problème rapporté</h3><div class="note">${safe(wo.reportedIssue).replace(/\n/g,'<br>')}</div>` : ""}
    ${wo.diagnostic ? `<h3>Diagnostic</h3><div class="note">${safe(wo.diagnostic).replace(/\n/g,'<br>')}</div>` : ""}
    ${wo.workDone ? `<h3>Travaux effectués</h3><div class="note">${safe(wo.workDone).replace(/\n/g,'<br>')}</div>` : ""}
    <h3>Détails</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Type</th><th>Description</th><th>Qté</th><th>Prix</th><th>Total</th></tr></thead>
        <tbody>${itemsRows}</tbody>
      </table>
    </div>
    <div class="divider"></div>
    <div class="note">
      <div class="row-between"><span>Sous-total</span><strong>${money(wo.subtotal)}</strong></div>
      <div class="row-between"><span>TPS (${pct(wo.tpsRate)})</span><strong>${money(wo.tpsAmount)}</strong></div>
      <div class="row-between"><span>TVQ (${pct(wo.tvqRate)})</span><strong>${money(wo.tvqAmount)}</strong></div>
      <div class="divider"></div>
      <div class="row-between" style="font-size:16px"><span><strong>Total TTC</strong></span><strong>${money(wo.total)}</strong></div>
    </div>
    ${wo.notes ? `<h3>Notes</h3><div class="note">${safe(wo.notes).replace(/\n/g,'<br>')}</div>` : ""}
  `);
}
window.__setWoStatus = async (id, next)=>{ await setWorkorderStatus(id, next); closeModal(); try{ toast("Statut mis à jour ✅"); }catch(e){} };
window.__deleteWo = async (id)=>{ if(!confirm("Supprimer cette réparation ?")) return; await deleteWorkorder(id); closeModal(); };

// Mécanicien: demande à l'admin de valider / finaliser la facture


window.__editInvoiceFromWorkorder = async (workorderId)=>{
  const wo = workorders.find(w=>w.id===workorderId);
  if(!wo){ alert("Réparation introuvable."); return; }
  const v = getVehicle(wo.vehicleId);
  if(!v){ alert("Véhicule introuvable."); return; }
  const c = getCustomer(v.customerId);
  const vehPlate = String(v.plate || v.plateNumber || "");
  const vehVin = String(v.vin || v.vinNumber || "");
  const vehKm = String(wo.km || v.currentKm || v.mileage || v.km || "");
  const vehTxt = [v.year,v.make,v.model].filter(Boolean).join(" ");

  openModal("Modifier facture", `
    <div class="muted">
      Client: <strong>${safe(c?.fullName||c?.name||"—")}</strong><br/>
      Véhicule: <strong>${safe(vehTxt||"—")}</strong> ${v.plate?`— Plaque: <strong>${safe(v.plate)}</strong>`:""}<br/>
      Facture: <strong>${safe(wo.invoiceNo || ("WO-" + String(wo.id||"").slice(0,6).toUpperCase()))}</strong>
    </div>
    <div class="divider"></div>
    <form class="form" id="woEditInvoiceForm">
      <div id="woEditError" class="alert" style="display:none"></div>
      <div class="row" style="gap:12px">
        <div style="flex:1; min-width:220px">
          <label>Statut</label>
          <select name="status">
            <option value="OUVERT" ${wo.status==="OUVERT"?"selected":""}>Ouvert</option>
            <option value="EN_COURS" ${wo.status==="EN_COURS"?"selected":""}>En cours</option>
            <option value="TERMINE" ${wo.status==="TERMINE"?"selected":""}>Terminé</option>
          </select>
        </div>
        <div style="flex:1; min-width:220px">
          <label>KM (visite)</label>
          <input name="km" inputmode="numeric" placeholder="ex: 123456" value="${safe(wo.km||v.currentKm||v.mileage||"")}" />
        </div>
      </div>

      ${currentRole==="admin" ? `
      <div class="row" style="gap:12px">
        <div style="flex:1; min-width:220px">
          <label>Assigné à (mécanicien)</label>
          <select name="assignedTo">
            <option value="">— Non assigné —</option>
            ${mechanics.map(m=>`<option value="${m.uid}" ${String(wo.assignedTo||"")===String(m.uid)?"selected":""}>${safe(m.name)}</option>`).join("")}
          </select>
        </div>
      </div>` : ``}

      <div class="row" style="gap:12px">
        <div style="flex:1; min-width:220px">
          <label>Paiement</label>
          <select name="paymentMethod">
            <option value="" ${!wo.paymentMethod?"selected":""}>Non défini</option>
            <option value="CASH" ${String(wo.paymentMethod||"").toUpperCase()==="CASH"?"selected":""}>Cash</option>
            <option value="CARTE" ${String(wo.paymentMethod||"").toUpperCase()==="CARTE"?"selected":""}>Carte</option>
            <option value="VIREMENT" ${String(wo.paymentMethod||"").toUpperCase()==="VIREMENT"?"selected":""}>Virement</option>
            <option value="AUTRE" ${String(wo.paymentMethod||"").toUpperCase()==="AUTRE"?"selected":""}>Autre</option>
          </select>
        </div>
        <div style="flex:1; min-width:220px">
          <label>Statut paiement</label>
          <select name="paymentStatus">
            <option value="NON_PAYE" ${String(wo.paymentStatus||"").toUpperCase()!=="PAYE"?"selected":""}>Non payé</option>
            <option value="PAYE" ${String(wo.paymentStatus||"").toUpperCase()==="PAYE"?"selected":""}>Payé</option>
          </select>
        </div>
      </div>

      <label>Problème rapporté (client)</label>
      <textarea name="reportedIssue" rows="3" placeholder="ex: bruit avant gauche...">${safe(wo.reportedIssue||"")}</textarea>
      <label>Diagnostic</label>
      <textarea name="diagnostic" rows="3">${safe(wo.diagnostic||"")}</textarea>
      <label>Travaux effectués</label>
      <textarea name="workDone" rows="3">${safe(wo.workDone||"")}</textarea>

      <h3>Lignes (pièces / main d’œuvre)</h3>
      <div class="table-wrap">
        <table id="woEditItemsTable">
          <thead><tr><th>Type</th><th>Description</th><th>Qté</th><th>Prix</th><th></th></tr></thead>
          <tbody></tbody>
        </table>
      </div>
      <div class="row">
        <button class="btn btn-ghost" type="button" id="btnWoEditAddLine">+ Ajouter une ligne</button>
        <span class="muted">Total TTC calculé automatiquement.</span>
      </div>
      <div class="note" id="woEditTotalsBox"></div>

      <label>Notes</label>
      <textarea name="notes" rows="3">${safe(wo.notes||"")}</textarea>

      <div class="row-between">
        <button class="btn btn-primary" type="submit">Enregistrer les modifications</button>
        <button class="btn btn-ghost" type="button" onclick="window.__closeModal()">Annuler</button>
      </div>
    </form>
  `);

  const tbody = modalBody.querySelector("#woEditItemsTable tbody");
  const totalsBox = modalBody.querySelector("#woEditTotalsBox");

  function addLine(def={}){
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>
        <select class="itType">
          <option value="PIECE">Pièce</option>
          <option value="MO">Main d’œuvre</option>
        </select>
      </td>
      <td><input class="itDesc" placeholder="ex: Plaquettes de frein" value="${safe(def.desc||"")}" /></td>
      <td><input class="itQty" inputmode="decimal" value="${safe(def.qty ?? 1)}" /></td>
      <td><input class="itUnit" inputmode="decimal" placeholder="0.00" value="${safe(def.unit ?? "")}" /></td>
      <td class="nowrap"><button class="btn btn-small btn-ghost" type="button">-</button></td>
    `;
    tbody.appendChild(tr);
    tr.querySelector(".itType").value = def.type || "PIECE";
    tr.querySelector("button").onclick = ()=>{ tr.remove(); recalc(); };
    ["input","change"].forEach(evt=>{
      tr.querySelector(".itType").addEventListener(evt, recalc);
      tr.querySelector(".itDesc").addEventListener(evt, recalc);
      tr.querySelector(".itQty").addEventListener(evt, recalc);
      tr.querySelector(".itUnit").addEventListener(evt, recalc);
    });
    recalc();
  }

  function collectItems(){
    const rows = [...tbody.querySelectorAll("tr")];
    return rows.map(r=>({
      type: r.querySelector(".itType").value,
      desc: r.querySelector(".itDesc").value,
      qty:  r.querySelector(".itQty").value,
      unit: r.querySelector(".itUnit").value
    }));
  }

  function recalc(){
    const items = collectItems();
    const t = calcTotals(items, settings.tpsRate, settings.tvqRate);
    totalsBox.innerHTML = `
      <div class="row-between"><span>Sous-total</span><strong>${money(t.subtotal)}</strong></div>
      <div class="row-between"><span>TPS (${pct(settings.tpsRate)})</span><strong>${money(t.tpsAmount)}</strong></div>
      <div class="row-between"><span>TVQ (${pct(settings.tvqRate)})</span><strong>${money(t.tvqAmount)}</strong></div>
      <div class="divider"></div>
      <div class="row-between" style="font-size:16px"><span><strong>Total TTC</strong></span><strong>${money(t.total)}</strong></div>
    `;
  }

  modalBody.querySelector("#btnWoEditAddLine").onclick = ()=>addLine({});
  if(Array.isArray(wo.items) && wo.items.length){ wo.items.forEach(it=>addLine(it)); }
  else { addLine({type:"PIECE", qty:1}); }

  modalBody.querySelector("#woEditInvoiceForm").onsubmit = async (e)=>{
    e.preventDefault();
    const fd = new FormData(e.target);
    const status = String(fd.get("status")||wo.status||"OUVERT");
    const km = String(fd.get("km")||"").trim();
    const reportedIssue = String(fd.get("reportedIssue")||"").trim();
    const diagnostic = String(fd.get("diagnostic")||"").trim();
    const workDone = String(fd.get("workDone")||"").trim();
    const notes = String(fd.get("notes")||"").trim();
    const paymentMethod = String(fd.get("paymentMethod")||"").trim();
    const paymentStatus = String(fd.get("paymentStatus")||"NON_PAYE").trim();
    const assignedTo = currentRole==="admin" ? String(fd.get("assignedTo")||"").trim() : String(wo.assignedTo || currentUid || "");
    const assignedName = mechanics.find(m=>m.uid===assignedTo)?.name || (currentRole==="admin" ? "" : (currentUserName || wo.assignedName || ""));
    const items = collectItems();
    const t = calcTotals(items, settings.tpsRate, settings.tvqRate);
    const err = modalBody.querySelector("#woEditError");
    if(!reportedIssue && !workDone && t.items.length===0){
      err.style.display="";
      err.textContent = "Ajoute au moins un problème, un travail, ou une ligne de facture.";
      return;
    }
    try{
      const payload = {
        status: (status==="TERMINE"?"TERMINE":(status==="EN_COURS"?"EN_COURS":"OUVERT")),
        km,
        reportedIssue,
        diagnostic,
        workDone,
        notes,
        paymentMethod,
        paymentStatus,
        assignedTo,
        assignedName,
        items: t.items,
        subtotal: t.subtotal,
        tpsRate: settings.tpsRate,
        tvqRate: settings.tvqRate,
        tpsAmount: t.tpsAmount,
        tvqAmount: t.tvqAmount,
        total: t.total,
        updatedAt: isoNow(),
        updatedAtTs: serverTimestamp(),
        updatedBy: currentUid,
        updatedByName: (currentUserName || auth?.currentUser?.email || "")
      };
      devLog("SAVE_REPAIR_INVOICE payload", {workorderId: wo.id, payload});
      await updateDoc(doc(colWorkorders(), wo.id), payload);
      try{ toast("Facture de la réparation mise à jour ✅"); }catch(e){}
      closeModal();
    }catch(ex){
      devLog("SAVE_REPAIR_INVOICE error", ex && ex.code ? ex.code : "", ex && ex.message ? ex.message : ex, ex && ex.stack ? ex.stack : "");
      alert("Erreur modification facture : " + (ex?.message || ex || "inconnue"));
    }
  };
};
window.__requestAdminValidationFromWorkorder = async (id)=>{
  // Ici on ne modifie PAS la facture (Option A: admin only)
  // On marque juste la réparation comme nécessitant l'intervention admin.
  const uid = auth?.currentUser?.uid || null;
  if(!uid) throw new Error("not-authenticated");
  const ref = doc(colWorkorders(), id);
  await updateDoc(ref, {
    needsAdminReview: true,
    adminRequestedAt: serverTimestamp(),
    adminRequestedBy: uid,
    updatedAtTs: serverTimestamp(),
    updatedBy: uid,
  });
  try{ toast("Demande envoyée à l'admin ✅"); }catch(e){}
};

/* Print */
window.__printWorkorder = async (workorderId)=>{
  const wo = workorders.find(w=>w.id===workorderId);
  if(!wo) return;
  const v = getVehicle(wo.vehicleId);
  const c = v ? getCustomer(v.customerId) : null;
  const vehTxt = v ? [v.year,v.make,v.model].filter(Boolean).join(" ") : "—";
  const printPlate = String(v?.plate || v?.plateNumber || "");
  const printVin = String(v?.vin || v?.vinNumber || "");
  const printKm = String(wo.km || v?.currentKm || v?.mileage || v?.km || "");
  const baseHref = String(new URL('.', window.location.href));
  const gName = (settings?.garageName || GARAGE.name || "Garage");
  const gAddr = (settings?.garageAddress || [GARAGE.address1, GARAGE.address2, GARAGE.country].filter(Boolean).join(" — "));
  const gPhone = (settings?.garagePhone || GARAGE.phone || "");
  const gEmail = (settings?.garageEmail || GARAGE.email || "");
  const gSign = (settings?.signatureName || "");
  const invNo = safe(wo.invoiceNo || ("WO-" + String(wo.id||"").slice(0,6).toUpperCase()));
  const rows = (wo.items||[]).map(it=>`
    <tr>
      <td>${it.type==="MO"?"Main d'œuvre":"Pièce"}</td>
      <td>${safe(it.desc)}</td>
      <td class="num">${safe(it.qty)}</td>
      <td class="num">${money(it.unit)}</td>
      <td class="num">${money(it.line)}</td>
    </tr>
  `).join("") || `<tr><td colspan="5">Aucune ligne</td></tr>`;

  const html = `
  <!doctype html><html lang="fr"><head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <base href="${baseHref}">
    <title>Facture ${invNo} — ${safe(gName)}</title>
    <style>
      /* Facture pro — optimisée PDF */
      :root{ --ink:#111827; --muted:#6b7280; --line:#e5e7eb; --soft:#f3f4f6; }
      *{ box-sizing:border-box; }
      body{ margin:24px; font-family: Arial, Helvetica, sans-serif; color:var(--ink); background:#fff; }
      .no-print{ margin-bottom:12px; }
      .no-print button{ padding:10px 14px; border-radius:10px; border:1px solid var(--line); background:#fff; cursor:pointer; }
      .sheet{ max-width: 920px; margin: 0 auto; }
      .header{ display:flex; justify-content:space-between; gap:18px; align-items:flex-start; padding-bottom:16px; border-bottom:2px solid var(--ink); }
      .brand{ display:flex; gap:14px; align-items:flex-start; }
      .logo{ width:64px; height:64px; border-radius:14px; border:1px solid var(--line); display:flex; align-items:center; justify-content:center; overflow:hidden; }
      .logo img{ width:100%; height:100%; object-fit:contain; background:#fff; }
      .brand h1{ margin:0; font-size:22px; letter-spacing:.2px; }
      .brand .lines{ margin-top:4px; color:var(--muted); font-size:12px; line-height:1.35; }
      .meta{ text-align:right; min-width: 240px; }
      .meta .tag{ font-size:12px; color:var(--muted); }
      .meta .inv{ font-size:18px; font-weight:700; margin:2px 0 8px 0; }
      .meta .kv{ font-size:12px; color:var(--ink); line-height:1.55; }
      .pill{ display:inline-block; padding:4px 10px; border-radius:999px; border:1px solid var(--line); background:var(--soft); font-weight:700; font-size:12px; }
      .grid{ display:grid; grid-template-columns: 1fr 1fr; gap:14px; margin-top:16px; }
      .card{ border:1px solid var(--line); border-radius:14px; padding:14px; }
      .card h3{ margin:0 0 8px 0; font-size:13px; text-transform:uppercase; letter-spacing:.08em; color:var(--muted); }
      .card .txt{ font-size:13px; line-height:1.45; }
      .section{ margin-top:16px; }
      .section h2{ margin:0 0 8px 0; font-size:14px; text-transform:uppercase; letter-spacing:.08em; color:var(--muted); }
      .note{ border:1px dashed var(--line); border-radius:14px; padding:12px 14px; font-size:13px; color:var(--ink); background:#fff; }
      table{ width:100%; border-collapse:collapse; margin-top:10px; }
      th, td{ padding:10px; border-bottom:1px solid var(--line); font-size:13px; vertical-align:top; }
      th{ text-align:left; background:var(--soft); font-size:12px; text-transform:uppercase; letter-spacing:.06em; color:#374151; }
      td.num{ text-align:right; white-space:nowrap; }
      .totalsWrap{ display:flex; justify-content:flex-end; margin-top:14px; }
      .totals{ width: 340px; border:1px solid var(--line); border-radius:14px; padding:12px 14px; }
      .totals .row{ display:flex; justify-content:space-between; padding:6px 0; font-size:13px; }
      .totals .row strong{ font-weight:700; }
      .totals .grand{ border-top:2px solid var(--ink); margin-top:8px; padding-top:10px; font-size:16px; font-weight:800; }
      .footer{ margin-top:18px; padding-top:14px; border-top:1px solid var(--line); display:flex; justify-content:space-between; gap:16px; }
      .footer .thanks{ color:var(--muted); font-size:12px; line-height:1.4; }
      .sign{ min-width: 240px; text-align:right; }
      .sign .line{ margin-top:26px; border-top:1px solid var(--line); padding-top:6px; font-size:12px; color:var(--muted); }
      @page{ margin: 14mm; }
      @media print{
        .no-print{ display:none; }
        body{ margin:0; }
        .sheet{ max-width:none; }
      }
      @media (max-width: 700px){
        body{ margin:14px; }
        .header{ flex-direction:column; }
        .meta{ text-align:left; min-width: unset; }
        .grid{ grid-template-columns: 1fr; }
        .totals{ width:100%; }
        .footer{ flex-direction:column; }
        .sign{ text-align:left; }
      }
    </style>
  </head><body>
    <div class="no-print"><button onclick="window.print()">Imprimer / Enregistrer en PDF</button></div>

    <div class="sheet">
      <div class="header">
        <div class="brand">
          <div class="logo"><img src="${safe(String(settings?.garageLogoUrl || settings?.logoUrl || "assets/logo.png"))}" alt="Logo"></div>
          <div>
            <h1>${safe(gName)}</h1>
            <div class="lines">
              ${safe(gAddr)}<br>
              ${safe(gEmail)} ${gEmail && gPhone ? "•" : ""} ${safe(gPhone)}<br>
              ${safe(GARAGE.tagline || "")}
            </div>
          </div>
        </div>
        <div class="meta">
          <div class="tag">FACTURE</div>
          <div class="inv">${invNo}</div>
          <div class="kv"><strong>Date:</strong> ${safe(String(wo.createdAt||"").slice(0,16))}</div>
          <div class="kv"><strong>Statut:</strong> <span class="pill">${safe(wo.status||"")}</span></div>
          <div class="kv" style="margin-top:8px; color:var(--muted)">
            <strong>TPS/TVH:</strong> ${safe(String(settings?.garageTpsNo || settings?.tpsNumber || GARAGE.tps || ""))}<br>
            <strong>TVQ:</strong> ${safe(String(settings?.garageTvqNo || settings?.tvqNumber || GARAGE.tvq || ""))}
          </div>
        </div>
      </div>

      <div class="grid">
        <div class="card">
          <h3>Client</h3>
          <div class="txt">
            <strong>${safe(c?.fullName||c?.name||"—")}</strong><br>
            ${safe(c?.phone||"")}<br>
            ${safe(c?.email||"")}
          </div>
        </div>
        <div class="card">
          <h3>Véhicule</h3>
          <div class="txt">
            <strong>${safe(vehTxt)}</strong><br>
            Plaque: ${safe(printPlate)}<br>
            VIN: ${safe(printVin)}<br>
            KM (visite): ${safe(printKm)}
          </div>
        </div>
      </div>

      ${wo.reportedIssue ? `<div class="section"><h2>Problème rapporté</h2><div class="note">${safe(wo.reportedIssue).replace(/\n/g,'<br>')}</div></div>` : ""}
      ${wo.diagnostic ? `<div class="section"><h2>Diagnostic</h2><div class="note">${safe(wo.diagnostic).replace(/\n/g,'<br>')}</div></div>` : ""}
      ${wo.workDone ? `<div class="section"><h2>Travaux effectués</h2><div class="note">${safe(wo.workDone).replace(/\n/g,'<br>')}</div></div>` : ""}

      <div class="section">
        <h2>Détails</h2>
        <table>
          <thead>
            <tr>
              <th style="width:110px">Type</th>
              <th>Description</th>
              <th style="width:70px" class="num">Qté</th>
              <th style="width:110px" class="num">Prix</th>
              <th style="width:120px" class="num">Total</th>
            </tr>
          </thead>
          <tbody>
            ${rows}
          </tbody>
        </table>
      </div>

      <div class="totalsWrap">
        <div class="totals">
          <div class="row"><span>Sous-total</span><strong>${money(wo.subtotal)}</strong></div>
          <div class="row"><span>TPS (${pct(wo.tpsRate)})</span><strong>${money(wo.tpsAmount)}</strong></div>
          <div class="row"><span>TVQ (${pct(wo.tvqRate)})</span><strong>${money(wo.tvqAmount)}</strong></div>
          <div class="row grand"><span>Total</span><span>${money(wo.total)}</span></div>
        </div>
      </div>

      <div class="footer">
        <div class="thanks">
          Merci pour votre confiance.<br>
          Pour toute question, contactez-nous: ${safe(gEmail)} ${gEmail && gPhone ? "•" : ""} ${safe(gPhone)}
        </div>
        <div class="sign">
          ${gSign ? `<div><strong>${safe(gSign)}</strong></div>` : `<div><strong>${safe(gName)}</strong></div>`}
          <div class="line">Signature</div>
        </div>
      </div>
    </div>
  </body></html>`;
  // Impression/PDF: ouvrir la fenêtre tout de suite (iPhone/Safari bloque les popups après un await)
  const w = window.open("", "_blank");

  // Sauvegarde automatique optionnelle: ne jamais bloquer l'impression si Firestore refuse l'update
  try{
    updateDoc(doc(colWorkorders(), workorderId), {
      invoiceHtml: html,
      invoiceSavedAt: serverTimestamp()
    }).catch(()=>{});
  }catch(e){}

  if(w && w.document){
    w.document.open();
    w.document.write(html);
    w.document.close();
    try{ w.focus(); }catch(e){}
  }else{
    alert("Impossible d'ouvrir la facture. Autorise les fenêtres contextuelles pour ce site.");
  }
};

/* Auth boot */
onAuthStateChanged(auth, async (user)=>{
  if(user){
    currentUid = user.uid;
    DATA_MODE = await detectDataMode();
    await ensureUserProfile(user);
    await loadRole();

    // If profile is missing, loadRole() signs out.
    if(!currentUid || currentRole === "unknown") return;

    const currentPage = (location.pathname.split('/').pop() || 'index.html').toLowerCase();
    if(currentRole === "superadmin" && currentPage !== 'superadmin.html'){
      window.location.replace('./superadmin.html');
      return;
    }

    $("viewAuth").style.display = "none";
    $("viewApp").style.display = "";
    $("navAuthed").style.display = "";

    if(unsubProfile) try{unsubProfile();}catch(e){}
    unsubProfile = onSnapshot(docStaffProfile(), (snap)=>{
      if(snap.exists()){
        const d = snap.data();
        currentRole = normalizeRole(d.role) || "mechanic"; window.currentRole = currentRole; currentGarageId = String(d.garageId || currentGarageId || "").trim(); window.currentGarageId = currentGarageId; if(d.disabled===true){ alert("Compte désactivé."); signOut(auth); return; }
        currentUserName = d.fullName || d.name || d.email || "";
        applyRoleUI();
      }
    });

    // settings/meta are admin-only (rules)
    if(currentRole === "admin"){
      if (currentRole === "admin" || currentRole === "superadmin") {
        await ensureSettingsDoc();
      }
      await loadMechanics();
    }
    unsubscribeAll();
    subscribeAll();

    if(currentRole === "mechanic"){
      go("repairs");
    }else{
      go("dashboard");
    }
    if(currentRole === "admin" || currentRole === "superadmin") renderSettings();
  }else{
    currentUid = null;
    currentRole = "unknown";
    currentUserName = "";
    if(unsubProfile) try{unsubProfile();}catch(e){}
    unsubProfile = null;

    unsubscribeAll();
    customers = []; vehicles = []; workorders = [];
    $("viewApp").style.display = "none";
    $("navAuthed").style.display = "none";
    $("viewAuth").style.display = "";
    showAuthMessage("", "");
  }
});

function renderInvoicePrint(){
  const area = document.getElementById("invPrintArea");
  if(!area) return;

  const custId = invCustomerEl?.value || "";
  const cust = customers.find(c=>c.id===custId) || {};
  const clientName = cust.fullName || "";
  const ref = String(invRefEl?.value || "").trim();
  const date = String(invDateEl?.value || "");
  const pay = invPaymentLabel(invPayMethodEl?.value || "cash");

  const items = readInvoiceItems();
  const partsSell = items.reduce((s,it)=> s + Number(it.price||0)*Number(it.qty||1), 0);
  const hours = Math.max(0, Number(invHoursEl?.value || 0));
  const labor = hours>0 ? (hours*Number(settings.laborRate||0)) : Math.max(0, Number(invLaborEl?.value || 0));
  const sub = partsSell + labor;
  const tax = sub * (Number(settings.tpsRate||0) + Number(settings.tvqRate||0));
  const grand = sub + tax;

  const setTxt=(id,val)=>{ const el=document.getElementById(id); if(el) el.textContent = val; };
  setTxt("printGarageLine", settings.garageName||"");
  setTxt("printGarageName", settings.garageName||"");
  setTxt("printGarageAddress", settings.garageAddress||"");
  setTxt("printGaragePhone", settings.garagePhone||"");
  setTxt("printGarageEmail", settings.garageEmail||"");
  setTxt("printClientName", clientName);
  setTxt("printInvoiceMeta", `${ref?("Réf: "+ref+" • "):""}${date?("Date: "+date+" • "):""}Paiement: ${pay}`);
  setTxt("printSub", money(sub));
  setTxt("printTax", money(tax));
  setTxt("printGrand", money(grand));
  setTxt("printSignature", settings.signatureName||"");

  const tbody = document.getElementById("printItemsTbody");
  if(tbody){
    tbody.innerHTML = items.map(it=>`
      <tr>
        <td>${safe(it.desc||"")}</td>
        <td>${safe(String(it.qty||1))}</td>
        <td style="text-align:right">${money(Number(it.price||0))}</td>
      </tr>
    `).join("");
  }
}

async function sendInvoiceEmail(){
  const to = String(invEmailEl?.value || "").trim();
  if(!to){
    alert("Ajoute l'email du client.");
    return;
  }
  const custId = invCustomerEl?.value || "";
  const cust = customers.find(c=>c.id===custId) || {};
  const clientName = cust.fullName || "";

  const ref = String(invRefEl?.value || "").trim() || "Facture";
  const date = String(invDateEl?.value || "");
  const pay = invPaymentLabel(invPayMethodEl?.value || "cash");

  const items = readInvoiceItems();
  const rows = items.map(it=>`
    <tr>
      <td style="padding:6px 8px;border-bottom:1px solid #eee">${safe(it.desc||"")}</td>
      <td style="padding:6px 8px;border-bottom:1px solid #eee;text-align:right">${safe(String(it.qty||1))}</td>
      <td style="padding:6px 8px;border-bottom:1px solid #eee;text-align:right">${money(Number(it.price||0))}</td>
    </tr>
  `).join("");

  const partsCost = items.reduce((s,it)=> s + Number(it.cost||0)*Number(it.qty||1), 0);
  const partsSell = items.reduce((s,it)=> s + Number(it.price||0)*Number(it.qty||1), 0);
  const hours = Math.max(0, Number(invHoursEl?.value || 0));
  const labor = hours>0 ? (hours*Number(settings.laborRate||0)) : Math.max(0, Number(invLaborEl?.value || 0));
  const sub = partsSell + labor;
  const tax = sub * (Number(settings.tpsRate||0) + Number(settings.tvqRate||0));
  const grand = sub + tax;
  const cardFee = (String(invPayMethodEl?.value||"") === "card") ? (grand*Number(settings.cardFeeRate||0)) : 0;
  const netProfit = sub - partsCost - cardFee;

  const html = `
  <div style="font-family:system-ui,-apple-system,Segoe UI,Roboto;max-width:720px">
    <h2 style="margin:0 0 6px 0">${safe(settings.garageName||"Garage")}</h2>
    <div style="color:#666;margin-bottom:14px">${safe(settings.garageAddress||"")}</div>

    <h3 style="margin:0 0 8px 0">Facture ${safe(ref)}</h3>
    <div style="color:#666;margin-bottom:10px">Date: ${safe(date)} • Paiement: ${safe(pay)}</div>
    <div style="margin:10px 0"><b>Client:</b> ${safe(clientName)}</div>

    <table style="width:100%;border-collapse:collapse;border:1px solid #eee;border-radius:10px;overflow:hidden">
      <thead>
        <tr style="background:#f7f7f7">
          <th style="text-align:left;padding:8px">Description</th>
          <th style="text-align:right;padding:8px">Qté</th>
          <th style="text-align:right;padding:8px">Prix</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>

    <div style="margin-top:14px;display:grid;grid-template-columns:1fr 1fr;gap:8px">
      <div></div>
      <div>
        <div style="display:flex;justify-content:space-between"><span>Sous-total</span><b>${money(sub)}</b></div>
        <div style="display:flex;justify-content:space-between;color:#666"><span>Taxes</span><b>${money(tax)}</b></div>
        <div style="display:flex;justify-content:space-between"><span>Total</span><b>${money(grand)}</b></div>
      </div>
    </div>

    <div style="margin-top:18px;color:#666">Merci.</div>
    <div style="margin-top:8px"><b>${safe(settings.signatureName||"")}</b></div>
  </div>`;

  await addDoc(collection(db, "mail"), {
    garageId: currentGarageId || "garage-demo",
    to,
    message: { subject: `Facture ${ref} - ${settings.garageName||"Garage"}`, html },
    createdAt: serverTimestamp()
  });

  alert("Email ajouté à la file d'envoi ✅");
}

// iOS Safari: parfois après window.print(), le scroll se bloque.
// On force un reset léger.
window.addEventListener("afterprint", ()=>{
  try{
    document.documentElement.style.overflow = "auto";
    document.body.style.overflow = "auto";
    document.body.style.height = "auto";
    window.scrollTo(0, window.scrollY);
  }catch(e){}
});

if(btnInvPdf) btnInvPdf.addEventListener("click", ()=>{
  try{
    renderInvoicePrint();
    const area = document.getElementById("invPrintArea");
    if(!area){ window.print(); return; }
    const w = window.open("", "_blank");
    if(!w){ window.print(); return; }
    w.document.open();
    w.document.write(`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
      <title>Facture</title><link rel="stylesheet" href="assets/style.css"></head><body>${area.outerHTML}<script>setTimeout(()=>{window.print();},300);<\/script>
<datalist id="yearListVehicle">
<option value="2027"></option>
<option value="2026"></option>
<option value="2025"></option>
<option value="2024"></option>
<option value="2023"></option>
<option value="2022"></option>
<option value="2021"></option>
<option value="2020"></option>
<option value="2019"></option>
<option value="2018"></option>
<option value="2017"></option>
<option value="2016"></option>
<option value="2015"></option>
<option value="2014"></option>
<option value="2013"></option>
<option value="2012"></option>
<option value="2011"></option>
<option value="2010"></option>
<option value="2009"></option>
<option value="2008"></option>
<option value="2007"></option>
<option value="2006"></option>
<option value="2005"></option>
<option value="2004"></option>
<option value="2003"></option>
<option value="2002"></option>
<option value="2001"></option>
<option value="2000"></option>
<option value="1999"></option>
<option value="1998"></option>
<option value="1997"></option>
<option value="1996"></option>
<option value="1995"></option>
<option value="1994"></option>
<option value="1993"></option>
<option value="1992"></option>
<option value="1991"></option>
<option value="1990"></option>
<option value="1989"></option>
<option value="1988"></option>
<option value="1987"></option>
<option value="1986"></option>
<option value="1985"></option>
<option value="1984"></option>
<option value="1983"></option>
<option value="1982"></option>
<option value="1981"></option>
<option value="1980"></option>
</datalist>
</body></html>`);
    w.document.close();
  }catch(e){ console.error(e); window.print(); }
});

function toast(msg){
  const el = document.createElement("div");
  el.className = "toast";
  el.textContent = msg;
  document.body.appendChild(el);
  setTimeout(()=>{ el.classList.add("show"); }, 10);
  setTimeout(()=>{ el.classList.remove("show"); setTimeout(()=>el.remove(), 250); }, 2200);
}

async function copyText(txt){
  try{ await navigator.clipboard.writeText(String(txt||"")); return true; }
  catch(e){ try{ window.prompt("Copier:", String(txt||"")); return true; }catch(_){ return false; } }
}
function buildInviteLink(code, email, garageId=currentGarageId){
  const url = new URL(window.location.href);
  url.hash = "";
  url.searchParams.set("invite", String(code||""));
  url.searchParams.set("email", String(email||""));
  url.searchParams.set("garageId", String(garageId||""));
  return url.toString();
}
function parseHashParams(){
  const out = {};
  try{
    const url = new URL(window.location.href);
    const setIf = (k,v)=>{ if(v !== null && v !== undefined && String(v).trim() !== "") out[k] = String(v); };
    setIf("invite", url.searchParams.get("invite") || url.searchParams.get("code"));
    setIf("email", url.searchParams.get("email"));
    setIf("garage", url.searchParams.get("garage") || url.searchParams.get("garageId") || url.searchParams.get("g"));
  }catch(e){}
  const h = (window.location.hash||"").replace(/^#/, "");
  h.split("&").forEach(part=>{
    const [k,v] = part.split("=");
    if(!k) return;
    const key = decodeURIComponent(k);
    const val = decodeURIComponent(v||"");
    if(val === "") return;
    out[key] = val;
    if(key === "garageId" && !out.garage) out.garage = val;
    if(key === "code" && !out.invite) out.invite = val;
  });
  return out;
}

async function resolveInviteRef(code, garageId=""){
  const cleanCode = String(code||"").trim();
  const cleanGarageId = String(garageId||"").trim();
  if(!cleanCode) throw new Error("Code invitation invalide");

  if(cleanGarageId){
    const directRef = garageDoc("invites", cleanCode, cleanGarageId);
    const directSnap = await getDoc(directRef);
    if(directSnap.exists()) return { ref: directRef, snap: directSnap, garageId: cleanGarageId };
  }

  const snap = await getDocs(query(collectionGroup(db,"invites"), where("code","==", cleanCode), limit(1)));
  if(snap.empty) throw new Error("Code invitation invalide");
  const found = snap.docs[0];
  const parentGarageId = String(found.ref?.parent?.parent?.id || "").trim();
  const gid = String(found.data()?.garageId || parentGarageId || "").trim();
  if(!gid) throw new Error("Invitation sans garageId");
  return { ref: found.ref, snap: found, garageId: gid };
}

async function registerWithInvite(fullName, code, email, password, garageId=""){
  email = String(email||"").trim().toLowerCase();

  const resolved = await resolveInviteRef(code, garageId);
  const invRef = resolved.ref;
  const invSnap = resolved.snap;
  const inviteGarageId = resolved.garageId;

  const inv = invSnap.data()||{};
  if(String(inv.email||"").toLowerCase() !== String(email||"").toLowerCase()) throw new Error("Invitation pour un autre email");
  if(inv.used) throw new Error("Invitation déjà utilisée");
  if(inv.active === false) throw new Error("Invitation désactivée");
  const role = normalizeRole(inv.role||"mechanic") || "mechanic";

  const cred = await createUserWithEmailAndPassword(auth, email, password);
  const uid = cred.user.uid;
  const nowTs = serverTimestamp();

  await setDoc(doc(db,"staff",uid), {
    uid,
    fullName,
    email,
    role,
    garageId: inviteGarageId,
    inviteCode: code,
    disabled: false,
    createdAt: nowTs,
    updatedAt: nowTs
  });

  await setDoc(doc(db,"users",uid), {
    uid,
    fullName,
    email,
    role,
    garageId: inviteGarageId,
    active: true,
    inviteCode: code,
    createdAt: nowTs,
    updatedAt: nowTs
  }, { merge:true });

  await updateDoc(invRef, {
    used:true,
    usedBy:uid,
    usedAt:serverTimestamp(),
    acceptedEmail: email
  });

  currentGarageId = inviteGarageId;
  window.currentGarageId = inviteGarageId;
  try{ localStorage.setItem("garageId", inviteGarageId); }catch(e){}

  try{ await logEvent("account_created",{inviteCode:code, role, garageId: inviteGarageId}); }catch(e){}
}

function wireAuthTabs(){
  const tabLogin = $("tabLogin");
  const tabReg = $("tabRegister");
  const fLogin = $("formLogin");
  const fReg = $("formRegisterInvite");
  if(!tabLogin || !tabReg || !fLogin || !fReg) return;

  tabLogin.onclick = ()=>{
    tabLogin.classList.add("active"); tabReg.classList.remove("active");
    fLogin.style.display = ""; fReg.style.display = "none";
  };
  tabReg.onclick = ()=>{
    tabReg.classList.add("active"); tabLogin.classList.remove("active");
    fReg.style.display = ""; fLogin.style.display = "none";
    const p = parseHashParams();
    if(p.invite) fReg.inviteCode.value = p.invite;
    if(p.email) fReg.email.value = p.email;
    if((p.garage || p.garageId) && fReg.garageId) fReg.garageId.value = p.garage || p.garageId;
  };

  // default: login
  tabLogin.onclick();

  // if hash has invite, auto open register
  const p = parseHashParams();
  if(p.invite || p.email) tabReg.onclick();

  fReg.onsubmit = async (ev)=>{
    ev.preventDefault();
    const fullName = String(fReg.fullName.value||"").trim();
    const code = String(fReg.inviteCode.value||"").trim();
    const email = String(fReg.email.value||"").trim().toLowerCase();
    const password = String(fReg.password.value||"").trim();
    const garageId = String(fReg.garageId?.value||localStorage.getItem("garageId")||"").trim();
    if(password.length < 6) return alert("Mot de passe: minimum 6 caractères");
    try{
      await registerWithInvite(fullName, code, email, password, garageId);
      alert("Compte créé ✅");
      // clean hash
      history.replaceState(null, "", window.location.pathname);
    }catch(e){

      alert("Erreur création compte: " + (e.message||e));
    }
  };
}
document.addEventListener("DOMContentLoaded", ()=>wireAuthTabs());

async function logEvent(type, data){
  try{
    if(!auth.currentUser) return;
    await addDoc(collection(db,"logs"), {
      garageId: currentGarageId || "garage-demo",
      uid: auth.currentUser.uid,
      email: auth.currentUser.email || "",
      type: String(type||""),
      data: data || {},
      createdAt: serverTimestamp()
    });
  }catch(e){

  }
}

async function createInviteCode(email, role){
  const emailLower = String(email || "").trim().toLowerCase();
  const normalizedRole = normalizeRole(role) || "mechanic";
  const garageId = await ensureCurrentGarageId();
  if(!garageId) throw new Error("missing-garageId");

  const garageSnap = await getDoc(doc(db, "garages", garageId));
  const garageData = garageSnap.exists() ? (garageSnap.data() || {}) : {};
  const garageName = String(garageData.name || garageData.garageName || garageId);

  let code = "";
  let inviteRef = null;
  let existsAlready = true;
  for(let i=0;i<8 && existsAlready;i++){
    code = "GP-" + Math.random().toString(36).slice(2, 8).toUpperCase();
    inviteRef = garageDoc("invites", code, garageId);
    const existingSnap = await getDoc(inviteRef);
    existsAlready = existingSnap.exists();
  }
  if(existsAlready || !inviteRef) throw new Error("Impossible de générer un code invitation");

  await setDoc(inviteRef, {
    code,
    garageId,
    garageName,
    email: emailLower,
    emailLower,
    role: normalizedRole,
    used: false,
    active: true,
    createdBy: auth.currentUser?.uid || "",
    createdAt: serverTimestamp(),
    updatedAt: serverTimestamp()
  });

  try{ await logEvent("invite_created",{code,email: emailLower,role: normalizedRole, garageId}); }catch(e){}
  return code;
}


async function loadInvites(){
  const tbody = $("invitesTbody");
  if(!tbody) return;
  if(currentRole !== "admin" && currentRole !== "superadmin"){ tbody.innerHTML = '<tr><td class="muted" colspan="6">Admin seulement.</td></tr>'; return; }
  tbody.innerHTML = '<tr><td class="muted" colspan="6">Chargement...</td></tr>';
  try{
    const snap = await getDocs(query(colInvites()));
    const rows = [];
    snap.forEach(d=>{
      const x=d.data()||{};
      rows.push({code:d.id, ...x});
    });
    if(rows.length===0){
      tbody.innerHTML = '<tr><td class="muted" colspan="6">Aucune invitation.</td></tr>';
      return;
    }
    tbody.innerHTML = rows.map(r=>{
      const used = r.used ? "Oui" : "Non";
      const dt = r.createdAt?.toDate ? r.createdAt.toDate().toLocaleString() : "—";
      const link = buildInviteLink(r.code, r.email, r.garageId || currentGarageId);
      return `<tr>
        <td><code>${safe(r.code)}</code></td>
        <td>${safe(r.email||"")}</td>
        <td>${safe(r.role||"")}</td>
        <td>${used}</td>
        <td class="muted">${safe(dt)}</td>
        <td style="display:flex;gap:8px;flex-wrap:wrap">
          <button class="btn btn-ghost btn-small" data-act="copyInvite" data-link="${safe(link)}">Copier lien</button>
          <button class="btn btn-ghost btn-small" data-act="emailInvite" data-code="${safe(r.code)}" data-email="${safe(r.email)}" data-role="${safe(r.role)}">Envoyer email</button>
        </td>
      </tr>`;
    }).join("");
  }catch(e){

    tbody.innerHTML = '<tr><td class="muted" colspan="6">Erreur.</td></tr>';
  }
}

async function sendInviteEmail(code, email, role){
  const link = buildInviteLink(code, email);
  const subject = "Invitation — Garage Pro One";
  const html = `
  <div style="font-family:Arial,sans-serif;line-height:1.5">
    <h2>Invitation Garage Pro One</h2>
    <p>Bonjour,</p>
    <p>Vous avez été invité en tant que <b>${safe(role||"mechanic")}</b>.</p>
    <p><b>Lien direct:</b><br/><a href="${link}">${link}</a></p>
    <p><b>Code invitation:</b> <code>${safe(code)}</code></p>
    <p>Si le lien ne fonctionne pas, ouvrez le site puis collez le code dans “Créer un compte (invitation)”.</p>
    <hr/>
    <small>Garage Pro One — Montréal</small>
  </div>`;
  const gid = await ensureCurrentGarageId();
  if(!gid) throw new Error("missing-garageId");
  await addDoc(collection(db,"mail"), { garageId: gid, to: email, message: { subject, html }, createdAt: serverTimestamp() });
  try{ await logEvent("invite_email_sent",{code,email,role,link}); }catch(e){}
}

function renderStaffRows(rows){
  const tbody = $("staffTbody");
  if(!tbody) return;
  if(currentRole !== "admin" && currentRole !== "superadmin"){ tbody.innerHTML = '<tr><td class="muted" colspan="5">Admin seulement.</td></tr>'; return; }
  if(!rows || rows.length===0){
    tbody.innerHTML = '<tr><td class="muted" colspan="5">Aucun employé.</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map(r=>{
    const active = (r.disabled===true) ? "Non" : "Oui";
    const isSelf = (r.uid === currentUid);
    const lockNote = isSelf ? '<span class="badge" style="margin-left:6px">Vous</span>' : '';
    return `<tr>
      <td>${safe(r.fullName||"")}${lockNote}</td>
      <td>${safe(r.email||"")}</td>
      <td>${safe(r.role||"")}</td>
      <td>${active}</td>
      <td style="display:flex;gap:8px;flex-wrap:wrap">
        <button class="btn btn-ghost btn-small" data-act="toggleDisabled" data-uid="${safe(r.uid)}" data-disabled="${r.disabled===true}" ${isSelf ? "disabled" : ""}>${r.disabled===true ? "Activer" : "Désactiver"}</button>
        <button class="btn btn-ghost btn-small" data-act="makeAdmin" data-uid="${safe(r.uid)}" ${isSelf ? "disabled" : ""}>Admin</button>
        <button class="btn btn-ghost btn-small" data-act="makeMech" data-uid="${safe(r.uid)}" ${isSelf ? "disabled" : ""}>Mécano</button>
      </td>
    </tr>`;
  }).join("");
}

function renderInviteRows(rows){
  const tbody = $("invitesTbody");
  if(!tbody) return;
  if(currentRole !== "admin" && currentRole !== "superadmin"){ tbody.innerHTML = '<tr><td class="muted" colspan="6">Admin seulement.</td></tr>'; return; }
  if(!rows || rows.length===0){
    tbody.innerHTML = '<tr><td class="muted" colspan="6">Aucune invitation.</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map(r=>{
    const used = r.used ? "Oui" : "Non";
    const createdAt = r.createdAt && r.createdAt.toDate ? r.createdAt.toDate().toLocaleString() : "";
    return `<tr>
      <td>${safe(r.code||"")}</td>
      <td>${safe(r.email||"")}</td>
      <td>${safe(r.role||"")}</td>
      <td>${used}</td>
      <td>${safe(createdAt)}</td>
      <td>
        <button class="btn btn-ghost btn-small" data-act="deleteInvite" data-code="${safe(r.code||"")}">Supprimer</button>
      </td>
    </tr>`;
  }).join("");
}

async function loadStaffList(){
  const tbody = $("staffTbody");
  if(!tbody) return;
  if(currentRole !== "admin" && currentRole !== "superadmin"){ tbody.innerHTML = '<tr><td class="muted" colspan="5">Admin seulement.</td></tr>'; return; }
  // Prefer live data when available
  if(staffLiveRows && staffLiveRows.length){
    renderStaffRows(staffLiveRows);
    return;
  }
  tbody.innerHTML = '<tr><td class="muted" colspan="5">Chargement...</td></tr>';
  try{
    const snap = await getDocs(currentGarageId ? query(collection(db,"staff"), where("garageId","==", currentGarageId)) : query(collection(db,"staff")));
    const rows = snap.docs.map(d=>({uid:d.id, ...(d.data()||{})}));
    renderStaffRows(rows);
  }catch(e){

    tbody.innerHTML = '<tr><td class="muted" colspan="5">Erreur.</td></tr>';
  }
}

async function countActiveAdmins(){
  // count admins that are NOT disabled (disabled != true)
  const snap = await getDocs(currentGarageId ? query(collection(db,"staff"), where("role","==","admin"), where("garageId","==", currentGarageId)) : query(collection(db,"staff"), where("role","==","admin")));
  const admins = snap.docs.map(d=>({uid:d.id, ...(d.data()||{})}));
  return admins.filter(a => a.disabled !== true).length;
}

async function guardNotLastAdmin(targetUid, actionLabel){
  // Only needed if target is currently an active admin
  const targetSnap = await getDoc(doc(db,"staff", targetUid));
  if(!targetSnap.exists()) return;
  const t = targetSnap.data()||{};
  const targetIsActiveAdmin = (String(t.role||"").toLowerCase()==="admin" && t.disabled !== true);
  if(!targetIsActiveAdmin) return;

  const n = await countActiveAdmins();
  if(n <= 1){
    throw new Error("Action refusée: impossible de " + actionLabel + " le dernier admin actif.");
  }
}

async function setStaffDisabled(uid, disabled){
  await updateDoc(doc(db,"staff",uid), { disabled: !!disabled, updatedAt: serverTimestamp() });
  try{ await logEvent("staff_disabled_changed",{targetUid:uid, disabled:!!disabled}); }catch(e){}
}
async function setStaffRole(uid, role){
  await updateDoc(doc(db,"staff",uid), { role: String(role), updatedAt: serverTimestamp() });
  try{ await logEvent("staff_role_changed",{targetUid:uid, role:String(role)}); }catch(e){}
}

function labelLogType(t){
  const m = {
    invite_created: "Invitation créée",
    invite_email_sent: "Invitation email envoyé",
    account_created: "Compte créé",
    staff_role_changed: "Rôle changé",
    staff_disabled_changed: "Statut employé",
    workorder_status: "Statut réparation",
    invoice_saved: "Facture sauvegardée"
  };
  return m[t] || t || "—";
}
async function loadLogs(){
  const tbody = $("logsTbody");
  if(!tbody) return;
  tbody.innerHTML = '<tr><td class="muted" colspan="4">Chargement...</td></tr>';
  try{
    const typeFilter = String($("logsFilterType")?.value||"");
    let q = query(collection(db,"logs"), orderBy("createdAt","desc"), limit(80));
    if(currentRole !== "admin" && auth.currentUser){
      q = query(collection(db,"logs"), where("uid","==",auth.currentUser.uid), orderBy("createdAt","desc"), limit(80));
    }
    const snap = await getDocs(q);
    const rows=[];
    snap.forEach(d=>{
      const x=d.data()||{};
      if(typeFilter && x.type !== typeFilter) return;
      rows.push(x);
    });
    if(rows.length===0){
      tbody.innerHTML = '<tr><td class="muted" colspan="4">Aucun log.</td></tr>';
      return;
    }
    tbody.innerHTML = rows.map(r=>{
      const dt = r.createdAt?.toDate ? r.createdAt.toDate().toLocaleString() : "—";
      const details = safe(JSON.stringify(r.data||{}));
      return `<tr>
        <td class="muted">${safe(dt)}</td>
        <td>${safe(r.email||"")}</td>
        <td><b>${safe(labelLogType(r.type))}</b></td>
        <td><code style="white-space:pre-wrap">${details}</code></td>
      </tr>`;
    }).join("");
  }catch(e){

    tbody.innerHTML = '<tr><td class="muted" colspan="4">Erreur chargement logs.</td></tr>';
  }
}

function wireEmployeesUI(){
  const btnCreate = $("btnCreateInvite");
  if(btnCreate){
    btnCreate.onclick = async ()=>{
      const email = String($("inviteEmail").value||"").trim().toLowerCase();
      const role = String($("inviteRole").value||"mechanic");
      if(!email.includes("@")) return alert("Email invalide");
      try{
        const code = await createInviteCode(email, role);
        const link = buildInviteLink(code, email);
        $("inviteCreatedInfo").textContent = "Code: "+code;
        $("btnCopyInviteLink").style.display = "";
        $("btnSendInviteEmail").style.display = "";
        $("btnCopyInviteLink").onclick = ()=>copyText(link).then(()=>alert("Lien copié ✅"));
        $("btnSendInviteEmail").onclick = ()=>sendInviteEmail(code, email, role).then(()=>alert("Email envoyé ✅")).catch(e=>{console.error(e); alert("Erreur envoi email");});
        await loadInvites();
      }catch(e){

        alert("Erreur création invitation : " + (e?.message || e));
      }
    };
  }

  const invitesT = $("invitesTbody");
  if(invitesT){
    invitesT.addEventListener("click",(ev)=>{
      const btn = ev.target.closest("[data-act]");
      if(!btn) return;
      const act = btn.getAttribute("data-act");
      if(act==="copyInvite"){
        const link = btn.getAttribute("data-link")||"";
        copyText(link).then(()=>alert("Lien copié ✅"));
      }
      if(act==="emailInvite"){
        const code = btn.getAttribute("data-code")||"";
        const email = btn.getAttribute("data-email")||"";
        const role = btn.getAttribute("data-role")||"mechanic";
        sendInviteEmail(code, email, role).then(()=>alert("Email envoyé ✅")).catch(e=>{console.error(e); alert("Erreur email");});
      }
    });
  }

  const staffT = $("staffTbody");
  if(staffT){
    staffT.addEventListener("click",(ev)=>{
      const btn = ev.target.closest("[data-act]");
      if(!btn) return;
      const act = btn.getAttribute("data-act");
      const uid = btn.getAttribute("data-uid")||"";
      if(!uid) return;
      if(act==="toggleDisabled"){
        const cur = btn.getAttribute("data-disabled")==="true";
        setStaffDisabled(uid, !cur).then(()=>loadStaffList());
      }
      if(act==="makeAdmin"){
        setStaffRole(uid, "admin").then(()=>loadStaffList());
      }
      if(act==="makeMech"){
        setStaffRole(uid, "mechanic").then(()=>loadStaffList());
      }
    });
  }

  const btnLogs = $("btnRefreshLogs");
  if(btnLogs) btnLogs.onclick = ()=>loadLogs();
  const sel = $("logsFilterType");
  if(sel) sel.onchange = ()=>loadLogs();
}

document.addEventListener("DOMContentLoaded", ()=>wireEmployeesUI());

try{
  onAuthStateChanged(auth, (u)=>{

  });
}catch(e){

}

function explainFirebaseError(e){
  const msg = (e && (e.message||e.code||e)) + "";
  if(msg.includes("permission-denied") || msg.includes("Missing or insufficient permissions")){

  }
}
window.addEventListener("unhandledrejection", (ev)=>{
  try{ explainFirebaseError(ev.reason); }catch(_){}
});

// ===== VIN decode (vPIC) =====
async function decodeVinAndFill(vin, setters) {
  const clean = (vin || '').trim();
  if (!clean || clean.length < 8) return;
  const url = `https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVinValuesExtended/${encodeURIComponent(clean)}?format=json`;
  const res = await fetch(url);
  const data = await res.json();
  const row = (data && data.Results && data.Results[0]) ? data.Results[0] : null;
  if (!row) return;

  const make = (row.Make || '').trim();
  const model = (row.Model || '').trim();
  const year = (row.ModelYear || '').trim();
  const body = ((row.BodyClass || '') + ' ' + (row.VehicleType || '')).toLowerCase();
  const fuel = (row.FuelTypePrimary || '').toLowerCase();
  const cyl = (row.EngineCylinders || '').toString().trim();

  // Map fuel to our values
  let moteur = 'Autre';
  if (fuel.includes('gas') || fuel.includes('gasoline') || fuel.includes('essence')) moteur = 'Essence';
  else if (fuel.includes('diesel')) moteur = 'Diesel';
  else if (fuel.includes('electric')) moteur = 'Électrique';
  else if (fuel.includes('hybrid')) moteur = 'Hybride';

  // Map body to our vehicle type (French)
  let typeVeh = 'Autre';
  if (body.includes('minivan') || body.includes('van')) typeVeh = 'Van';
  else if (body.includes('sedan') || body.includes('saloon') || body.includes('berline')) typeVeh = 'Berline';
  else if (body.includes('sport utility') || body.includes('suv') || body.includes('crossover')) typeVeh = 'VUS';
  else if (body.includes('pickup') || body.includes('pick-up') || body.includes('truck')) typeVeh = 'Pickup';
  else if (body.includes('coupe')) typeVeh = 'Coupé';

  setters.setMake?.(make);
  setters.setModel?.(model);
  setters.setYear?.(year);
  setters.setEngineType?.(moteur);
  setters.setCylinders?.(cyl);
  setters.setVehicleType?.(typeVeh);
}

// Delegate click on VIN decode buttons inside modals/forms
document.addEventListener('click', async (e) => {
  const btn = e.target && e.target.closest ? e.target.closest('[data-vin-decode]') : null;
  if (!btn) return;

  const modal = btn.closest('.modal') || document;
  const vinEl = modal.querySelector('input[name="vin"], input#vin, input[placeholder="VIN"]');
  if (!vinEl) return;

  btn.disabled = true;
  btn.textContent = 'Analyse VIN...';
  try {
    const makeEl = modal.querySelector('input[name="make"], input#make, input[placeholder*="Marque"]');
    const modelEl = modal.querySelector('input[name="model"], input#model, input[placeholder*="Modèle"]');
    const yearEl = modal.querySelector('input[name="year"], input[name="annee"], input[name="modelYear"], input#year, input#annee');
    const engineTypeEl = modal.querySelector('select[name="engineType"], select[name="moteurType"], select#engineType');
    const cylindersEl = modal.querySelector('select[name="cylinders"], input[name="cylinders"], input[name="cylindres"], select#cylinders, input#cylinders');
    const vehicleTypeEl = modal.querySelector('select[name="bodyType"], select[name="vehicleType"], select[name="typeVehicule"], select#bodyType, select#vehicleType');

    await decodeVinAndFill(vinEl.value, {
      setMake: (v) => { if (makeEl && v) { makeEl.value = v; makeEl.dispatchEvent(new Event('input', { bubbles:true })); } },
      setModel: (v) => { if (modelEl && v) { modelEl.value = v; modelEl.dispatchEvent(new Event('input', { bubbles:true })); } },
      setYear: (v) => { if (yearEl && v) { yearEl.value = v; yearEl.dispatchEvent(new Event('input', { bubbles:true })); } },
      setEngineType: (v) => { if (engineTypeEl && v) { engineTypeEl.value = v; engineTypeEl.dispatchEvent(new Event('change', { bubbles:true })); } },
      setCylinders: (v) => { if (cylindersEl && v) { cylindersEl.value = v; cylindersEl.dispatchEvent(new Event('change', { bubbles:true })); } },
      setVehicleType: (v) => { if (vehicleTypeEl && v) { vehicleTypeEl.value = v; vehicleTypeEl.dispatchEvent(new Event('change', { bubbles:true })); } },
    });
  } catch (err) {

    alert('Erreur VIN: ' + (err?.message || err));
  } finally {
    btn.disabled = false;
    btn.textContent = 'Remplir via VIN';
  }
});

// ===== VIN BARCODE SCAN (Quagga2) =====
let _vinScanActive = false;
let _vinTorchOn = false;

function _getActiveTrackSafe(){
  try{
    if(window.Quagga && Quagga.CameraAccess && typeof Quagga.CameraAccess.getActiveTrack === 'function'){
      return Quagga.CameraAccess.getActiveTrack();
    }
  }catch(e){ /* ignore */ }
  return null;
}

function _trackTorchSupported(track){
  try{
    if(!track || typeof track.getCapabilities !== 'function') return false;
    const caps = track.getCapabilities();
    return !!(caps && (caps.torch === true || caps.torch));
  }catch(e){
    return false;
  }
}

async function _setTrackTorch(track, on){
  if(!track || typeof track.applyConstraints !== 'function') return false;
  try{
    await track.applyConstraints({ advanced: [{ torch: !!on }] });
    return true;
  }catch(e){
    return false;
  }
}

function _wireTorchButton(btnId, getState, setState){
  const btn = document.getElementById(btnId);
  if(!btn) return;
  const track = _getActiveTrackSafe();
  if(!_trackTorchSupported(track)){
    btn.style.display = 'none';
    return;
  }
  btn.style.display = '';
  const render = ()=>{
    btn.textContent = getState() ? 'Lampe ✅' : 'Lampe';
  };
  render();
  btn.onclick = async ()=>{
    const next = !getState();
    const ok = await _setTrackTorch(_getActiveTrackSafe(), next);
    if(ok){
      setState(next);
      render();
    }
  };
}

// PRO: debounce auto-decode when VIN reaches 17 chars (no need to click "Remplir via VIN")
const _vinAutoTimers = new WeakMap();
function _sanitizeVin(raw){
  return String(raw||'').toUpperCase().replace(/[^A-Z0-9]/g,'');
}
function _looksLikeVin(v){
  // VINs exclude I,O,Q in most standards
  if(!v || v.length !== 17) return false;
  if(/[IOQ]/.test(v)) return false;
  return /^[A-HJ-NPR-Z0-9]{17}$/.test(v);
}

async function _autoDecodeVinForInput(vinInput){
  if(!vinInput) return;
  const modal = vinInput.closest('.modal') || document;
  const vin = _sanitizeVin(vinInput.value);
  if(!_looksLikeVin(vin)) return;

  // don't spam if user keeps typing / scanner fires multiple detections
  const existing = _vinAutoTimers.get(vinInput);
  if(existing) clearTimeout(existing);

  const t = setTimeout(async ()=>{
    try{
      // Try to locate related fields inside the same modal
      const makeEl = modal.querySelector('input[name="make"], input#make, input[placeholder*="Marque"]');
      const modelEl = modal.querySelector('input[name="model"], input#model, input[placeholder*="Modèle"]');
      const yearEl = modal.querySelector('input[name="year"], input[name="annee"], input[name="modelYear"], input#year, input#annee');
      const engineTypeEl = modal.querySelector('select[name="engineType"], select[name="moteurType"], select#engineType');
      const cylindersEl = modal.querySelector('select[name="cylinders"], input[name="cylinders"], input[name="cylindres"], select#cylinders, input#cylinders');
      const vehicleTypeEl = modal.querySelector('select[name="bodyType"], select[name="vehicleType"], select[name="typeVehicule"], select#bodyType, select#vehicleType');

      // Visual feedback if a decode button exists
      const decodeBtn = modal.querySelector('[data-vin-decode]');
      const prevText = decodeBtn ? decodeBtn.textContent : null;
      if(decodeBtn){
        decodeBtn.disabled = true;
        decodeBtn.textContent = 'Remplissage...';
      }

      await decodeVinAndFill(vin, {
        setMake: (v) => { if (makeEl && v) { makeEl.value = v; makeEl.dispatchEvent(new Event('input', { bubbles:true })); } },
        setModel: (v) => { if (modelEl && v) { modelEl.value = v; modelEl.dispatchEvent(new Event('input', { bubbles:true })); } },
        setYear: (v) => { if (yearEl && v) { yearEl.value = v; yearEl.dispatchEvent(new Event('input', { bubbles:true })); } },
        setEngineType: (v) => { if (engineTypeEl && v) { engineTypeEl.value = v; engineTypeEl.dispatchEvent(new Event('change', { bubbles:true })); } },
        setCylinders: (v) => { if (cylindersEl && v) { cylindersEl.value = v; cylindersEl.dispatchEvent(new Event('change', { bubbles:true })); } },
        setVehicleType: (v) => { if (vehicleTypeEl && v) { vehicleTypeEl.value = v; vehicleTypeEl.dispatchEvent(new Event('change', { bubbles:true })); } },
      });

      if(decodeBtn){
        decodeBtn.textContent = '✅ Rempli';
        setTimeout(()=>{
          decodeBtn.textContent = prevText || 'Remplir via VIN';
          decodeBtn.disabled = false;
        }, 900);
      }
    }catch(err){

      // silent: keep manual options available
      const decodeBtn = modal.querySelector('[data-vin-decode]');
      if(decodeBtn){
        decodeBtn.disabled = false;
        decodeBtn.textContent = 'Remplir via VIN';
      }
    }
  }, 650);

  _vinAutoTimers.set(vinInput, t);
}

function openVinScanModal(){
  const m = document.getElementById("vinScanModal");
  if(!m) return;
  m.style.display = "flex";
  m.setAttribute("aria-hidden","false");
  m.removeAttribute("inert");
  document.body.classList.add("modal-open");
}
function closeVinScanModal(){
  const m = document.getElementById("vinScanModal");
  if(!m) return;
  m.style.display = "none";
  m.setAttribute("aria-hidden","true");
  m.setAttribute("inert","");
  document.body.classList.remove("modal-open");
}

// PRO (beta): OCR VIN from the current camera frame using Tesseract.js loaded on demand
async function _ensureTesseract(){
  if(window.Tesseract) return window.Tesseract;
  await new Promise((res, rej)=>{
    const s = document.createElement('script');
    s.src = 'https://unpkg.com/tesseract.js@5/dist/tesseract.min.js';
    s.onload = ()=>res();
    s.onerror = ()=>rej(new Error('Impossible de charger Tesseract.js'));
    document.head.appendChild(s);
  });
  return window.Tesseract;
}

function _extractVinFromText(text){
  const t = String(text||'').toUpperCase().replace(/[^A-Z0-9\s]/g,' ');
  // Find the first plausible 17-char VIN in OCR text
  const m = t.match(/[A-HJ-NPR-Z0-9]{17}/g);
  if(!m || !m.length) return null;
  // Prefer ones without I,O,Q (already excluded by regex)
  return m[0];
}

// --------- PLAQUE (License plate) scan (OCR) ---------
let _plateStream = null;
let _plateTorchOn = false;

function openPlateScanModal(){
  const m = document.getElementById('plateScanModal');
  if(!m) return;
  m.style.display = 'flex';
  m.setAttribute('aria-hidden','false');
  m.removeAttribute('inert');
  document.body.classList.add('modal-open');
}

function _stopPlateCamera(){
  try{
    if(_plateStream){
      for(const t of _plateStream.getTracks()) t.stop();
    }
  }catch(e){ /* ignore */ }
  _plateStream = null;
  _plateTorchOn = false;
}

function closePlateScanModal(){
  const m = document.getElementById('plateScanModal');
  if(!m) return;
  m.style.display = 'none';
  m.setAttribute('aria-hidden','true');
  m.setAttribute('inert','');
  document.body.classList.remove('modal-open');
  _stopPlateCamera();
  const viewport = document.getElementById('plateScanViewport');
  if(viewport) viewport.innerHTML = '';
}

function _normalizePlateRaw(s){
  // Uppercase + strip separators
  let t = String(s||'').toUpperCase().replace(/[^A-Z0-9]/g,'');
  // Common OCR swaps
  const swaps = { 'O':'0', 'I':'1', 'L':'1', 'Z':'2', 'S':'5', 'B':'8' };
  // For mixed strings, apply swaps only when it helps keep alnum length reasonable.
  t = t.split('').map(ch=>swaps[ch] || ch).join('');
  return t;
}

function _isPlausiblePlateCAUS(s){
  const t = _normalizePlateRaw(s);
  // CA/US plates vary widely; keep it conservative to reduce false positives.
  // Typical: 5-8 chars, sometimes 4; allow 4-8.
  if(t.length < 4 || t.length > 8) return false;
  // Must contain at least one letter and one digit in most cases, but allow all-letters personalized.
  const hasA = /[A-Z]/.test(t);
  const hasD = /[0-9]/.test(t);
  if(!(hasA || hasD)) return false;
  if(hasA && hasD) return true;
  // Personalized: allow all letters length 4-8
  return hasA && !hasD;
}

function _extractPlateFromText(text){
  // Keep alnum only, then look for plausible sequences (Canada + USA)
  const raw = String(text||'').toUpperCase().replace(/[^A-Z0-9\s]/g,' ');
  const candidates = (raw.match(/[A-Z0-9]{4,8}/g) || [])
    .map(_normalizePlateRaw)
    .filter(s=>_isPlausiblePlateCAUS(s));
  if(!candidates.length) return null;

  // Prefer 6-7 length, then 5/8, then others
  const score = (s)=>{
    if(s.length===6 || s.length===7) return 200 + s.length;
    if(s.length===5) return 150;
    if(s.length===8) return 140;
    return 100 + s.length;
  };
  candidates.sort((a,b)=>score(b)-score(a));
  return candidates[0];
}

async function _ocrPlateFromVideo(videoEl){
  if(!videoEl) throw new Error('Vidéo introuvable');
  const w = videoEl.videoWidth || 1280;
  const h = videoEl.videoHeight || 720;
  const canvas = document.createElement('canvas');
  canvas.width = w;
  canvas.height = h;
  const ctx = canvas.getContext('2d');
  ctx.drawImage(videoEl, 0, 0, w, h);

  const T = await _ensureTesseract();
  const { data } = await T.recognize(canvas, 'eng', {
    tessedit_char_whitelist: 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
  });
  const plate = _extractPlateFromText(data && data.text);
  if(!plate) throw new Error('Plaque non détectée. Rapproche la caméra, augmente la lumière, puis réessaie.');
  return plate;
}

async function startPlateScanner(){
  // Auto plate scan (Canada + USA) with stability check
  openPlateScanModal();
  const viewport = document.getElementById('plateScanViewport');
  if(!viewport) throw new Error('Viewport plaque introuvable');
  viewport.innerHTML = '';

  const statusEl = document.getElementById('plateScanStatus');
  const lastEl = document.getElementById('plateScanLast');
  const setStatus = (msg)=>{ if(statusEl) statusEl.textContent = msg; };
  const setLast = (msg)=>{ if(lastEl) lastEl.textContent = msg; };

  const wrap = document.createElement('div');
  wrap.style.position = 'relative';
  wrap.style.width = '100%';
  wrap.style.height = '100%';

  const video = document.createElement('video');
  video.setAttribute('playsinline','');
  video.autoplay = true;
  video.muted = true;
  video.style.width = '100%';
  video.style.height = '100%';
  video.style.borderRadius = '12px';

  const frame = document.createElement('div');
  frame.className = 'plate-frame state-none';

  wrap.appendChild(video);
  wrap.appendChild(frame);
  viewport.appendChild(wrap);

  // Start camera
  _plateStream = await navigator.mediaDevices.getUserMedia({
    video: {
      facingMode: { ideal: 'environment' },
      width: { ideal: 1280 },
      height: { ideal: 720 }
    },
    audio: false
  });
  video.srcObject = _plateStream;
  await new Promise(res=>{ video.onloadedmetadata = ()=>res(); });
  try{ await video.play(); }catch(e){ /* ignore */ }

  const btnClose = document.getElementById('btnClosePlateScan');
  if(btnClose) btnClose.onclick = ()=>closePlateScanModal();

  const track = _plateStream.getVideoTracks && _plateStream.getVideoTracks()[0];

  // Torch toggle (best effort) + auto night mode (default ON if supported)
  const torchBtn = document.getElementById('btnToggleTorchPlate');
  if(torchBtn){
    if(!_trackTorchSupported(track)){
      torchBtn.style.display = 'none';
    }else{
      torchBtn.style.display = '';
      // Auto-enable torch by default for better OCR in garages (user can disable)
      if(!_plateTorchOn){
        const ok = await _setTrackTorch(track, true);
        if(ok) _plateTorchOn = true;
      }
      const render = ()=>{ torchBtn.textContent = _plateTorchOn ? 'Lampe ✅' : 'Lampe'; };
      render();
      torchBtn.onclick = async ()=>{
        const next = !_plateTorchOn;
        const ok = await _setTrackTorch(track, next);
        if(ok){ _plateTorchOn = next; render(); }
      };
    }
  }

  // Manual capture fallback (runs OCR once)
  const btnOcr = document.getElementById('btnPlateOcr');
  if(btnOcr){
    btnOcr.onclick = async ()=>{
      const prev = btnOcr.textContent;
      btnOcr.disabled = true;
      btnOcr.textContent = 'Lecture...';
      try{
        const plate = await _ocrPlateFromVideo(video);
        const qs = document.getElementById('quickSearch');
        if(qs) qs.value = plate;
        closePlateScanModal();
        try{ runQuickSearch(); }catch(e){ /* ignore */ }
      }catch(err){

        alert(err.message || 'Erreur OCR plaque');
      }finally{
        btnOcr.disabled = false;
        btnOcr.textContent = prev;
      }
    };
  }

  // Auto loop: OCR on a cropped center region for speed
  let last = null;
  let stableCount = 0;
  let running = true;

  const stopIfClosed = ()=>{
    const m = document.getElementById('plateScanModal');
    if(!m || m.style.display==='none'){
      running = false;
    }
  };

  const cropAndOcr = async ()=>{
    stopIfClosed();
    if(!running) return;

    // Show searching state
    frame.classList.remove('state-ok','state-none');
    frame.classList.add('state-search');
    setStatus('Recherche…');
    try{
      const w = video.videoWidth || 1280;
      const h = video.videoHeight || 720;
      if(w < 10 || h < 10) throw new Error('Caméra pas prête');

      // Crop around center (plate likely in middle)
      const cw = Math.floor(w * 0.75);
      const ch = Math.floor(h * 0.30);
      const sx = Math.floor((w - cw) / 2);
      const sy = Math.floor((h - ch) / 2);

      const canvas = document.createElement('canvas');
      canvas.width = cw;
      canvas.height = ch;
      const ctx = canvas.getContext('2d');
      ctx.drawImage(video, sx, sy, cw, ch, 0, 0, cw, ch);

      const T = await _ensureTesseract();
      const { data } = await T.recognize(canvas, 'eng', {
        tessedit_char_whitelist: 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
      });
      const plate = _extractPlateFromText(data && data.text);

      if(!plate){
        stableCount = 0;
        last = null;
        frame.classList.remove('state-ok','state-search');
        frame.classList.add('state-none');
        setStatus('Aucune plaque…');
        setLast('—');
        return;
      }

      setLast('Lu: ' + plate);

      if(plate === last){
        stableCount += 1;
      }else{
        last = plate;
        stableCount = 1;
      }

      if(stableCount >= 2){
        // Accept
        frame.classList.remove('state-search','state-none');
        frame.classList.add('state-ok');
        setStatus('Plaque détectée ✅');
        const qs = document.getElementById('quickSearch');
        if(qs) qs.value = plate;
        // Small delay so user sees green frame
        setTimeout(()=>{
          try{ closePlateScanModal(); }catch(e){ /* ignore */ }
          try{ runQuickSearch(); }catch(e){ /* ignore */ }
        }, 250);
        running = false;
      }else{
        frame.classList.remove('state-ok','state-none');
        frame.classList.add('state-search');
        setStatus('Stabilisation…');
      }

    }catch(err){

      frame.classList.remove('state-ok','state-search');
      frame.classList.add('state-none');
      setStatus('Erreur caméra/OCR…');
    }
  };

  // Kick off loop: every ~900ms (Tesseract is heavy)
  setStatus('Prêt…');
  setLast('—');

  const loop = async ()=>{
    stopIfClosed();
    if(!running) return;
    await cropAndOcr();
    // Schedule next tick
    if(running) setTimeout(loop, 900);
  };
  loop();
}

async function _ocrVinFromViewport(){
  const viewport = document.getElementById('vinScanViewport');
  if(!viewport) throw new Error('Viewport VIN introuvable');

  // Try canvas first, otherwise capture from <video>
  let canvas = viewport.querySelector('canvas');
  if(!canvas){
    const video = viewport.querySelector('video');
    if(!video) throw new Error('Vidéo caméra introuvable');
    canvas = document.createElement('canvas');
    canvas.width = video.videoWidth || 1280;
    canvas.height = video.videoHeight || 720;
    const ctx = canvas.getContext('2d');
    ctx.drawImage(video, 0, 0, canvas.width, canvas.height);
  }

  const T = await _ensureTesseract();
  const { data } = await T.recognize(canvas, 'eng', {
    tessedit_char_whitelist: 'ABCDEFGHJKLMNPRSTUVWXYZ0123456789',
  });
  const vin = _extractVinFromText(data && data.text);
  if(!vin) throw new Error('VIN non détecté sur la photo. Essaie de zoomer et d’avoir plus de lumière.');
  return vin;
}

async function startVinScanner(onResult){
  if(_vinScanActive) return;
  if(typeof Quagga === "undefined"){
    alert("Scanner non disponible (Quagga non chargé).");
    return;
  }
  _vinScanActive = true;
  openVinScanModal();

  const viewport = document.getElementById("vinScanViewport");
  if(viewport) viewport.innerHTML = "";

  return new Promise((resolve, reject)=>{
    try{
      Quagga.init({
        inputStream: {
          type: "LiveStream",
          target: viewport,
          constraints: {
            facingMode: "environment",
            width: { min: 640 },
            height: { min: 360 }
          }
        },
        locator: { patchSize: "medium", halfSample: true },
        decoder: {
          // VIN barcodes are often CODE_39 / CODE_128
          readers: ["code_39_reader","code_128_reader","codabar_reader"]
        },
        locate: true
      }, function(err){
        if(err){

          _vinScanActive = false;
          closeVinScanModal();
          alert("Erreur caméra: " + (err.message||err));
          reject(err);
          return;
        }
        Quagga.start();

        // PRO: Try to force continuous autofocus on supported browsers (best effort)
        try{
          if(Quagga.CameraAccess && typeof Quagga.CameraAccess.getActiveTrack === 'function'){
            const track = Quagga.CameraAccess.getActiveTrack();
            if(track && typeof track.applyConstraints === 'function'){
              track.applyConstraints({ advanced: [{ focusMode: 'continuous' }] }).catch(()=>{});
            }
          }
        }catch(e){ /* ignore */ }

        // PRO: torch / flash toggle (best effort)
        _vinTorchOn = false;
        _wireTorchButton('btnToggleTorchVin', ()=>_vinTorchOn, (v)=>{ _vinTorchOn = v; });
      });

      const handler = (data)=>{
        const code = data && data.codeResult && data.codeResult.code ? String(data.codeResult.code) : "";
        const vin = code.replace(/[^A-Z0-9]/gi,"").toUpperCase();
        if(vin.length < 8) return;
        // stop quickly to avoid multiple detections
        Quagga.offDetected(handler);
        try{ _setTrackTorch(_getActiveTrackSafe(), false); }catch(e){}
        try{ Quagga.stop(); }catch(e){}
        _vinScanActive = false;
        _vinTorchOn = false;
        closeVinScanModal();
        onResult && onResult(vin);
        resolve(vin);
      };

      Quagga.onDetected(handler);

      const closeBtn = document.getElementById("btnCloseVinScan");
      if(closeBtn){
        closeBtn.onclick = ()=>{
          // turn off torch if possible
          try{ _setTrackTorch(_getActiveTrackSafe(), false); }catch(e){}
          try{ Quagga.stop(); }catch(e){}
          _vinScanActive = false;
          _vinTorchOn = false;
          closeVinScanModal();
          resolve(null);
        };
      }

      const ocrBtn = document.getElementById('btnVinOcr');
      if(ocrBtn){
        ocrBtn.onclick = async ()=>{
          const prev = ocrBtn.textContent;
          ocrBtn.disabled = true;
          ocrBtn.textContent = 'Analyse...';
          try{
            const vin = await _ocrVinFromViewport();
            // stop camera after OCR
            try{ _setTrackTorch(_getActiveTrackSafe(), false); }catch(e){}
            try{ Quagga.stop(); }catch(e){}
            _vinScanActive = false;
            _vinTorchOn = false;
            closeVinScanModal();
            onResult && onResult(vin);
            resolve(vin);
          }catch(err){

            alert(err?.message || String(err));
          }finally{
            ocrBtn.disabled = false;
            ocrBtn.textContent = prev || 'Lire VIN (photo)';
          }
        };
      }
    }catch(e){

      _vinScanActive = false;
      closeVinScanModal();
      reject(e);
    }
  });
}

// Handle scan button clicks
document.addEventListener('click', async (e)=>{
  const btn = e.target && e.target.closest ? e.target.closest('[data-vin-scan]') : null;
  if(!btn) return;

  const modal = btn.closest('.modal') || document;
  const vinEl = modal.querySelector('input[name="vin"], input#vin, input[placeholder="VIN"]');
  if(!vinEl) return;

  btn.disabled = true;
  btn.textContent = "Scanner...";
  try{
    const vin = await startVinScanner((v)=>{
      if(vinEl) vinEl.value = v;
    });
    if(vin){
      // after scan, auto decode VIN (no manual button needed)
      await _autoDecodeVinForInput(vinEl);
    }
  }catch(err){

  }finally{
    btn.disabled = false;
    btn.textContent = "Scanner VIN";
  }
});

// Handle plate scan button clicks (camera OCR)
document.addEventListener('click', async (e)=>{
  const btn = e.target && e.target.closest ? e.target.closest('[data-plate-scan]') : null;
  if(!btn) return;

  const modal = btn.closest('.modal') || document;
  const plateEl = modal.querySelector('input[name="plate"], input#plate, input[placeholder*="plaque" i]');
  if(!plateEl) return;

  btn.disabled = true;
  const oldTxt = btn.textContent;
  if(btn.classList && !btn.classList.contains('icon')) btn.textContent = "Scanner...";
  try{
    const plate = await startPlateScanner();
    if(plate && plateEl){
      plateEl.value = plate;
      // trigger input event (if any listeners)
      plateEl.dispatchEvent(new Event('input', { bubbles:true }));
    }
  }catch(err){

  }finally{
    btn.disabled = false;
    if(btn.classList && !btn.classList.contains('icon')) btn.textContent = oldTxt || "Scanner plaque";
  }
});

// PRO: auto-decode VIN while typing / pasting
document.addEventListener('input', (e)=>{
  const el = e.target;
  if(!(el instanceof HTMLInputElement)) return;
  const isVin = (el.name && el.name.toLowerCase() === 'vin') || el.id === 'vin' || (el.placeholder && el.placeholder.toLowerCase().includes('vin'));
  if(!isVin) return;
  el.value = _sanitizeVin(el.value);
  _autoDecodeVinForInput(el);
});

// ===== PARTS / INVENTORY BARCODE SCAN (Quagga2) =====
let _barcodeScanActive = false;
let _barcodeTorchOn = false;

function _openBarcodeScanModal(title){
  const m = document.getElementById('barcodeScanModal');
  if(!m) return;
  const t = document.getElementById('barcodeScanTitle');
  if(t && title) t.textContent = title;
  m.style.display = 'flex';
  m.setAttribute('aria-hidden','false');
  m.removeAttribute('inert');
  document.body.classList.add('modal-open');
}

function _closeBarcodeScanModal(){
  const m = document.getElementById('barcodeScanModal');
  if(!m) return;
  m.style.display = 'none';
  m.setAttribute('aria-hidden','true');
  m.setAttribute('inert','');
  document.body.classList.remove('modal-open');
}

async function startBarcodeScanner({ title='Scanner code‑barres', readers=["ean_reader","upc_reader","upc_e_reader","code_128_reader","code_39_reader"], onResult } = {}){
  if(_barcodeScanActive) return;
  if(typeof Quagga === 'undefined'){
    alert('Scanner non disponible (Quagga non chargé).');
    return;
  }
  _barcodeScanActive = true;
  _openBarcodeScanModal(title);

  const viewport = document.getElementById('barcodeScanViewport');
  if(viewport) viewport.innerHTML = '';

  return new Promise((resolve, reject)=>{
    try{
      Quagga.init({
        inputStream: {
          type: 'LiveStream',
          target: viewport,
          constraints: {
            facingMode: 'environment',
            width: { min: 640 },
            height: { min: 360 }
          }
        },
        locator: { patchSize: 'medium', halfSample: true },
        decoder: { readers },
        locate: true
      }, function(err){
        if(err){

          _barcodeScanActive = false;
          _closeBarcodeScanModal();
          alert('Erreur caméra: ' + (err.message||err));
          reject(err);
          return;
        }
        Quagga.start();

        // best effort continuous autofocus
        try{
          if(Quagga.CameraAccess && typeof Quagga.CameraAccess.getActiveTrack === 'function'){
            const track = Quagga.CameraAccess.getActiveTrack();
            if(track && typeof track.applyConstraints === 'function'){
              track.applyConstraints({ advanced: [{ focusMode: 'continuous' }] }).catch(()=>{});
            }
          }
        }catch(e){ /* ignore */ }

        // torch / flash toggle (best effort)
        _barcodeTorchOn = false;
        _wireTorchButton('btnToggleTorchBarcode', ()=>_barcodeTorchOn, (v)=>{ _barcodeTorchOn = v; });
      });

      const handler = (data)=>{
        const code = data && data.codeResult && data.codeResult.code ? String(data.codeResult.code) : '';
        const cleaned = code.trim();
        if(cleaned.length < 6) return;
        Quagga.offDetected(handler);
        try{ _setTrackTorch(_getActiveTrackSafe(), false); }catch(e){}
        try{ Quagga.stop(); }catch(e){}
        _barcodeScanActive = false;
        _barcodeTorchOn = false;
        _closeBarcodeScanModal();
        onResult && onResult(cleaned);
        resolve(cleaned);
      };

      Quagga.onDetected(handler);

      const closeBtn = document.getElementById('btnCloseBarcodeScan');
      if(closeBtn){
        closeBtn.onclick = ()=>{
          try{ _setTrackTorch(_getActiveTrackSafe(), false); }catch(e){}
          try{ Quagga.stop(); }catch(e){}
          _barcodeScanActive = false;
          _barcodeTorchOn = false;
          _closeBarcodeScanModal();
          resolve(null);
        };
      }
    }catch(e){

      _barcodeScanActive = false;
      _closeBarcodeScanModal();
      reject(e);
    }
  });
}

// Buttons: add data-barcode-scan to any button and it will fill the closest input (or [data-barcode-target])
document.addEventListener('click', async (e)=>{
  const btn = e.target && e.target.closest ? e.target.closest('[data-barcode-scan]') : null;
  if(!btn) return;

  const mode = (btn.getAttribute('data-barcode-scan') || 'parts').toLowerCase();
  const modal = btn.closest('.modal') || document;
  const targetSel = btn.getAttribute('data-barcode-target');
  const target = targetSel ? modal.querySelector(targetSel) : (modal.querySelector('input[name="barcode"], input[name="partNumber"], input#barcode') || null);

  btn.disabled = true;
  const prev = btn.textContent;
  btn.textContent = 'Scanner...';
  try{
    const title = mode === 'inventory' ? 'Scanner inventaire' : 'Scanner pièce';
    const code = await startBarcodeScanner({ title, onResult: (c)=>{ if(target) target.value = c; } });
    if(code && target){
      target.dispatchEvent(new Event('input', { bubbles: true }));
      target.dispatchEvent(new Event('change', { bubbles: true }));
    }
  }catch(err){

  }finally{
    btn.disabled = false;
    btn.textContent = prev || 'Scanner';
  }
});
