# Garage Pro One — Synchro automatique (iPhone + PC) — HTML/CSS/JS + Firebase

✅ Même site sur iPhone et PC, **données synchronisées automatiquement** (cloud).
Connexion: **Email / Mot de passe**.

## 1) Créer Firebase (Google)
1. Firebase Console → crée un projet
2. **Authentication** → Sign-in method → **Email/Password** (Enable)
3. **Firestore Database** → Create database

## 2) Mettre la config Firebase (OBLIGATOIRE)
Firebase Console → Project settings → Your apps → Web app → SDK setup and configuration

Copie l'objet `firebaseConfig` et colle-le dans:
- `assets/firebase-config.js`

Exemple:
```js
window.FIREBASE_CONFIG = {
  apiKey: "...",
  authDomain: "...",
  projectId: "...",
  storageBucket: "...",
  messagingSenderId: "...",
  appId: "..."
};
```

## 3) Règles de sécurité Firestore
Firebase Console → Firestore Database → Rules
Copie/colle le fichier `firestore.rules`.

## 4) Mettre en ligne (Firebase Hosting)
Sur PC:
```bash
npm i -g firebase-tools
firebase login
firebase init hosting
firebase deploy
```

## Utilisation
- Ouvre le site
- Onglet **Créer compte**
- Connecte-toi sur iPhone et PC avec le même email/mot de passe
➡️ mêmes clients / réparations.

---

## Promotions: créer + envoyer par email

La page **Promotions** te permet:
- d’enregistrer une promotion (objet + message)
- puis de l’envoyer par email à tes clients (ceux qui ont un champ **email** valide)

### Important (emails)
Un site statique (GitHub Pages) ne peut pas envoyer des emails “proprement” sans backend.
👉 La solution incluse: **Firebase Cloud Functions + SendGrid**.

### 1) Déployer Firebase Functions (PC requis)
1) Installe Firebase Tools:
```bash
npm i -g firebase-tools
firebase login
```
2) Dans le dossier du projet:
```bash
firebase init functions
```
3) Remplace/merge avec le dossier `functions/` fourni dans ce projet.

### 2) Configurer SendGrid
1) Crée un compte SendGrid et génère une API Key
2) Configure les variables:
```bash
firebase functions:config:set sendgrid.key="TON_SENDGRID_API_KEY" sendgrid.from="ton_email@ton-domaine.com"
```

### 3) Déployer
```bash
cd functions
npm install
cd ..
firebase deploy --only functions
```

### 4) Utiliser
- Ajoute des clients avec un email (page **Clients**)
- Crée une promotion (page **Promotions**)
- Clique **Envoyer** (ou mets un email de test)

Variables dans le message: `{name}` et `{phone}`.
