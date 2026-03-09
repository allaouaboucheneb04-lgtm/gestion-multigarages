
// Firebase initialization (compatible GitHub Pages)
import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
import { getFirestore } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js";
import { getAuth } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js";
import { getAnalytics } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-analytics.js";

const firebaseConfig = window.FIREBASE_CONFIG;

const app = initializeApp(firebaseConfig);

window.db = getFirestore(app);
window.auth = getAuth(app);
window.analytics = getAnalytics(app);

console.log("Firebase connected");
