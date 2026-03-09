VERSION FIREBASE COMPLÈTE - TAXI ÉTOILE

Cette version utilise :
- Firebase Authentication (login email/mot de passe)
- Cloud Firestore (réservations en temps réel)
- Dashboard admin protégé par collection admins

À FAIRE DANS FIREBASE
1. Ouvrir Firebase Console
2. Authentication > Sign-in method > activer Email/Password
3. Créer un utilisateur admin dans Authentication
4. Firestore Database > créer la base en mode production ou test
5. Dans Firestore, créer le document :
   collection: admins
   document id: UID du compte admin
   contenu:
   {
     email: "tonadmin@email.com",
     role: "admin",
     active: true
   }
6. Coller le contenu de firebase.rules.txt dans Firestore Rules puis Publish
7. Déployer le site sur GitHub Pages

STRUCTURE FIRESTORE
- reservations/{autoId}
- admins/{uid}

FONCTIONS INCLUSES
- réservation client avec email, passagers, vol, notes, aller-retour
- sauvegarde Firebase
- dashboard temps réel
- login admin Firebase
- filtre trajet
- filtre statut
- changement de statut
- suppression
- export JSON

IMPORTANT
- Les clés Firebase côté front-end sont normales pour un site web.
- La sécurité réelle se fait avec les Firestore Rules.
