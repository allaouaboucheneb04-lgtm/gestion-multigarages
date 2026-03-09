// ⚠️ À remplacer avec tes identifiants EmailJS
const SERVICE_ID = "service_93dlf6r";
const TEMPLATE_ID = "template_do45xgh";
const PUBLIC_KEY = "sQJj_VedDt7vd7ivw";

emailjs.init(PUBLIC_KEY);

document.getElementById("reservationForm").addEventListener("submit", function(e) {
    e.preventDefault();

    const params = {
        nom: document.getElementById("nom").value,
        telephone: document.getElementById("telephone").value,
        depart: document.getElementById("depart").value,
        arrivee: document.getElementById("arrivee").value,
        heure: document.getElementById("heure").value
    };

    emailjs.send(SERVICE_ID, TEMPLATE_ID, params)
        .then(res => {
            document.getElementById("msg").innerText = "Réservation envoyée avec succès !";
        })
        .catch(err => {
            document.getElementById("msg").innerText = "Erreur lors de l'envoi.";
        });
});
