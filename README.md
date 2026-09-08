# ZIQVONA V1 — 2 moun

MVP pou teste chat an tan reyèl ak apèl vwa/videyo WebRTC.

## Kouri lokalman
Node.js 18+ nesesè.

    npm install
    npm start

Ouvri http://localhost:3000. Louvri 2 fenèt/tabs epi konekte ak 2 non diferan.

## Test sou 2 telefòn
Pou chat, yon server sou menm Wi‑Fi ka sèvi. Pou kamera/mikwo WebRTC, navigatè mobil yo mande HTTPS (eksepte localhost). Deploye app la sou yon host HTTPS/WSS pou 2 telefòn teste voice/video.

## Fonksyon
- Login demo
- Online presence
- 1-to-1 real-time messaging via WebSocket
- Typing indicator
- WebRTC voice/video signaling + STUN
- Mute/camera controls
- Responsive turquoise UI

## Pou production
Ajoute database, secure authentication, password hashing, persistent message history, TURN server, rate limiting, moderation/reporting, push notifications, and production HTTPS/WSS.
