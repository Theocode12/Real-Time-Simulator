
---

# **📖 Tennis Simulation & Streaming System(In Progress)**

## **1. Introduction**
### **1.1 Project Overview**
The **Tennis Simulation & Streaming System** is a real-time tennis game simulation engine designed for relaxation and enjoyment. Whether you want to **spectate AI matches**, **challenge a friend**, or **play against AI**, the system offers a seamless and engaging experience. It features **WebSocket-based live streaming**, customizable game rules, and built-in video & audio communication for multiplayer matches.

### **1.2 Key Features**  
- 🎾 **Create & Play Your Game**: Set up a match and compete with friends or AI.  
- 🏟 **Spectator Mode**: Watch live AI vs. AI and player matches in real-time.  
- 🔄 **Live Streaming**: Game results are streamed gradually for a realistic experience.  
- 🎥 **Video & Audio Communication**: Built-in video calls for multiplayer matches.  
- ⚙ **Highly Configurable Rules**: Customize game, set, and match conditions to fit your style.
- 🤖 AI Commentary: Real-time AI-generated insights and play-by-play analysis to enhance your match experience.

---

## **2. Game Mechanics & Internals**
### **2.1 Highly Configurable Rules**
Users can tweak multiple parameters to define how their tennis matches are played:
- **Points to win a game** (Standard, No-Ad, Custom).
- **Set rules** (Min/max points, tie-break triggers).
- **Match rules** (Number of sets required to win).

### **2.2 Internals - How the Simulation Works**
- The game engine **simulates** an entire match instantly based on predefined AI logic.
- Results are **stored** in a backend system (file, Redis, or database).
- Data is **streamed progressively** via WebSockets to simulate real-time play.

---

## **3. API Endpoints**
### **3.1 HTTP Endpoints**
- `POST /start-game` → Start a new match and return a WebSocket URL.
- `GET /match/{match_id}` → Get match metadata (players, rules, status).
- `GET /live-matches` → Fetch ongoing AI & player matches.
- `POST /api/v1/games/{game_id}/control-token` → Issue an expiring owner token for an active game. This trusted control-plane endpoint requires an `X-Secret` header matching the server's `API_SECRET`; configure `GAME_CONTROL_TOKEN_SECRET` to sign the game-scoped tokens returned in the response.

The owner token is sent with game control WebSocket messages. Keep `GAME_CONTROL_TOKEN_SECRET` identical across backend instances and separate from `API_SECRET`.

### **3.2 WebSocket Endpoints**
- `ws://server/game/{game_id}` → Stream live game data.
- `ws://server/chat/{game_id}` → Real-time chat during a match.

---

## **4. Deployment & Setup**
### **4.1 Running the Backend**
```bash
cd backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload
```

### **4.2 Running the Frontend**
```bash
cd frontend
npm install
npm run dev
```

### **4.3 Redis Setup (Optional)**
If using Redis for storage:
```bash
docker run -d --name redis -p 6379:6379 redis
```

---

## **5. Future Plans**
We plan to expand beyond just tennis! More games will be incorporated to make the platform a **relaxing and fun hub** for players and spectators alike. Stay tuned!
