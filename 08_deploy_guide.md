# Deploy Guide — Deal Pipeline AI
**Backend su Railway · Frontend su GitHub Pages · Dominio da Wix**

---

## Struttura finale

```
tuodominio.com  (o app.tuodominio.com)
      │
      └── GitHub Pages ──── index.html  (frontend)
                                  │ chiamate API
      Railway.app ─────────── 06_api.py  (backend FastAPI)
                                  │
                             Supabase  (database)
```

---

## PARTE 1 — Backend su Railway

### 1.1 Crea account Railway
1. Vai su **railway.app** → Sign up con GitHub
2. Il piano gratuito include **500 ore/mese** — sufficiente per uso quotidiano

### 1.2 Crea nuovo progetto
1. Dashboard Railway → **New Project** → **Deploy from GitHub repo**
2. Seleziona il tuo repository
3. Railway rileva automaticamente `Procfile` e usa `railway.json`

### 1.3 Aggiungi le variabili d'ambiente
In Railway → il tuo progetto → **Variables** → aggiungi:

| Variabile              | Valore                          |
|------------------------|---------------------------------|
| `SUPABASE_URL`         | https://xxxx.supabase.co        |
| `SUPABASE_SERVICE_KEY` | eyJ...                          |
| `OPENAI_API_KEY`       | sk-...                          |
| `GOOGLE_SHEET_ID`      | ID del tuo Sheet                |
| `GOOGLE_DRIVE_FOLDER_ID` | ID cartella Drive (opzionale) |
| `GOOGLE_CREDENTIALS_PATH` | `/app/credentials.json`     |
| `ALLOWED_ORIGINS`      | https://tuodominio.com,https://tuonome.github.io |

> **Nota su credentials.json**: Railway non ha un file system persistente.
> Aggiungi il contenuto JSON come variabile `GOOGLE_CREDENTIALS_JSON`
> e usa questo codice in `06_api.py` per ricostruire il file all'avvio:
> ```python
> import json, os
> creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
> if creds_json:
>     with open('credentials.json', 'w') as f:
>         json.dump(json.loads(creds_json), f)
> ```

### 1.4 Copia l'URL del tuo backend
Dopo il deploy: Railway → il tuo progetto → **Settings** → **Domains**
Trovi l'URL tipo: `https://deal-pipeline-ai.up.railway.app`

### 1.5 Aggiorna il frontend
Nel file `index.html` (ex `07_frontend.html`), modifica questa riga:
```javascript
const PROD_API = 'https://deal-pipeline-ai.up.railway.app'; // ← il tuo URL Railway
```

---

## PARTE 2 — Frontend su GitHub Pages

### 2.1 Prepara il repository
1. Crea un repo su GitHub (es. `deal-pipeline-ai`, **pubblico**)
2. Rinomina `07_frontend.html` → `index.html`
3. Carica nel repo: `index.html` (e opzionalmente gli altri file Python come riferimento)

Struttura del repo:
```
deal-pipeline-ai/
├── index.html          ← il frontend (ex 07_frontend.html)
├── 01_schema.sql
├── 02_etl_pipeline.py
├── 03_search_engine.py
├── 04_github_actions.yml  → copiare in .github/workflows/etl_nightly.yml
├── 05_requirements.txt
├── 06_api.py
├── Procfile
├── railway.json
└── .env.example
```

### 2.2 Abilita GitHub Pages
1. Repository → **Settings** → **Pages** (menu laterale)
2. Source: **Deploy from a branch**
3. Branch: `main` · Folder: `/ (root)` → **Save**
4. Dopo 1-2 minuti: il sito è live su `https://tuonome.github.io/deal-pipeline-ai`

### 2.3 Aggiungi il dominio personalizzato
1. Settings → Pages → **Custom domain**
2. Inserisci il tuo dominio, es. `app.tuodominio.com` → **Save**
3. GitHub mostrerà un messaggio: "DNS check in progress"

---

## PARTE 3 — Configurazione DNS su Wix

### Caso A — Sottodominio (CONSIGLIATO)
Usi `app.tuodominio.com` per il sito, `tuodominio.com` resta su Wix.

1. **Wix** → Domains → il tuo dominio → **DNS Records** → **Add Record**
2. Aggiungi un record **CNAME**:

| Tipo  | Host | Valore                      | TTL  |
|-------|------|-----------------------------|------|
| CNAME | app  | tuonome.github.io           | 3600 |

3. Attendi 5–30 minuti per la propagazione DNS
4. Su GitHub Pages: "Your site is published at https://app.tuodominio.com" ✓

### Caso B — Dominio root
Usi `tuodominio.com` direttamente (il sito Wix non sarà più raggiungibile).

Aggiungi 4 record **A** su Wix DNS:

| Tipo | Host | Valore          | TTL  |
|------|------|-----------------|------|
| A    | @    | 185.199.108.153 | 3600 |
| A    | @    | 185.199.109.153 | 3600 |
| A    | @    | 185.199.110.153 | 3600 |
| A    | @    | 185.199.111.153 | 3600 |

> ⚠️ Con il Caso B, il sito Wix originale non sarà più accessibile su quel dominio.
> Se hai ancora contenuti Wix da tenere, usa il Caso A con un sottodominio.

---

## PARTE 4 — Abilitare HTTPS (automatico)

GitHub Pages abilita HTTPS automaticamente una volta configurato il dominio.
Su GitHub → Settings → Pages → spunta **Enforce HTTPS** (disponibile dopo ~1h dalla propagazione DNS).

---

## Checklist finale

- [ ] Backend deployato su Railway e risponde su `https://xxxx.up.railway.app/health`
- [ ] `index.html` aggiornato con URL Railway corretto
- [ ] `index.html` caricato su GitHub, Pages abilitato
- [ ] Record DNS aggiunto su Wix (CNAME o A record)
- [ ] Dominio personalizzato impostato su GitHub Pages
- [ ] HTTPS forzato su GitHub Pages
- [ ] Test end-to-end: apri il dominio, fai una ricerca, verifica i risultati

---

## Troubleshooting

**"DNS check failed" su GitHub Pages**
→ Aspetta fino a 24h. Se persiste, controlla che il record CNAME punti esattamente a `tuonome.github.io` (senza `/` finale).

**CORS error nel browser (Cannot access API)**
→ Verifica che `ALLOWED_ORIGINS` su Railway includa il tuo dominio esatto (con `https://`).

**Railway va in sleep dopo inattività**
→ Piano gratuito: il servizio va in sleep dopo 30 min senza richieste. La prima richiesta può impiegare 10-15 secondi. Per evitarlo, upgrada a Railway Starter (~$5/mese) o usa un servizio di "keep-alive" (es. UptimeRobot che fa ping ogni 5 minuti gratuitamente).

**Credenziali Google non funzionano su Railway**
→ Aggiungi il contenuto di `credentials.json` come variabile `GOOGLE_CREDENTIALS_JSON` e aggiungi il codice di ricostruzione all'inizio di `06_api.py` (vedi Parte 1.3).
