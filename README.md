# Crypto Positioning Recorder

Μικρό σύστημα που μαζεύει **δωρεάν** δεδομένα δομής αγοράς (open interest, funding rate,
long/short ratios) από **Binance, Bybit, MEXC** και τα αποθηκεύει σε Parquet — τρέχοντας
μόνο του στο cloud της GitHub, **χωρίς να χρειάζεται ο υπολογιστής σου ανοιχτός**.

> **Γιατί είναι δωρεάν & ασφαλές:** χρησιμοποιεί **μόνο public market-data endpoints** —
> **κανένα API key**. Άρα το repo μπορεί να είναι **public** χωρίς κανέναν κίνδυνο, και τα
> public repos έχουν **απεριόριστα δωρεάν λεπτά** στο GitHub Actions.

---

## Τι μαζεύει

| Exchange | Open Interest | Funding | Long/Short ratios |
|---|:---:|:---:|---|
| **Binance** | ✅ | ✅ | global + top-trader account, top-trader position, taker buy/sell |
| **Bybit** | ✅ | ✅ | account ratio (buy/sell) |
| **MEXC** | ✅ (holdVol) | ✅ | — (το MEXC δεν τα δίνει μέσω public API) |

Σύμβολα (προεπιλογή): `BTC, ETH, SOL, BNB, DOGE, XRP`. Άλλαξέ τα στην κορυφή του `recorder.py`
(μεταβλητή `SYMBOLS`).

---

## Μορφή δεδομένων

Ένα αρχείο **ανά exchange ανά ημέρα**: `data/<exchange>/<YYYY-MM-DD>.parquet`

«Long format» — μία γραμμή ανά μέτρηση:

| Στήλη | Περιγραφή |
|---|---|
| `arrival_iso` | πότε το μαζέψαμε (UTC, ISO8601) |
| `arrival_ms` | το ίδιο σε epoch ms |
| `event_ms` | πότε **ισχύει** το δεδομένο κατά το exchange (αν το δίνει· αλλιώς κενό) |
| `exchange` | `binance` / `bybit` / `mexc` |
| `symbol` | π.χ. `BTCUSDT`, `BTC_USDT` |
| `source` | το endpoint (π.χ. `premiumIndex`, `accountRatio`) |
| `metric` | το όνομα του μεγέθους (π.χ. `lastFundingRate`, `openInterest`) |
| `value` | η τιμή (float) |

Οι **δύο χρονοσημάνσεις** (arrival vs event) είναι σκόπιμες: εξασφαλίζουν *point-in-time*
ορθότητα στην έρευνα — ξέρεις πάντα τι ήταν διαθέσιμο τη στιγμή κάθε απόφασης.

---

## Εγκατάσταση (μία φορά, ~5 λεπτά)

1. **Φτιάξε ένα νέο PUBLIC repository** στο GitHub (π.χ. `crypto-positioning-recorder`).

2. **Ανέβασε τα αρχεία** αυτού του φακέλου στο repo. Είτε μέσω της ιστοσελίδας
   (Add file → Upload files), είτε με git:
   ```bash
   git init
   git add .
   git commit -m "initial: positioning recorder"
   git branch -M main
   git remote add origin https://github.com/<USERNAME>/<REPO>.git
   git push -u origin main
   ```

3. **Δώσε δικαίωμα εγγραφής στο workflow** (κρίσιμο — αλλιώς δεν θα μπορεί να αποθηκεύει):
   `Settings` → `Actions` → `General` → ενότητα **Workflow permissions** →
   επίλεξε **Read and write permissions** → `Save`.

4. **Ξεκίνα το χρονοδιάγραμμα** με μία χειροκίνητη εκτέλεση:
   tab **Actions** → **Record Crypto Positioning Data** → **Run workflow**.
   (Τα προγραμματισμένα workflows συχνά αρχίζουν να «πυροδοτούνται» μόνο μετά την πρώτη
   χειροκίνητη εκτέλεση.)

Από εκεί και πέρα τρέχει **μόνο του κάθε ~15 λεπτά**. Δες τα δεδομένα να συσσωρεύονται
στον φάκελο `data/`.

---

## (Προαιρετικό) Ειδοποιήσεις Telegram σε αποτυχία

Το GitHub **δεν** σε ειδοποιεί αν ένα scheduled run αποτύχει. Για να παίρνεις μήνυμα:

1. Φτιάξε bot μέσω [@BotFather](https://t.me/BotFather) → πάρε το **token**.
2. Βρες το **chat id** σου (π.χ. μέσω [@userinfobot](https://t.me/userinfobot)).
3. Στο repo: `Settings` → `Secrets and variables` → `Actions` → **New repository secret**,
   πρόσθεσε δύο: `TELEGRAM_BOT_TOKEN` και `TELEGRAM_CHAT_ID`.

Χωρίς αυτά, ο recorder δουλεύει κανονικά — απλώς δεν στέλνει ειδοποιήσεις.

---

## Πώς διαβάζεις τα δεδομένα

Κατέβασε τα τελευταία (`git pull`) και:

```python
import polars as pl

# Όλα τα Parquet του Binance
df = pl.read_parquet("data/binance/*.parquet")

# Παράδειγμα: funding rate του BTC σε χρονοσειρά (long → wide)
btc = (
    df.filter((pl.col("symbol") == "BTCUSDT") & (pl.col("metric") == "lastFundingRate"))
      .select("arrival_iso", "value")
      .sort("arrival_iso")
)
print(btc.tail())
```

Τοπικά στα Windows: `py -3.12 recorder.py` (για να δοκιμάσεις ένα snapshot με το χέρι).

---

## Όρια & καλά-να-ξέρεις (GitHub Actions)

- **UTC μόνο.** Το cron δεν έχει ζώνη ώρας. Το `7,22,37,52 * * * *` = κάθε 15′ σε UTC.
  Τα ίδια τα δεδομένα φέρουν UTC χρονοσήμανση· μετατροπή σε ώρα Ελλάδας γίνεται μόνο
  στην ανάλυση/εμφάνιση.
- **Ελάχιστο διάστημα 5 λεπτά**, και τα runs μπορεί να **καθυστερούν 5–30′** σε ώρες αιχμής.
  Κατάλληλο για snapshots των 15′/1ώρας — **όχι** για δευτερόλεπτα ή liquidation cascades.
- **Auto-disable μετά από 60 ημέρες αδράνειας** του repo. Επειδή ο recorder κάνει commit
  δεδομένα σε κάθε run, το repo **δεν** μένει αδρανές, οπότε αυτό λύνεται από μόνο του.
- **Γεωγραφικά:** οι runners της GitHub είναι σε US datacenters. Τα public market-data
  endpoints συνήθως είναι προσβάσιμα — αλλά **αν** κάποια στιγμή τα endpoints της Binance
  μπλοκαριστούν από εκεί, ο recorder συνεχίζει κανονικά με **Bybit + MEXC** (κάθε exchange
  είναι απομονωμένο).

---

## Επόμενο βήμα

Όταν συσσωρευτούν μερικές εβδομάδες δεδομένων, αυτά τροφοδοτούν τις ερευνητικές κατευθύνσεις
**OI×price** και **cross-asset** του framework. Παράλληλα, η έρευνα σε **funding** και
**volatility** μπορεί να ξεκινήσει αμέσως με το δωρεάν *ιστορικό* funding του MEXC και τα
δωρεάν OHLCV — δεν χρειάζονται καθόλου αυτή τη συσσώρευση.

---

*Εκπαιδευτικό εργαλείο. Δεν αποτελεί επενδυτική συμβουλή.*
