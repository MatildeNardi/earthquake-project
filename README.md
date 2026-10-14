# Analisi co-occorrenza terremoti

Progetto per il corso di *Scalable and Cloud Programming*

L'obiettivo è trovare la **coppia di località (celle) con il massimo numero di co-occorrenze** di terremoti, elencando in ordine crescente le date in cui questi co-occorrono.

- Le coordinate sono approssimate alla **prima cifra decimale** (arrotondamento al più vicino) e i duplicati ottenuti con l'arrotondamento rimossi
- La finestra temporale è di **un giorno**.


Esempio di output:

```
((38.8, -122.8), (38.8, -122.7))
2000-01-05
2000-03-17
...
```

## Struttura del repository

| File | Contenuto |
|---|---|
| `src/main/scala/Main.scala` | Punto di ingresso: legge gli argomenti, il CSV, esegue l'analisi e stampa i risultati |
| `src/main/scala/Earthquake.scala` | Parsing delle righe (data, arrotondamento delle coordinate) e definizione dei tipi |
| `src/main/scala/CooccurrencyAnalysis.scala` | Approcci `pairs`, `stripes`, `pruned` e le varianti di raggruppamento |
| `build.sbt` | Configurazione sbt (Scala 2.12, Spark 3.5) |

## Approcci implementati 
Paradigma: map-reduce.

**Raggruppamento per giorno** (`groupby`, `aggregate`, `combine`): per ogni giorno si ottiene l'insieme ordinato delle celle distinte. I giorni con meno di 2 celle vengono scartati, perché non producono coppie. Le tre varianti differiscono solo nel modo di aggregare:

- `groupby`: `groupByKey` seguito da `distinct`.
- `aggregate`: `aggregateByKey` con `HashSet`.
- `combine`: `combineByKey` con `HashSet`.

**Approcci per il conteggio delle co-occorrenze:**

- **`pairs`**: per ogni giorno emette tutte le coppie `(A, B)` con `A < B` con valore 1, poi `reduceByKey(_ + _)`.
- **`stripes`**: per ogni giorno e cella `A` emette una mappa `{B -> 1}`. Le mappe della stessa `A` vengono fuse con `reduceByKey`.
- **`pruned`**: sfrutta il limite `count(A,B) <= min(freq(A), freq(B))`, dove `freq(X)` è il numero di giorni in cui compare la cella `X`.
  1. Si calcola la frequenza di ogni cella.
  2. Si cerca la miglior coppia tra le `k` celle più frequenti (`k = 200`): il suo conteggio `c0` è un limite inferiore del massimo.
  3. Si scartano le celle con `freq < c0`, perché non possono far parte della coppia vincente.
  4. Si esegue `pairs` solo sulle celle rimaste.

## Requisiti

- JDK 11 o 17
- sbt
- Scala 2.12 / Spark 3.5.x (su Dataproc: image `2.2-debian12`)

## Compilazione

```bash
sbt package
```

Il JAR viene generato in `target/scala-2.12/earthquake-project_2.12-0.1.0.jar`.


## Esecuzione in locale

Comando:
```bash
sbt "run <input_path> <pairs|stripes|pruned> [num_partizioni] [groupby|aggregate|combine]"
```

| Argomento | Significato | Default |
|---|---|---|
| `input_path` | Percorso del CSV (colonne `date`, `latitude`, `longitude`), locale o `gs://...` | obbligatorio |
| approccio | `pairs`, `stripes` o `pruned` | obbligatorio |
| `num_partizioni` | Numero di partizioni (`repartition`); `0` = nessuna repartition | `0` |
| raggruppamento | `groupby`, `aggregate` o `combine` | `groupby` |

L'output stampa: il tempo totale, il numero di co-occorrenze, la coppia vincente e la lista di date.

Esempio:  
```bash
sbt "run data/dataset-earthquakes-trimmed.csv pruned 8 combine"
```

## Esecuzione su Google Cloud Dataproc


### 1. Caricamento di JAR e dataset su Cloud Storage

Prima di lanciare il programma su DataProc, compilare il JAR (vedi sez. precedente) e caricare il file su un bucket in Google Cloud Storage precedentemente creato.

Nel bucket va caricato anche il dataset (file csv).

### 2. Configurazione iniziale

```bash
gcloud config set project <ID_PROGETTO>
gcloud config set dataproc/region europe-west1
gcloud services enable dataproc.googleapis.com compute.googleapis.com storage.googleapis.com

export BUCKET=<nome-bucket>
export REGION=europe-west1
```


### 3. Creazione del cluster

```bash
gcloud dataproc clusters create eq-cluster \
  --region=$REGION \
  --num-workers 2 \
  --master-boot-disk-size 240 \
  --worker-boot-disk-size 240 \
  --master-machine-type=n2-standard-4 \
  --worker-machine-type=n2-standard-4 \
  --image-version=2.2-debian12
```

### 4. Esecuzione di un job

```bash
gcloud dataproc jobs submit spark \
  --cluster=eq-cluster --region=$REGION \
  --class=Main \
  --jars=gs://$BUCKET/earthquake-project_2.12-0.1.0.jar \
  -- gs://$BUCKET/dataset-earthquakes-full.csv pruned 32 combine
```

### 6. Eliminazione del cluster

```bash
gcloud dataproc clusters delete eq-cluster --region=$REGION
```
