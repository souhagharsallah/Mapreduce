# Mini MapReduce - Compteur de mots

Ce projet est une simulation simple du modèle **MapReduce** en Java avec des sockets.

Le principe est le suivant : le `Coordinator` distribue les fichiers texte aux `MapWorkers`.  
Chaque `MapWorker` compte les mots de son fichier, puis envoie les résultats intermédiaires aux `ReduceWorkers`.  
Les `ReduceWorkers` regroupent ensuite les occurrences pour obtenir le comptage final.

## Compilation

Se placer dans le dossier racine du projet, c'est-à-dire le dossier qui contient `src`.

Créer le dossier de compilation :

```powershell
mkdir out
```

Compiler tous les fichiers Java :

```powershell
$files = Get-ChildItem -Recurse -Filter *.java .\src | ForEach-Object { $_.FullName }
javac -d out $files

OU

javac -d out src\Main.java src\common\*.java src\coordinator\*.java src\Map\*.java src\Reduce\*.java

```

## Exécution

Le projet utilise des sockets, donc il faut ouvrir **5 terminaux** dans le même dossier du projet.

### Terminal 1 - Reducer 0

```powershell
java -cp out Reduce.ReduceWorker 0 6001 2
```

### Terminal 2 - Reducer 1

```powershell
java -cp out Reduce.ReduceWorker 1 6002 2
```

### Terminal 3 - MapWorker 0

```powershell
java -cp out Map.MapWorker 0 5001
```

### Terminal 4 - MapWorker 1

```powershell
java -cp out Map.MapWorker 1 5002
```

### Terminal 5 - Coordinator

```powershell
java -cp out coordinator.Coordinator
```

## Ordre de lancement

L'ordre d'exécution est important :

```text
1. Lancer les ReduceWorkers
2. Lancer les MapWorkers
3. Lancer le Coordinator
``
