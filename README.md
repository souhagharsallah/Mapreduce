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

javac -d out src\Main.java src\common\*.java src\coordinator\*.java src\Map\*.java src\Reducer\*.java

```

## Exécution

Il faut ouvrir un terminal dans le même dossier du projet pour lancer le Manager:

### Terminal 

java -cp out coordinator.Coordinator
