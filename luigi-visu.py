import luigi
import logging
import time


class Tache1(luigi.Task):
    fichier_sortie = luigi.Parameter(default="fichier1.txt")

    def output(self):
        return luigi.LocalTarget(self.fichier_sortie)

    def run(self):
        logging.info(f"Exécution de Tache1. Écriture dans {self.fichier_sortie}")
        with self.output().open("w") as f:
            f.write("Hello from Task 1!")

        # Ajout d'une boucle infinie avec un délai
        while True:
            time.sleep(60)  # Attendre 60 secondes


class Tache2(luigi.Task):
    fichier_sortie = luigi.Parameter(default="fichier2.txt")

    def requires(self):
        return Tache1(fichier_sortie="fichier_tache1.txt")

    def output(self):
        return luigi.LocalTarget(self.fichier_sortie)

    def run(self):
        logging.info(f"Exécution de Tache2. Écriture dans {self.fichier_sortie}")
        with self.input().open("r") as fin, self.output().open("w") as fout:
            fout.write(fin.read() + " And Task 2!")

        # Ajout d'une boucle infinie avec un délai
        while True:
            time.sleep(60)  # Attendre 60 secondes


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    luigi.build([Tache2()], scheduler_host="localhost")
