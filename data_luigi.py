import luigi
import pandas as pd
import schedule
import time


class ExtractData(luigi.Task):
    file_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget("extracted_data.csv")

    def run(self):
        df = pd.read_csv(self.file_path)
        # ... (traitement initial des données si nécessaire)
        df.to_csv(self.output().path, index=False)


class TransformData(luigi.Task):
    def requires(self):
        return ExtractData(file_path="iris.csv")

    def output(self):
        return luigi.LocalTarget("transformed_data.csv")

    def run(self):
        df = pd.read_csv(self.input().path)
        # ... (opérations de transformation des données)
        df.to_csv(self.output().path, index=False)


class LoadData(luigi.Task):
    def requires(self):
        return TransformData()

    def output(self):
        return luigi.LocalTarget("data_loaded.txt")

    def run(self):
        df = pd.read_csv(self.input().path)
        # ... (chargement des données dans une base de données, un entrepôt, etc.)
        with self.output().open("w") as f:
            f.write("Données chargées avec succès!")


def run_luigi_tasks():
    luigi.build([LoadData()], local_scheduler=True)


# Exécution toutes les 30 secondes
schedule.every(30).seconds.do(run_luigi_tasks)

while True:
    schedule.run_pending()
    time.sleep(1)  # Vérifier chaque seconde

# # executer une fois
# if __name__ == "__main__":
#     luigi.build([LoadData()], local_scheduler=True)

# # executer tous les jours
# def run_luigi_tasks():
#     luigi.build([LoadData()], local_scheduler=True)

# schedule.every().day.at("00:00").do(run_luigi_tasks)  # Exécution à minuit

# while True:
#     schedule.run_pending()
#     time.sleep(60)  # Vérifier toutes les minutes
