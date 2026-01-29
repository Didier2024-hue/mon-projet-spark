from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, isnan, when, count, trim, regexp_replace, lower, length
from pyspark.sql.types import DoubleType
from urllib.request import urlretrieve
import os
import re
import shutil

# CONFIGURATION & INITIALISATION
# ======================================================================

DATA_DIR = "./data/"
CACHE_DIR = "./cache/"

def clean_column_name(col_name):
    return re.sub(r'[^a-zA-Z0-9]', '_', col_name).lower()

def init_directories():
    for directory in [DATA_DIR, CACHE_DIR]:
        if not os.path.exists(directory):
            os.makedirs(directory)
        else:
            if directory == CACHE_DIR:
                shutil.rmtree(CACHE_DIR)
                os.makedirs(CACHE_DIR)

init_directories()

# Initialisation de Spark
spark = SparkSession.builder \
    .appName("GooglePlayStore-DataPrep") \
    .config("spark.sql.csv.parser.quoteMode", "ALL") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()

# Téléchargement des fichiers
files = {
    "gps_app.csv": "https://assets-datascientest.s3.eu-west-1.amazonaws.com/gps_app.csv",
    "gps_user.csv": "https://assets-datascientest.s3.eu-west-1.amazonaws.com/gps_user.csv"
}

print("\n" + "="*80)
print("TÉLÉCHARGEMENT DES FICHIERS")
print("="*80)
for filename, url in files.items():
    filepath = os.path.join(DATA_DIR, filename)
    print(f"Téléchargement de {filename}...")
    urlretrieve(url, filepath)

# Options pour la lecture des CSV
csv_options = {
    "header": True,
    "inferSchema": True,
    "quote": '"',
    "escape": '"',
    "sep": ",",
    "multiLine": True,
    "encoding": "UTF-8"
}

def load_and_prepare(filepath):
    df = spark.read.options(**csv_options).csv(filepath)
    for column in df.columns:
        df = df.withColumnRenamed(column, clean_column_name(column))
    return df

print("\n" + "="*80)
print("CHARGEMENT DES DONNÉES")
print("="*80)
df_app = load_and_prepare(os.path.join(DATA_DIR, "gps_app.csv"))
df_user = load_and_prepare(os.path.join(DATA_DIR, "gps_user.csv"))

df_app.cache()
df_user.cache()

print("\nVérification des données:")
print("-"*80)
print(f"gps_app: {df_app.count()} lignes, {len(df_app.columns)} colonnes")
print(f"gps_user: {df_user.count()} lignes, {len(df_user.columns)} colonnes")

print("\n" + "="*80)
print("SAUVEGARDE EN CACHE PARQUET")
print("="*80)
df_app.write.mode("overwrite").parquet(os.path.join(CACHE_DIR, "df_app.parquet"))
df_user.write.mode("overwrite").parquet(os.path.join(CACHE_DIR, "df_user.parquet"))

# Q.2 - SCHÉMAS DES DONNÉES
print("\n" + "="*80)
print("Q.2 - SCHÉMAS DES DONNÉES")
print("="*80)
print("\nSchéma gps_app:")
df_app.printSchema()
print("\nSchéma gps_user:")
df_user.printSchema()

print("\nObservations sur les schémas:")
print("-"*80)
print("- gps_app: Schéma cohérent avec les attentes. Certaines colonnes sont numériques, d'autres catégorielles.")
print("- gps_user: Présence de colonnes numériques pour les notes utilisateur et d'informations utilisateur sous forme de texte.")

# Q.3 - DataFrame gps_app.csv
print("\n" + "="*80)
print("Q.3 - TRAITEMENT DES DONNÉES gps_app.csv")
print("="*80)

print("\nQ.3.1 - Imputation des valeurs manquantes pour les ratings")
df_app = df_app.withColumn("rating", when(isnan(col("rating")), None).otherwise(col("rating")))
df_app = df_app.withColumn("rating", col("rating").cast("double"))
missing_before = df_app.filter(col("rating").isNull()).count()
total = df_app.count()
print(f"Ratings manquants avant imputation: {missing_before}/{total} ({missing_before/total*100:.2f}%)")
rating_median = df_app.approxQuantile("rating", [0.5], 0.01)[0]
print(f"Médiane des ratings: {rating_median:.4f}")
df_app = df_app.fillna({"rating": rating_median})
missing_after = df_app.filter(col("rating").isNull()).count()
print(f"Ratings manquants après imputation: {missing_after}")

print("\nConclusion:")
print("-"*80)
print("La médiane est utilisée pour l'imputation afin de minimiser l'impact des outliers et préserver une mesure centrale représentative.")

print("\n" + "="*80)
print("Q.3.2 - Imputation des valeurs manquantes pour 'type'")
print("="*80)

total_count = df_app.count()
missing_count = df_app.filter(col("type").isNull() | (trim(col("type")) == "")).count()
print(f"Analyse de la colonne 'type':")
print(f"- Nombre total de lignes: {total_count}")
print(f"- Valeurs manquantes (NULL ou vides): {missing_count}")
print(f"- Pourcentage de valeurs manquantes: {missing_count/total_count*100:.2f}%")

mode_value = df_app.filter((col("type").isNotNull()) & (trim(col("type")) != "")) \
                  .groupBy("type") \
                  .count() \
                  .orderBy("count", ascending=False) \
                  .first()[0]
print(f"\nValeur majoritaire (pour imputation): '{mode_value}'")

df_app = df_app.withColumn(
    "type",
    when((col("type").isNull()) | (trim(col("type")) == ""), mode_value)
    .otherwise(col("type"))
)

final_missing = df_app.filter(col("type").isNull() | (trim(col("type")) == "")).count()
print(f"\nValeurs manquantes après imputation: {final_missing}")

print("\nDistribution finale:")
df_app.groupBy("type").count().orderBy("count", ascending=False).show(truncate=False)

print("\n" + "="*80)
print("Q.3.3 - Imputation des valeurs manquantes pour 'content_rating'")
print("="*80)

df_app = df_app.withColumn("content_rating", when(isnan(col("content_rating")), None).otherwise(col("content_rating")))
missing_before = df_app.filter(col("content_rating").isNull()).count()
print(f"Valeurs manquantes avant imputation: {missing_before}/{total} ({missing_before/total*100:.2f}%)")

mode_value = df_app.filter(col("content_rating").isNotNull()).groupBy("content_rating").count().orderBy("count", ascending=False).first()["content_rating"]
df_app = df_app.fillna({"content_rating": mode_value})
missing_after = df_app.filter(col("content_rating").isNull()).count()
print(f"Valeurs manquantes après imputation: {missing_after}")

print("\nConclusion:")
print("-"*80)
print("Imputation par la valeur 'Everyone', cohérente avec le fait que la plupart des applications s'adressent à un large public.")

print("\n" + "="*80)
print("Q.3.4 - Imputation des valeurs manquantes pour 'current_ver' et 'android_ver'")
print("="*80)

df_app = df_app.withColumn("current_ver", when(isnan(col("current_ver")), None).otherwise(col("current_ver")))
df_app = df_app.withColumn("android_ver", when(isnan(col("android_ver")), None).otherwise(col("android_ver")))
missing_before_current = df_app.filter(col("current_ver").isNull()).count()
missing_before_android = df_app.filter(col("android_ver").isNull()).count()
print(f"current_ver manquants avant imputation: {missing_before_current}/{total} ({missing_before_current/total*100:.2f}%)")
print(f"android_ver manquants avant imputation: {missing_before_android}/{total} ({missing_before_android/total*100:.2f}%)")

mode_current_ver = df_app.filter(col("current_ver").isNotNull()).groupBy("current_ver").count().orderBy("count", ascending=False).first()["current_ver"]
mode_android_ver = df_app.filter(col("android_ver").isNotNull()).groupBy("android_ver").count().orderBy("count", ascending=False).first()["android_ver"]
df_app = df_app.fillna({"current_ver": mode_current_ver, "android_ver": mode_android_ver})
missing_after_current = df_app.filter(col("current_ver").isNull()).count()
missing_after_android = df_app.filter(col("android_ver").isNull()).count()
print(f"current_ver manquants après imputation: {missing_after_current}")
print(f"android_ver manquants après imputation: {missing_after_android}")

print("\nConclusion:")
print("-"*80)
print("Imputation par la modalité majoritaire, conservant la cohérence des versions pour les applications.")

print("\n" + "="*80)
print("Q.3.5 - Vérification qu'il ne reste plus de valeurs manquantes dans gps_app")
print("="*80)

def count_missing(df):
    return df.select([
        count(when(col(c).isNull() | isnan(col(c)) | (col(c) == ''), c)).alias(c)
        for c in df.columns
    ])

print("Valeurs manquantes restantes dans gps_app:")
count_missing(df_app).show()

# Q.4 - DataFrame gps_user.csv
print("\n" + "="*80)
print("Q.4 - TRAITEMENT DES DONNÉES gps_user.csv")
print("="*80)

print("\nQ.4.1 - Analyse des valeurs manquantes dans df_user")
print("Valeurs manquantes avant traitement:")
count_missing(df_user).show()

print("\nLignes avec valeurs manquantes sur colonnes critiques:")
df_user.select("translated_review", "sentiment", "sentiment_polarity", "sentiment_subjectivity") \
    .filter(
        col("translated_review").isNull() | (col("translated_review") == '') |
        col("sentiment").isNull() |
        col("sentiment_polarity").isNull() |
        col("sentiment_subjectivity").isNull()
    ).show(5)

print("\n" + "="*80)
print("Q.4.2 - Imputation des valeurs manquantes dans df_user")
print("="*80)

df_user = df_user.filter(
    (col("translated_review").isNotNull()) & (col("translated_review") != '') &
    col("sentiment").isNotNull() &
    col("sentiment_polarity").isNotNull() &
    col("sentiment_subjectivity").isNotNull()
)
print(f"Nombre de lignes après suppression des reviews manquantes: {df_user.count()}")

if "rating" in df_user.columns:
    mean_rating = df_user.select(mean(col("rating"))).first()[0]
    df_user = df_user.fillna({"rating": mean_rating})

print("\nValeurs manquantes après imputation/suppression:")
count_missing(df_user).show()

print("\nConclusion:")
print("-"*80)
print("- Suppression des reviews non exploitables pour garantir la qualité des analyses de sentiment.")
print("- Imputation des ratings par la moyenne si nécessaire pour conserver l'information quantitative.")

print("\n" + "="*80)
print("Q.4.3 - Vérification finale de l'absence de valeurs manquantes")
print("="*80)
print("\nVérification finale des valeurs manquantes:")

def getMissingValues(df):
    missing_counts = []
    for c in df.columns:
        missing_count = df.filter(col(c).isNull() | isnan(col(c)) | (col(c) == '')).count()
        missing_counts.append((c, missing_count))
    missing_df = spark.createDataFrame(missing_counts, schema=["column", "missing_count"])
    return missing_df

def missingTable(missing_df):
    print("\n" + "="*80)
    print("TABLEAU DES VALEURS MANQUANTES")
    print("="*80)
    total_rows = df_user.count()
    rows = missing_df.collect()
    print(f"{'Colonne':30} | {'Valeurs manquantes':>15} | {'Pourcentage (%)':>15}")
    print("-"*70)
    for row in rows:
        col_name = row['column']
        miss = row['missing_count']
        pct = (miss / total_rows) * 100 if total_rows > 0 else 0
        print(f"{col_name:30} | {miss:15d} | {pct:15.2f}")
    print("="*80)

missing_df_user = getMissingValues(df_user)
missingTable(missing_df_user)

print("\n" + "="*80)
print("Q.5.1 - Vérification de la présence de valeurs non numériques dans sentiment_polarity et sentiment_subjectivity")
print("="*80)

non_numeric_polarity = df_user.filter(
    col("sentiment_polarity").isNotNull() & col("sentiment_polarity").cast(DoubleType()).isNull()
)

non_numeric_subjectivity = df_user.filter(
    col("sentiment_subjectivity").isNotNull() & col("sentiment_subjectivity").cast(DoubleType()).isNull()
)

print(f"Valeurs non numériques dans 'sentiment_polarity': {non_numeric_polarity.count()}")
print(f"Valeurs non numériques dans 'sentiment_subjectivity': {non_numeric_subjectivity.count()}")

print("\n" + "="*80)
print("Q.5.2 - Conversion des colonnes sentiment_polarity et sentiment_subjectivity en float")
print("="*80)

colonnes_a_convertir = ["sentiment_polarity", "sentiment_subjectivity"]

for col_name in colonnes_a_convertir:
    df_user = df_user.withColumn(col_name, col(col_name).cast("float"))

df_user.printSchema()

print("\n" + "="*80)
print("Q.5.3 - Nettoyage des caractères spéciaux dans translated_review")
print("="*80)

df_user = df_user.withColumn("translated_review", regexp_replace(col("translated_review"), "[^\\w\\s]", " "))
df_user = df_user.withColumn("translated_review", regexp_replace(col("translated_review"), "\\s{2,}", " "))

df_user.select("translated_review").show(truncate=False)

print("\n" + "*" * 80)
print("Q.5.4 Minimisation de tous les caractères de la colonne translated_review")
print("*" * 80 + "\n")

df_user = df_user.withColumn("translated_review", lower("translated_review"))

df_user.select("translated_review").show(5, truncate=100)

print("\n" + "*" * 80)
print("Q.5.5 Affichage du nombre de commentaires pour chaque groupe de tailles (1 à 10 caractères)")
print("*" * 80 + "\n")

df_user = df_user.withColumn("review_length", length("translated_review"))

df_user.groupBy("review_length") \
    .count() \
    .filter("review_length >= 1 AND review_length <= 10") \
    .orderBy("review_length") \
    .show(truncate=False)

print("\n" + "*" * 80)
print("Q.5.6 Filtrer les commentaires de taille supérieure ou égale à 3")
print("*" * 80 + "\n")

df_user = df_user.filter(length("translated_review") >= 3)

df_user.select("translated_review").show(5, truncate=100)

from pyspark.sql.functions import col, lower, trim

print("Q.5.7 Calcul des 20 mots les plus fréquents dans les commentaires positifs")
print("*" * 80 + "\n")

# Normaliser la colonne sentiment (minuscules, suppression espaces)
df_user = df_user.withColumn("sentiment_normalized", lower(trim(col("sentiment"))))

# Filtrer les commentaires positifs
df_pos = df_user.filter(col("sentiment_normalized") == "positive")

print(f"Nombre de commentaires positifs : {df_pos.count()}")

# Récupérer les commentaires traduits sous forme de RDD
rdd_reviews = df_pos.select("translated_review").rdd.map(lambda row: row[0])

# Split des mots, mise en minuscules et création de paires (mot, 1)
words_rdd = rdd_reviews.flatMap(lambda text: text.split()) \
    .map(lambda word: (word.lower(), 1)) \
    .filter(lambda x: x[0] != "")  # filtrer les mots vides

# Compter les occurrences par mot
counts_rdd = words_rdd.reduceByKey(lambda a, b: a + b)

# Extraire les 20 mots les plus fréquents (par nombre décroissant)
top_20_words = counts_rdd.takeOrdered(20, key=lambda x: -x[1])

print("Top 20 mots les plus fréquents dans les commentaires positifs :")
for word, count in top_20_words:
    print(f"{word} : {count}")

print("\nTraitement terminé avec succès.")

from pyspark.sql.functions import col, regexp_replace, to_date
from pyspark.sql.types import IntegerType, DoubleType

# Q.6.1 Conversion de la colonne 'reviews' en integer
print("Q.6.1 Conversion de la colonne 'reviews' en integer")
df_clean = df_app.withColumn("reviews_clean", regexp_replace(col("reviews"), "[^0-9]", ""))
df_clean = df_clean.withColumn("reviews_int", col("reviews_clean").cast(IntegerType()))
df_clean = df_clean.drop("reviews", "reviews_clean").withColumnRenamed("reviews_int", "reviews")

print("Constatation Q.6.1 :")
print("- Suppression des caractères non numériques dans 'reviews'.")
print("- Conversion en integer effectuée.")
print(f"- Exemple de valeurs 'reviews' après conversion :")
df_clean.select("reviews").show(5, truncate=False)

# Q.6.2 Conversion de la colonne 'installs' en integer
print("\nQ.6.2 Conversion de la colonne 'installs' en integer")
df_clean = df_clean.withColumn("installs_clean", regexp_replace(col("installs"), "[^0-9]", ""))
df_clean = df_clean.withColumn("installs_int", col("installs_clean").cast(IntegerType()))
df_clean = df_clean.drop("installs", "installs_clean").withColumnRenamed("installs_int", "installs")

print("Constatation Q.6.2 :")
print("- Retrait des signes '+' et virgules dans 'installs'.")
print("- Conversion en integer réalisée.")
print(f"- Exemple de valeurs 'installs' après conversion :")
df_clean.select("installs").show(5, truncate=False)

# Q.6.3 Conversion de la colonne 'price' en double
print("\nQ.6.3 Conversion de la colonne 'price' en double")
df_clean = df_clean.withColumn("price_clean", regexp_replace(col("price"), "[^0-9.,]", ""))
df_clean = df_clean.withColumn("price_clean", regexp_replace(col("price_clean"), ",", "."))
df_clean = df_clean.withColumn("price_double", col("price_clean").cast(DoubleType()))
df_clean = df_clean.drop("price", "price_clean").withColumnRenamed("price_double", "price")

print("Constatation Q.6.3 :")
print("- Suppression des symboles monétaires et uniformisation des décimales (virgule remplacée par point).")
print("- Conversion en double pour traitement financier.")
print(f"- Exemple de valeurs 'price' après conversion :")
df_clean.select("price").show(5, truncate=False)

# Q.6.4 Conversion de la colonne 'last_updated' en date
print("\nQ.6.4 Conversion de la colonne 'last_updated' en date")
df_clean = df_clean.withColumn("last_updated_date", to_date(col("last_updated"), "MMMM d, yyyy"))

print("Constatation Q.6.4 :")
print("- Transformation de la chaîne texte en type date.")
print("- Permettra l'analyse chronologique et le filtrage temporel.")
print(f"- Exemple de valeurs 'last_updated_date' :")
df_clean.select("last_updated", "last_updated_date").show(5, truncate=False)

print("\nSchema final après nettoyage :")
df_clean.printSchema()

print("\nTraitement terminé avec succès.")

# ======================================================================
# FERMETURE SPARK
# ======================================================================
spark.stop()
