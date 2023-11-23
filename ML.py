from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import functions as F
# Load data from a CSV file
dataset = spark.read.csv("path/to/your/data.csv", header=True, inferSchema=True)

# Select columns for k-means (excluding 'username' and 'userid')
selected_columns = [col for col in dataset.columns if col not in ['username', 'userid']]
selected_dataset = dataset.select(selected_columns)

# Trains a k-means model
kmeans = KMeans().setK(2).setSeed(1).maxIterations(20)
model = kmeans.fit(selected_dataset)

# Make predictions
predictions = model.transform(selected_dataset)

# Add 'username' and 'userid' columns back to the result
final_result = dataset.join(predictions, on=selected_dataset.columns)

# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(final_result)
print("Silhouette with squared euclidean distance = " + str(silhouette))

# Shows the final result
final_result.show()


# Assuming 'username' and 'prediction' are column names in the final_result DataFrame
clustered_usernames = final_result.select("username", "prediction").groupBy("prediction").agg(F.collect_list("username").alias("usernames"))

# Show the clustered usernames
clustered_usernames.show(truncate=False)

for row in clustered_usernames.collect():
    cluster = row["prediction"]
    usernames = row["usernames"]

    # Convert the list of usernames to a string
    usernames_str = "\n".join(usernames)

    # Save the usernames to a text file
    with open(f"cluster_{cluster}_usernames.txt", "w") as file:
        file.write(usernames_str)
