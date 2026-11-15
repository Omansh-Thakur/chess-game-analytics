from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('chess-etl').getOrCreate()

RAW = 'hdfs://master:9000/chess/raw'
OUT = 'hdfs://master:9000/chess/parquet'
Q = chr(34)


def read_csv(path):
    return (spark.read.option('header', True)
            .option('quote', Q).option('escape', Q).csv(path))


games = read_csv(RAW + '/games.csv')

games = (games
    .withColumn('white_elo', F.col('white_elo').cast('int'))
    .withColumn('black_elo', F.col('black_elo').cast('int'))
    .withColumn('plies', F.col('plies').cast('int'))
    .withColumn('has_clock', F.col('has_clock').cast('int'))
    .withColumn('has_eval', F.col('has_eval').cast('int'))
    .withColumn('game_date', F.to_date(F.regexp_replace('date', '[.]', '-')))
    .withColumn('hour', F.hour(F.to_timestamp('time', 'HH:mm:ss')))
    .withColumn('moves', F.ceil(F.col('plies') / 2))
    .withColumn('winner', F.when(F.col('result') == '1-0', 'white')
                           .when(F.col('result') == '0-1', 'black')
                           .when(F.col('result') == '1/2-1/2', 'draw'))
    .withColumn('white_score', F.when(F.col('winner') == 'white', 1.0)
                                .when(F.col('winner') == 'draw', 0.5)
                                .otherwise(0.0))
    .dropna(subset=['white_elo', 'black_elo', 'winner'])
    .filter(F.col('plies') > 0)
    .withColumn('avg_elo', (F.col('white_elo') + F.col('black_elo')) / 2)
    .withColumn('elo_diff', F.col('white_elo') - F.col('black_elo'))
    .withColumn('opening_family', F.trim(F.split(F.col('opening'), ':').getItem(0)))
    .withColumn('elo_bracket', F.when(F.col('avg_elo') < 1000, '<1000')
                                .when(F.col('avg_elo') < 1400, '1000-1399')
                                .when(F.col('avg_elo') < 1800, '1400-1799')
                                .when(F.col('avg_elo') < 2200, '1800-2199')
                                .otherwise('2200+')))

games.write.mode('overwrite').parquet(OUT + '/games')

moves = read_csv(RAW + '/moves.csv')
for c in ['ply', 'clock_sec', 'eval_cp', 'loss_cp', 'mover_elo']:
    moves = moves.withColumn(c, F.col(c).cast('int'))
moves.write.mode('overwrite').parquet(OUT + '/moves')

print('games rows:', spark.read.parquet(OUT + '/games').count())
print('moves rows:', spark.read.parquet(OUT + '/moves').count())
spark.stop()

