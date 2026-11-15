from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('chess-analysis').getOrCreate()

BASE = 'hdfs://master:9000/chess'
MIN_GAMES = 200

games = spark.read.parquet(BASE + '/parquet/games').cache()
moves = spark.read.parquet(BASE + '/parquet/moves')


def save(df, name):
    df.coalesce(1).write.mode('overwrite').option('header', True).csv(BASE + '/results/' + name)


def pct(outcome):
    return F.round(100 * F.avg(F.when(F.col('winner') == outcome, 1).otherwise(0)), 2)


def outcomes():
    return [F.count('*').alias('games'), pct('white').alias('white_pct'),
            pct('draw').alias('draw_pct'), pct('black').alias('black_pct')]


def bracket(col):
    return (F.when(F.col(col) < 1000, '<1000').when(F.col(col) < 1400, '1000-1399')
             .when(F.col(col) < 1800, '1400-1799').when(F.col(col) < 2200, '1800-2199')
             .otherwise('2200+'))


# 1. Overall summary
save(games.agg(F.count('*').alias('games'), F.round(F.avg('moves'), 1).alias('avg_moves'),
               F.round(F.avg('avg_elo'), 0).alias('avg_elo'), pct('white').alias('white_pct'),
               pct('draw').alias('draw_pct'), pct('black').alias('black_pct')), 'summary')

# 2. Opening results by rating bracket
save(games.filter(F.col('opening_family') != '?')
          .groupBy('opening_family', 'elo_bracket').agg(*outcomes())
          .filter(F.col('games') >= MIN_GAMES), 'opening_winrates')

# 3. Most popular openings
total = games.count()
save(games.groupBy('opening_family').agg(F.count('*').alias('games'))
          .withColumn('share_pct', F.round(100 * F.col('games') / total, 2))
          .orderBy(F.desc('games')).limit(50), 'opening_popularity')

# 4. Game length by speed
save(games.groupBy('speed').agg(F.count('*').alias('games'),
          F.round(F.avg('moves'), 1).alias('avg_moves'),
          F.percentile_approx('moves', 0.5).alias('median_moves')).orderBy('speed'), 'game_length')

# 5. Colour advantage by rating bracket
save(games.groupBy('elo_bracket').agg(*outcomes()), 'color_by_bracket')

# 6. How games end
save(games.groupBy('speed', 'termination').agg(F.count('*').alias('games')), 'terminations')

# 7. Activity by hour of day
save(games.groupBy('hour').agg(F.count('*').alias('games'),
          F.round(F.avg('avg_elo'), 0).alias('avg_elo')).orderBy('hour'), 'hourly_activity')

# 8. Does the rating difference predict the result?
save(games.withColumn('elo_diff_bucket',
          F.round(F.greatest(F.least(F.col('elo_diff'), F.lit(400)), F.lit(-400)) / 100) * 100)
          .withColumn('expected', 1 / (1 + F.pow(F.lit(10.0), -F.col('elo_diff') / 400)))
          .groupBy('elo_diff_bucket').agg(F.count('*').alias('games'),
               F.round(F.avg('white_score'), 4).alias('actual_white_score'),
               F.round(F.avg('expected'), 4).alias('expected_white_score'))
          .filter(F.col('games') >= MIN_GAMES).orderBy('elo_diff_bucket'), 'rating_upsets')

# 9. Time pressure and blunders (needs games with clock and eval data)
m = (moves.withColumn('clock_bucket',
          F.when(F.col('clock_sec') < 10, '0-9s').when(F.col('clock_sec') < 30, '10-29s')
           .when(F.col('clock_sec') < 60, '30-59s').when(F.col('clock_sec') < 180, '60-179s')
           .otherwise('180s+'))
          .withColumn('blunder', (F.col('loss_cp') >= 200).cast('int')))

save(m.groupBy('speed', 'clock_bucket').agg(F.count('*').alias('moves'),
          F.round(100 * F.avg('blunder'), 3).alias('blunder_pct'),
          F.round(F.avg('loss_cp'), 1).alias('avg_loss_cp'))
          .filter(F.col('moves') >= MIN_GAMES), 'time_pressure')

# 10. Blunder rate by rating
save(m.withColumn('rating_bracket', bracket('mover_elo')).groupBy('rating_bracket')
          .agg(F.count('*').alias('moves'), F.round(100 * F.avg('blunder'), 3).alias('blunder_pct')),
     'blunders_by_rating')

spark.stop()

