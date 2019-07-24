
# Train test 'leave one out' splitting strategy for testing implicit recommendation systems

def LOO_traintest_split(df, test_size = 1, partition_column = 'rank'):
  #leave one out train test split
  window_1 = Window.partitionBy(df['super_id']).orderBy(rand())
  df = df.select('*', rank().over(window_1).alias('rank'))

  train = df.filter(df['rank'] > test_size)
  test = df.filter(df['rank'] <= test_size)

  return(train,test)


# Train test 'last n' splitting strategy for testing timeseries models

def timeseries_traintest_split(df, test_size, order_column, partition_column):
  # ranking first
  window = Window.partitionBy(df[partition_column]).orderBy(order_column)
  df = df.select('*', rank().over(window).alias('rank'))

  #works for ascending rank only
  max_rank = df.agg({'rank': "max"}).collect()[0][0]
  
  train = df.filter(df['rank'] < max_rank - test_size )
  test = df.filter(df['rank'] >= max_rank - test_size )

  return(train,test)

